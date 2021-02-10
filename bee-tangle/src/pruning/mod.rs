// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod config;

use crate::{
    MsTangle,
    {metadata::MessageMetadata, unconfirmed_message::UnconfirmedMessage},
};
use bee_message::{
    milestone::{Milestone, MilestoneIndex},
    payload::{indexation::HashedIndex, Payload},
    solid_entry_point::SolidEntryPoint,
    Message, MessageId,
};
use bee_storage::access::{Batch, Delete, Fetch};
use futures::executor;
use hashbrown::HashMap;

pub use config::{PruningConfig, PruningConfigBuilder};

#[allow(dead_code)]
pub(crate) const SOLID_ENTRY_POINT_CHECK_THRESHOLD_PAST: u32 = 50;
#[allow(dead_code)]
pub(crate) const ADDITIONAL_PRUNING_THRESHOLD: u32 = 50;

use crate::{storage::StorageBackend, traversal};
use log::{info, warn};

#[derive(Debug)]
#[allow(dead_code)]
pub enum Error {
    MilestoneNotFoundInTangle(u32),
    Storage(Box<dyn std::error::Error + Send>),
}

/// Get the new solid entry points.
#[allow(dead_code)]
pub async fn get_new_solid_entry_points<B: StorageBackend>(
    tangle: &MsTangle<B>,
    target_index: MilestoneIndex,
) -> Result<HashMap<MessageId, MilestoneIndex>, Error> {
    let mut solid_entry_points = HashMap::<MessageId, MilestoneIndex>::new();
    for index in *target_index - SOLID_ENTRY_POINT_CHECK_THRESHOLD_PAST..*target_index {
        // TODO: make Apply return `Future`
        if let Some(message_id) = tangle.get_milestone_message_id(MilestoneIndex(index)).await {
            traversal::visit_parents_depth_first(
                tangle,
                message_id,
                |_, _, metadata| async move {
                    // collect all msg that were referenced by that milestone or newer
                    if let Some(milestone_index) = metadata.milestone_index() {
                        *milestone_index >= index
                    } else {
                        false
                    }
                },
                |id, _, metadata| {
                    if !executor::block_on(tangle.is_solid_entry_point(id)) {
                        if let Some(milestone_index) = metadata.milestone_index() {
                            solid_entry_points.insert(*id, milestone_index);
                        }
                    }
                },
                |_, _, _| {},
                |_| {},
            )
            .await;
        } else {
            return Err(Error::MilestoneNotFoundInTangle(index));
        }
    }
    Ok(solid_entry_points)
}

/// Prune the unconfirmed (i.e., unreferenced) messages in the database, and return the number of pruned messages.
#[allow(dead_code)]
pub async fn prune_unreferenced_messages<B: StorageBackend>(
    storage: &B,
    purning_milestone_index: &MilestoneIndex,
) -> Result<usize, Error> {
    // Column pairs those should be deleted form the database
    let mut message_ids: Vec<MessageId> = Vec::new();
    let mut hasheds_id_to_prune: Vec<(HashedIndex, MessageId)> = Vec::new();
    let mut messages_id_pairs_to_prune: Vec<(MessageId, MessageId)> = Vec::new();

    // Get the unconfirmed (unreferenced) messages
    let unconfirmed_messages =
        Fetch::<MilestoneIndex, Vec<UnconfirmedMessage>>::fetch(&*storage, &purning_milestone_index)
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?
            .unwrap();
    // Record the count of unconfirmed (unreferenced) messages to prune
    let pruned_unconfirmed_message_count = unconfirmed_messages.len();

    // Start the batch operation
    let mut batch = B::batch_begin();
    for msg in unconfirmed_messages {
        // Delete the (MilestoneIndex, UnconfirmedMessage) pairs
        Batch::<(MilestoneIndex, UnconfirmedMessage), ()>::batch_delete(
            &*storage,
            &mut batch,
            &(*purning_milestone_index, msg),
        )
        .map_err(|e| Error::Storage(Box::new(e)))?;

        // Record other pairs to be deleted
        let msg_content = Fetch::<MessageId, Message>::fetch(&*storage, &*msg)
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?
            .unwrap();
        message_ids.push(*msg.message_id());
        if let Some(Payload::Indexation(payload)) = msg_content.payload() {
            hasheds_id_to_prune.push((payload.hash(), *msg));
        }
        for parent in msg_content.parents() {
            messages_id_pairs_to_prune.push((*parent, *msg))
        }
    }
    // Prune the (MilestoneIndex, UnconfirmedMessage) pairs
    storage
        .batch_commit(batch, true)
        .await
        .map_err(|e| Error::Storage(Box::new(e)))?;

    // Prune the messages
    prune_messages(storage, message_ids, hasheds_id_to_prune, messages_id_pairs_to_prune).await?;
    Ok(pruned_unconfirmed_message_count)
}

/// Prunes the (`MessageId`, `Message`), (`MessageId`, `MessageMetadata`), (`HashedIndex`, `MessageId`), and
/// (`MessageId`, `MessageId`) pairs from the database.
#[allow(dead_code)]
pub async fn prune_messages<B: StorageBackend>(
    storage: &B,
    message_ids: Vec<MessageId>,
    hasheds_id_to_prune: Vec<(HashedIndex, MessageId)>,
    messages_id_pairs_to_prune: Vec<(MessageId, MessageId)>,
) -> Result<(), Error> {
    let mut batch = B::batch_begin();
    for id in message_ids {
        Batch::<MessageId, Message>::batch_delete(&*storage, &mut batch, &id)
            .map_err(|e| Error::Storage(Box::new(e)))?;
        Batch::<MessageId, MessageMetadata>::batch_delete(&*storage, &mut batch, &id)
            .map_err(|e| Error::Storage(Box::new(e)))?;
    }
    for (hashed_id, id) in hasheds_id_to_prune {
        Batch::<(HashedIndex, MessageId), ()>::batch_delete(&*storage, &mut batch, &(hashed_id, id))
            .map_err(|e| Error::Storage(Box::new(e)))?;
    }
    for (parent, child) in messages_id_pairs_to_prune {
        Batch::<(MessageId, MessageId), ()>::batch_delete(&*storage, &mut batch, &(parent, child))
            .map_err(|e| Error::Storage(Box::new(e)))?;
    }
    storage
        .batch_commit(batch, true)
        .await
        .map_err(|e| Error::Storage(Box::new(e)))?;
    Ok(())
}

/// Prunes the `Milestone` and `OutputDiff` from the database.
#[allow(dead_code)]
pub async fn prune_milestone<B: StorageBackend>(storage: &B, milestone_index: MilestoneIndex) -> Result<(), Error> {
    // Delete milestone.
    Delete::<MilestoneIndex, Milestone>::delete(&*storage, &milestone_index)
        .await
        .map_err(|e| Error::Storage(Box::new(e)))?;
    // TODO: solve the OutputDiff cyclic dependency issue
    // Delete output_diff.
    // Delete::<MilestoneIndex, OutputDiff>::delete(&*storage, &milestone_index)
    //     .await
    //     .map_err(|e| Error::Storage(Box::new(e)))?;
    Ok(())
}

// NOTE we don't prune cache, but only prune the database.
#[allow(dead_code)]
pub async fn prune_database<B: StorageBackend>(
    tangle: &MsTangle<B>,
    storage: &B,
    mut target_index: MilestoneIndex,
) -> Result<(), Error> {
    let target_index_max = MilestoneIndex(
        *tangle.get_snapshot_index() - SOLID_ENTRY_POINT_CHECK_THRESHOLD_PAST - ADDITIONAL_PRUNING_THRESHOLD - 1,
    );
    if target_index > target_index_max {
        target_index = target_index_max;
    }
    // Update the solid entry points in the static MsTangle.
    let new_solid_entry_points = get_new_solid_entry_points(tangle, target_index).await?;

    // Clear the solid_entry_points in the static MsTangle.
    tangle.clear_solid_entry_points().await;

    // TODO update the whole solid_entry_points in the static MsTangle w/o looping.
    for (id, milestone_index) in new_solid_entry_points.into_iter() {
        tangle
            .add_solid_entry_point(SolidEntryPoint::from(id), milestone_index)
            .await;
    }

    // We have to set the new solid entry point index.
    // This way we can cleanly prune even if the pruning was aborted last time.
    tangle.update_entry_point_index(target_index);

    prune_unreferenced_messages(storage, &tangle.get_pruning_index()).await?;

    // Iterate through all milestones that have to be pruned.
    for milestone_index in *tangle.get_pruning_index() + 1..*target_index + 1 {
        info!("Pruning milestone {}...", milestone_index);

        let mut deleted_messages = prune_unreferenced_messages(storage, &MilestoneIndex(milestone_index)).await?;

        if let Some(milestone_message_id) = tangle.get_milestone_message_id(MilestoneIndex(milestone_index)).await {
            let mut messages_to_prune: Vec<MessageId> = Vec::new();
            let mut hasheds_id_to_prune: Vec<(HashedIndex, MessageId)> = Vec::new();
            let mut messages_id_pairs_to_prune: Vec<(MessageId, MessageId)> = Vec::new();

            // Traverse the approvees of the milestone transaction.
            traversal::visit_parents_depth_first(
                tangle,
                milestone_message_id,
                |_, _, _| {
                    // NOTE everything that was referenced by that milestone can be pruned
                    //      (even transactions of older milestones)
                    async move { true }
                },
                |id, msg, _| {
                    messages_to_prune.push(*id);
                    if let Some(Payload::Indexation(payload)) = msg.payload() {
                        hasheds_id_to_prune.push((payload.hash(), *id));
                    }
                    for parent in msg.parents() {
                        messages_id_pairs_to_prune.push((*parent, *id))
                    }
                },
                |_, _, _| {},
                |_| {},
            )
            .await;

            // NOTE The metadata of solid entry points can be deleted from the database,
            //      because we only need the hashes of them, and don't have to trace their parents.
            prune_milestone(storage, MilestoneIndex(milestone_index)).await?;

            deleted_messages += messages_to_prune.len();
            prune_messages(
                storage,
                messages_to_prune,
                hasheds_id_to_prune,
                messages_id_pairs_to_prune,
            )
            .await?;

            tangle.update_pruning_index(MilestoneIndex(milestone_index));
            info!(
                "Pruning milestone {}. Pruned {} messages.",
                milestone_index, deleted_messages
            );
            // TODO trigger pruning milestone index changed event if needed.
            //      notify peers about our new pruning milestone index by
            //      broadcast_heartbeat()
        } else {
            warn!("Pruning milestone {} failed! Milestone not found!", milestone_index);
            continue;
        }
    }
    Ok(())
}

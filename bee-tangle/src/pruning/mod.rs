// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod config;

use crate::MsTangle;
use bee_message::{milestone::MilestoneIndex, MessageId};
pub use config::{PruningConfig, PruningConfigBuilder};
use futures::executor;
use hashbrown::HashMap;

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
}

// // TODO testing
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
                    metadata.milestone_index().is_some() && *metadata.milestone_index().unwrap() >= index
                },
                |id, _, metadata| {
                    if !executor::block_on(tangle.is_solid_entry_point(id)) {
                        if metadata.milestone_index().is_none() {
                            solid_entry_points.insert(*id, metadata.milestone_index().unwrap());
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

// TODO: remove the unconfirmed messages in the database and return (the number of pruned txs,
//       the number of chcked messages)
#[allow(dead_code)]
pub async fn prune_unreferenced_messages(_purning_milestone_index: &MilestoneIndex) -> (usize, usize) {
    unimplemented!()
}

// TODO remove the confirmed transactions in the database and return the deleted tx count.
#[allow(dead_code)]
pub async fn prune_messages(_hashes: Vec<MessageId>) -> usize {
    unimplemented!()
}

// TODO prunes the milestone metadata and the ledger diffs from the database for the given milestone.
#[allow(dead_code)]
pub async fn prune_milestone(_milestone_index: MilestoneIndex) {
    // Delete ledger_diff for milestone with milestone_index.
    // Delete milestone storage (if we have this) for milestone with milestone_index.
    unimplemented!()
}

// NOTE we don't prune cache, but only prune the database.
#[allow(dead_code)]
pub async fn prune_database<B: StorageBackend>(
    tangle: &MsTangle<B>,
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
        tangle.add_solid_entry_point(id, milestone_index).await;
    }

    // We have to set the new solid entry point index.
    // This way we can cleanly prune even if the pruning was aborted last time.
    tangle.update_entry_point_index(target_index);

    prune_unreferenced_messages(&tangle.get_pruning_index()).await;

    // Iterate through all milestones that have to be pruned.
    for milestone_index in *tangle.get_pruning_index() + 1..*target_index + 1 {
        info!("Pruning milestone {}...", milestone_index);

        let (mut deleted_transactions, mut checked_mesages) =
            prune_unreferenced_messages(&MilestoneIndex(milestone_index)).await;

        if let Some(milestone_message_id) = tangle.get_milestone_message_id(MilestoneIndex(milestone_index)).await {
            let mut messages_to_prune: Vec<MessageId> = Vec::new();

            // Traverse the approvees of the milestone transaction.
            traversal::visit_parents_depth_first(
                tangle,
                milestone_message_id,
                |_, _, _| {
                    // NOTE everything that was referenced by that milestone can be pruned
                    //      (even transactions of older milestones)
                    async move { true }
                },
                |id, _, _| messages_to_prune.push(*id),
                |_, _, _| {},
                |_| {},
            )
            .await;

            // // NOTE The metadata of solid entry points can be deleted from the database,
            // //      because we only need the hashes of them, and don't have to trace their parents.
            prune_milestone(MilestoneIndex(milestone_index)).await;

            checked_mesages += messages_to_prune.len();
            deleted_transactions += prune_messages(messages_to_prune).await;

            tangle.update_pruning_index(MilestoneIndex(milestone_index));
            info!(
                "Pruning milestone {}. Pruned {}/{} messages.",
                milestone_index, deleted_transactions, checked_mesages
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

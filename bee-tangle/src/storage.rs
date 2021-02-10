// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use crate::{metadata::MessageMetadata, unconfirmed_message::UnconfirmedMessage};

// use bee_ledger::model::OutputDiff;
use bee_message::{
    milestone::{Milestone, MilestoneIndex},
    payload::indexation::HashedIndex,
    Message, MessageId,
};
use bee_snapshot::storage::StorageBackend as SnapshotStorageBackend;
use bee_storage::{
    access::{Batch, BatchBuilder, Delete, Fetch, Insert},
    backend,
};

pub trait StorageBackend:
    backend::StorageBackend
    + BatchBuilder
    + Batch<MessageId, Message>
    + Batch<(MessageId, MessageId), ()>
    + Batch<MessageId, MessageMetadata>
    + Batch<(MilestoneIndex, UnconfirmedMessage), ()>
    + Batch<(HashedIndex, MessageId), ()>
    + Delete<MilestoneIndex, Milestone>
    // + Delete<MilestoneIndex, OutputDiff>
    + Insert<MessageId, Message>
    + Insert<MessageId, MessageMetadata>
    + Insert<(MessageId, MessageId), ()>
    + Insert<MilestoneIndex, Milestone>
    + Fetch<MessageId, Message>
    + Fetch<MessageId, MessageMetadata>
    + Fetch<MessageId, Vec<MessageId>>
    + Fetch<MilestoneIndex, Milestone>
    + Fetch<MilestoneIndex, Vec<UnconfirmedMessage>>
    + SnapshotStorageBackend
{
}

impl<T> StorageBackend for T where
    T: backend::StorageBackend
        + BatchBuilder
        + Batch<MessageId, Message>
        + Batch<(MessageId, MessageId), ()>
        + Batch<MessageId, MessageMetadata>
        + Batch<(MilestoneIndex, UnconfirmedMessage), ()>
        + Batch<(HashedIndex, MessageId), ()>
        + Delete<MilestoneIndex, Milestone>
        // + Delete<MilestoneIndex, OutputDiff>
        + Insert<MessageId, Message>
        + Insert<MessageId, MessageMetadata>
        + Insert<(MessageId, MessageId), ()>
        + Insert<MilestoneIndex, Milestone>
        + Fetch<MessageId, Message>
        + Fetch<MessageId, MessageMetadata>
        + Fetch<MessageId, Vec<MessageId>>
        + Fetch<MilestoneIndex, Milestone>
        + Fetch<MilestoneIndex, Vec<UnconfirmedMessage>>
        + SnapshotStorageBackend
{
}

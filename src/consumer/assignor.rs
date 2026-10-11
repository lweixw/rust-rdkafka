//! Application partition assignors for the classic consumer group protocol.
//!
//! A [`PartitionAssignor`] takes part in the classic group protocol exactly as
//! librdkafka's built-in assignors do: every member's JoinGroup metadata
//! carries the bytes its [`subscription_userdata`] returns, the group leader's
//! [`assign`] decides each member's partitions and userdata, and every member's
//! [`on_assignment`] receives the partitions and userdata the leader assigned
//! to it.
//!
//! The assignor is reached through [`ConsumerContext::assignor`] and registered
//! with librdkafka when the consumer is created. `partition.assignment.strategy`
//! must then name it, alone or alongside built-in assignors of the same
//! protocol; a registration the strategy does not name is unused. Creating the
//! consumer fails when the name is empty or contains a comma, a space, a tab
//! or a line break, when it is a built-in assignor's name, or when
//! `heartbeat.interval.ms` is not less than `session.timeout.ms`; a name with
//! a NUL fails with [`KafkaError::Nul`].
//!
//! All three callbacks run on librdkafka's internal main thread, outside any
//! async runtime. They must not block and must not call the consumer: methods
//! such as [`Consumer::assignment`], [`Consumer::member_id`] and
//! [`Consumer::committed`] wait for that thread. `assign` receives an
//! [`AssignmentTask`] that it may complete inline or hand to another thread
//! (through a stored runtime handle, say); once `assign` has returned, the
//! thread holding the task may call the consumer. No heartbeat is sent while
//! the assignment is pending and every member's SyncGroup waits for it, so the
//! task must complete within `session.timeout.ms` less `heartbeat.interval.ms`
//! of the JoinGroup response, or the library gives the assignment up and
//! rejoins. A panic in a callback is caught at the FFI boundary and logged: a
//! panicking `subscription_userdata` sends no userdata, a panicking `assign`
//! fails the assignment (the group rejoins) unless the task was already
//! completed or handed off, and a panicking `on_assignment` leaves the
//! assignment applied.
//!
//! Under [`AssignorProtocol::Cooperative`] the post-assignment adjustment the
//! built-in assignors rely on is not applied: the assignor itself withholds a
//! partition that moves between members (assigning it to nobody, so that its
//! owner revokes it and a follow-up rebalance hands it over). An assignment
//! that moves a partition between members in one round, or assigns a partition
//! twice, fails and the group rejoins.
//!
//! [`subscription_userdata`]: PartitionAssignor::subscription_userdata
//! [`assign`]: PartitionAssignor::assign
//! [`on_assignment`]: PartitionAssignor::on_assignment
//! [`Consumer::assignment`]: crate::consumer::Consumer::assignment
//! [`Consumer::member_id`]: crate::consumer::Consumer::member_id
//! [`Consumer::committed`]: crate::consumer::Consumer::committed
//! [`KafkaError::Nul`]: crate::error::KafkaError::Nul

use std::any::Any;
use std::borrow::Cow;
use std::ffi::{CStr, CString};
use std::fmt;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::os::raw::{c_char, c_void};
use std::panic::{self, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::config::NativeClientConfig;
use crate::consumer::ConsumerContext;
use crate::error::KafkaResult;
use crate::log::error;
use crate::metadata::Metadata;
use crate::topic_partition_list::TopicPartitionList;
use crate::util::{cstr_to_owned, ptr_to_slice};

/// The rebalance protocol an application assignor speaks.
///
/// A consumer that has joined reports its protocol as a
/// [`RebalanceProtocol`](crate::consumer::RebalanceProtocol), whose `None`
/// (not yet joined) has no place in a registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssignorProtocol {
    /// Every member gives up all its partitions at the start of a rebalance
    /// and receives its new assignment whole.
    Eager,
    /// Members keep the partitions they own across a rebalance; a partition
    /// moves only after its owner has revoked it.
    Cooperative,
}

/// This consumer's group identity at the time of an assignment (what Java
/// passes to `onAssignment` as its `ConsumerGroupMetadata`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssignedGroup {
    /// The group id.
    pub group_id: String,
    /// The generation the assignment belongs to.
    pub generation_id: i32,
    /// This consumer's member id.
    pub member_id: String,
    /// This consumer's `group.instance.id`, if it has one.
    pub group_instance_id: Option<String>,
}

impl AssignedGroup {
    unsafe fn from_ptr(ptr: *const RDKafkaConsumerGroupMetadata) -> AssignedGroup {
        let group_instance_id = rdsys::rd_kafka_consumer_group_metadata_group_instance_id(ptr);
        AssignedGroup {
            group_id: cstr_to_owned(rdsys::rd_kafka_consumer_group_metadata_group_id(ptr)),
            generation_id: rdsys::rd_kafka_consumer_group_metadata_generation_id(ptr),
            member_id: cstr_to_owned(rdsys::rd_kafka_consumer_group_metadata_member_id(ptr)),
            group_instance_id: if group_instance_id.is_null() {
                None
            } else {
                Some(cstr_to_owned(group_instance_id))
            },
        }
    }
}

/// An application partition assignor, see the [module documentation](self).
///
/// The callbacks run on librdkafka's thread while the application may use
/// the same assignor from its own, hence the bounds.
pub trait PartitionAssignor: Send + Sync {
    /// The name `partition.assignment.strategy` selects the assignor by.
    ///
    /// Non-empty, without commas, spaces, tabs, line breaks or NULs, and not a
    /// built-in assignor's name (`range`, `roundrobin`, `cooperative-sticky`).
    fn name(&self) -> &str;

    /// The rebalance protocol the assignor speaks.
    fn protocol(&self) -> AssignorProtocol;

    /// Produces the userdata this member puts into its JoinGroup metadata.
    ///
    /// `topics` are the subscribed topic names and `owned_partitions` the
    /// partitions this member owns going into the rebalance: `None` when it
    /// owns nothing yet, which under [`AssignorProtocol::Eager`] is always (the
    /// leader then reads the same member's owned partitions back as an empty
    /// list). An empty return sends no userdata, which is the default.
    #[allow(unused_variables)]
    fn subscription_userdata(
        &self,
        topics: &[&str],
        owned_partitions: Option<&TopicPartitionList>,
    ) -> Vec<u8> {
        Vec::new()
    }

    /// Assigns partitions and userdata to every member of the group; called on
    /// the leader only.
    ///
    /// `member_id` is this consumer's own member id and `metadata` the cluster
    /// metadata for the subscribed topics as received: a topic may carry an
    /// error ([`MetadataTopic::error`]) or no partitions, and `topic.blacklist`
    /// is not applied. Both are valid during the call only, so an assignor
    /// that completes the `task` elsewhere copies what it needs first.
    ///
    /// [`MetadataTopic::error`]: crate::metadata::MetadataTopic::error
    fn assign(&self, member_id: &str, metadata: &Metadata, task: AssignmentTask);

    /// Receives the partitions and userdata the leader assigned to this
    /// member, before the assignment is applied.
    ///
    /// Called on every successful SyncGroup, with an empty list and empty
    /// userdata when the leader assigned this member nothing; not called when
    /// the SyncGroup fails. `userdata` is empty when the leader assigned none.
    #[allow(unused_variables)]
    fn on_assignment(
        &self,
        assignment: &TopicPartitionList,
        userdata: &[u8],
        group: &AssignedGroup,
    ) {
    }
}

/// One assignment the leader's [`PartitionAssignor::assign`] has to make.
///
/// The task owns the group's members for as long as it lives. Read them with
/// [`member`](AssignmentTask::member), set each member's assignment and
/// userdata with [`set_assignment`](AssignmentTask::set_assignment) and
/// [`set_userdata`](AssignmentTask::set_userdata), then
/// [`complete`](AssignmentTask::complete) the task; the members' assignments
/// are then sent to the group, and a member left unset gets no partitions and
/// no userdata. The task may be moved to another thread and completed there.
/// A task that is dropped without being completed fails the assignment and
/// the group rejoins, as does [`fail`](AssignmentTask::fail).
///
/// A task completed after the group rejoined, unsubscribed, picked another
/// assignor or gave the assignment up, or after the consumer was dropped, is
/// discarded. A task that is neither completed nor dropped (`mem::forget`)
/// leaks its members, and the library gives the assignment up when its budget
/// runs out.
pub struct AssignmentTask {
    pending: *mut RDKafkaAssignorPending,
    members: *mut RDKafkaAssignorMember,
    member_count: usize,
}

// SAFETY: librdkafka does not touch the members while the assignment is
// pending, and `rd_kafka_assignor_complete` may be called from any thread.
unsafe impl Send for AssignmentTask {}

impl AssignmentTask {
    /// The number of members in the group.
    pub fn member_count(&self) -> usize {
        self.member_count
    }

    /// The member at `idx`, which must be less than
    /// [`member_count`](AssignmentTask::member_count).
    pub fn member(&self, idx: usize) -> GroupMember<'_> {
        GroupMember {
            ptr: self.member_ptr(idx),
            _task: PhantomData,
        }
    }

    /// Sets the partitions assigned to the member at `idx`; topic and
    /// partition are copied, nothing else.
    pub fn set_assignment(&mut self, idx: usize, assignment: &TopicPartitionList) {
        let member = self.member_ptr(idx);
        unsafe { rdsys::rd_kafka_assignor_member_set_assignment(member, assignment.ptr()) }
    }

    /// Sets the userdata assigned to the member at `idx`; empty means none.
    ///
    /// The protocol carries at most `i32::MAX` bytes.
    pub fn set_userdata(&mut self, idx: usize, userdata: &[u8]) {
        assert_userdata_fits(userdata);
        let member = self.member_ptr(idx);
        unsafe {
            rdsys::rd_kafka_assignor_member_set_userdata(
                member,
                userdata.as_ptr() as *const c_void,
                userdata.len(),
            )
        }
    }

    fn member_ptr(&self, idx: usize) -> *mut RDKafkaAssignorMember {
        assert!(
            idx < self.member_count,
            "member index {} out of range for {} members",
            idx,
            self.member_count
        );
        unsafe { rdsys::rd_kafka_assignor_member_at(self.members, idx) }
    }

    /// Sends the members' assignments and userdata to the group.
    pub fn complete(self) {
        self.finish(RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR, None);
    }

    /// Fails the assignment; `reason` is logged and the group rejoins.
    pub fn fail(self, reason: &str) {
        self.finish(RDKafkaRespErr::RD_KAFKA_RESP_ERR__FAIL, Some(reason));
    }

    fn finish(mut self, err: RDKafkaRespErr, reason: Option<&str>) {
        let reason = reason.map(|reason| {
            CString::new(reason.replace('\0', "")).expect("interior NULs were removed")
        });
        self.complete_pending(err, reason.as_ref().map_or(ptr::null(), |r| r.as_ptr()));
    }

    /// Completes the pending handle once; later calls do nothing.
    fn complete_pending(&mut self, err: RDKafkaRespErr, reason: *const c_char) {
        let pending = std::mem::replace(&mut self.pending, ptr::null_mut());
        if !pending.is_null() {
            unsafe { rdsys::rd_kafka_assignor_complete(pending, err, reason) }
        }
    }
}

const DROPPED: &[u8] = b"assignment task dropped without completing\0";

impl Drop for AssignmentTask {
    fn drop(&mut self) {
        self.complete_pending(
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__FAIL,
            DROPPED.as_ptr() as *const c_char,
        );
    }
}

impl fmt::Debug for AssignmentTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AssignmentTask")
            .field("member_count", &self.member_count)
            .field("completed", &self.pending.is_null())
            .finish()
    }
}

/// One member of the group, as seen by the leader's
/// [`PartitionAssignor::assign`]. Reads only; the task sets. The reads borrow
/// the task, so they may be collected across members and live until the task
/// is set or completed.
///
/// The member's rack, topic names and owned partitions are the bytes it put
/// into its JoinGroup metadata, which nothing validates; they are decoded
/// lossily, as Java does, so a member sending non-UTF-8 cannot fail the
/// leader.
pub struct GroupMember<'a> {
    ptr: *const RDKafkaAssignorMember,
    _task: PhantomData<&'a AssignmentTask>,
}

impl<'a> GroupMember<'a> {
    /// The member id.
    pub fn id(&self) -> Cow<'a, str> {
        unsafe { lossy_str(rdsys::rd_kafka_assignor_member_id(self.ptr)) }
    }

    /// The member's `group.instance.id`, if it has one.
    pub fn group_instance_id(&self) -> Option<Cow<'a, str>> {
        unsafe { opt_lossy_str(rdsys::rd_kafka_assignor_member_group_instance_id(self.ptr)) }
    }

    /// The member's `client.rack`, if it reported one.
    pub fn rack_id(&self) -> Option<Cow<'a, str>> {
        unsafe { opt_lossy_str(rdsys::rd_kafka_assignor_member_rack_id(self.ptr)) }
    }

    /// The topics the member subscribed to.
    pub fn subscription(&self) -> Vec<Cow<'a, str>> {
        let list = unsafe { rdsys::rd_kafka_assignor_member_subscription(self.ptr) };
        unsafe { list_elems(list) }
            .iter()
            .map(|e| unsafe { lossy_str(e.topic) })
            .collect()
    }

    /// A copy of the partitions the member owns going into the rebalance, or
    /// `None` for a member whose subscription metadata predates owned
    /// partitions.
    pub fn owned_partitions(&self) -> Option<TopicPartitionList> {
        let ptr = unsafe { rdsys::rd_kafka_assignor_member_owned_partitions(self.ptr) };
        if ptr.is_null() {
            return None;
        }
        let elems = unsafe { list_elems(ptr) };
        let mut list = TopicPartitionList::with_capacity(elems.len());
        for e in elems {
            list.add_partition(&unsafe { lossy_str(e.topic) }, e.partition);
        }
        Some(list)
    }

    /// The userdata the member's subscription carried; empty when none.
    pub fn userdata(&self) -> &'a [u8] {
        let mut size = 0;
        let data = unsafe { rdsys::rd_kafka_assignor_member_userdata(self.ptr, &mut size) };
        unsafe { ptr_to_slice(data, size) }
    }
}

impl fmt::Debug for GroupMember<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupMember")
            .field("id", &self.id())
            .finish()
    }
}

fn assert_userdata_fits(userdata: &[u8]) {
    assert!(
        userdata.len() <= i32::MAX as usize,
        "assignor userdata of {} bytes exceeds the protocol's limit of {}",
        userdata.len(),
        i32::MAX
    );
}

/// A string librdkafka hands out, decoded lossily; a null pointer is empty.
unsafe fn lossy_str<'a>(ptr: *const c_char) -> Cow<'a, str> {
    if ptr.is_null() {
        Cow::Borrowed("")
    } else {
        CStr::from_ptr(ptr).to_string_lossy()
    }
}

unsafe fn opt_lossy_str<'a>(ptr: *const c_char) -> Option<Cow<'a, str>> {
    if ptr.is_null() {
        None
    } else {
        Some(lossy_str(ptr))
    }
}

/// The elements of a list librdkafka owns; an empty list may have no element
/// array at all.
unsafe fn list_elems<'a>(list: *const RDKafkaTopicPartitionList) -> &'a [RDKafkaTopicPartition] {
    if list.is_null() {
        return &[];
    }
    ptr_to_slice((*list).elems as *const c_void, (*list).cnt.max(0) as usize)
}

/// Copies a list librdkafka owns into a `TopicPartitionList` of our own.
/// `TopicPartitionList` hands out writable elements from a shared reference,
/// so a view over librdkafka's `const` list would let safe code write into
/// memory the library goes on to use.
unsafe fn copied_list(ptr: *const RDKafkaTopicPartitionList) -> TopicPartitionList {
    TopicPartitionList::from_ptr(rdsys::rd_kafka_topic_partition_list_copy(ptr))
}

/// Logs a panic a callback caught. The payload is dropped and the logger is
/// called under their own catch, so that neither can unwind across the
/// `extern "C"` boundary.
fn log_caught_panic(payload: Box<dyn Any + Send>, message: &str) {
    let again = panic::catch_unwind(AssertUnwindSafe(move || {
        drop(payload);
        error!("{}", message);
    }));
    if let Err(payload) = again {
        // A payload whose drop panics is leaked rather than dropped here.
        std::mem::forget(payload);
    }
}

/// Registers the context's assignor, if it has one, on the native
/// configuration, with the context as the callbacks' opaque; the `Arc` must
/// outlive the client.
pub(crate) fn register<C: ConsumerContext>(
    native_config: &NativeClientConfig,
    context: &Arc<C>,
) -> KafkaResult<()> {
    let Some(assignor) = context.assignor() else {
        return Ok(());
    };
    let name = CString::new(assignor.name())?;
    let protocol = match assignor.protocol() {
        AssignorProtocol::Eager => RDKafkaAssignorProtocol::RD_KAFKA_ASSIGNOR_PROTOCOL_EAGER,
        AssignorProtocol::Cooperative => {
            RDKafkaAssignorProtocol::RD_KAFKA_ASSIGNOR_PROTOCOL_COOPERATIVE
        }
    };
    unsafe {
        rdsys::rd_kafka_conf_set_assignor(
            native_config.ptr(),
            name.as_ptr(),
            protocol,
            Some(subscription_cb::<C>),
            Some(assign_cb::<C>),
            Some(on_assignment_cb::<C>),
            Arc::as_ptr(context) as *mut c_void,
        )
    };
    Ok(())
}

unsafe fn assignor_of<'a, C: ConsumerContext + 'a>(
    opaque: *mut c_void,
) -> &'a dyn PartitionAssignor {
    let context = &*(opaque as *const C);
    context
        .assignor()
        .expect("the context registered an assignor and must keep returning it")
}

unsafe extern "C" fn subscription_cb<C: ConsumerContext>(
    rk: *mut RDKafka,
    topics: *const *const c_char,
    topic_cnt: usize,
    owned_partitions: *const RDKafkaTopicPartitionList,
    userdata: *mut *mut c_void,
    userdata_size: *mut usize,
    opaque: *mut c_void,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let topics: Vec<Cow<'_, str>> =
            ptr_to_slice::<*const c_char>(topics as *const c_void, topic_cnt)
                .iter()
                .map(|&t| lossy_str(t))
                .collect();
        let topics: Vec<&str> = topics.iter().map(|t| &**t).collect();
        let owned = if owned_partitions.is_null() {
            None
        } else {
            Some(copied_list(owned_partitions))
        };
        let bytes = assignor_of::<C>(opaque).subscription_userdata(&topics, owned.as_ref());
        assert_userdata_fits(&bytes);
        bytes
    }));
    match result {
        Ok(bytes) if !bytes.is_empty() => {
            let buf = rdsys::rd_kafka_mem_malloc(rk, bytes.len());
            ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, bytes.len());
            *userdata = buf;
            *userdata_size = bytes.len();
        }
        Ok(_) => {}
        Err(payload) => log_caught_panic(
            payload,
            "partition assignor panicked in subscription_userdata; sending none",
        ),
    }
}

unsafe extern "C" fn assign_cb<C: ConsumerContext>(
    _rk: *mut RDKafka,
    member_id: *const c_char,
    metadata: *const RDKafkaMetadata,
    members: *mut RDKafkaAssignorMember,
    member_cnt: usize,
    pending: *mut RDKafkaAssignorPending,
    _errstr: *mut c_char,
    _errstr_size: usize,
    opaque: *mut c_void,
) -> RDKafkaAssignorResult {
    let task = AssignmentTask {
        pending,
        members,
        member_count: member_cnt,
    };
    // The task completes the assignment, inline or later: a task the assignor
    // drops, including by panicking, completes it with an error.
    let result = panic::catch_unwind(AssertUnwindSafe(move || {
        let metadata = ManuallyDrop::new(Metadata::from_ptr(metadata));
        assignor_of::<C>(opaque).assign(&lossy_str(member_id), &metadata, task);
    }));
    if let Err(payload) = result {
        log_caught_panic(payload, "partition assignor panicked in assign");
    }
    RDKafkaAssignorResult::RD_KAFKA_ASSIGNOR_PENDING
}

unsafe extern "C" fn on_assignment_cb<C: ConsumerContext>(
    _rk: *mut RDKafka,
    assignment: *const RDKafkaTopicPartitionList,
    userdata: *const c_void,
    userdata_size: usize,
    group_metadata: *const RDKafkaConsumerGroupMetadata,
    opaque: *mut c_void,
) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let assignment = copied_list(assignment);
        let userdata = ptr_to_slice(userdata, userdata_size);
        let group = AssignedGroup::from_ptr(group_metadata);
        assignor_of::<C>(opaque).on_assignment(&assignment, userdata, &group);
    }));
    if let Err(payload) = result {
        log_caught_panic(
            payload,
            "partition assignor panicked in on_assignment; the assignment stands",
        );
    }
}

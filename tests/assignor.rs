//! Test the application partition assignor against a broker.

use std::collections::BTreeMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rdkafka::admin::AdminOptions;
use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{
    AssignedGroup, AssignmentTask, AssignorProtocol, BaseConsumer, Consumer, ConsumerContext,
    PartitionAssignor, RebalanceProtocol,
};
use rdkafka::error::KafkaError;
use rdkafka::metadata::Metadata;
use rdkafka::{ClientConfig, TopicPartitionList};

use crate::utils::admin;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::rand::*;

mod utils;

const ASSIGNOR_NAME: &str = "test-assignor";

/// What `subscription_userdata` was given in one call.
#[derive(Clone, Debug, PartialEq)]
struct SubscriptionCall {
    topics: Vec<String>,
    owned: Option<Vec<(String, i32)>>,
}

/// What the leader saw in one `assign` call, members in id order.
#[derive(Clone, Debug)]
struct AssignCall {
    leader_id: String,
    member_ids: Vec<String>,
    instance_ids: Vec<Option<String>>,
    rack_ids: Vec<Option<String>>,
    subscriptions: Vec<Vec<String>>,
    owned: Vec<Option<Vec<(String, i32)>>>,
    userdata: Vec<Vec<u8>>,
}

/// What a member received in one `on_assignment` call.
#[derive(Clone, Debug)]
struct Assigned {
    partitions: Vec<(String, i32)>,
    userdata: Vec<u8>,
    group: AssignedGroup,
}

/// How the leader completes a task.
#[derive(Clone, Copy, Debug)]
enum Completion {
    /// Complete from a second thread after a short delay.
    Thread,
    /// Complete inline, inside the callback.
    Inline,
}

/// Assigns partition `p` of a topic to the `p % n`-th of the `n` members
/// subscribed to it (members in id order); under COOPERATIVE a partition
/// owned by a member other than its target is withheld for that round,
/// except once when `violate_once` is set, when it is moved in the same
/// round (which the library rejects). The userdata assigned to a member is
/// its subscription userdata reversed, with a suffix.
struct RoundRobinAssignor {
    tag: Vec<u8>,
    protocol: AssignorProtocol,
    completion: Completion,
    assign_calls: Mutex<Vec<AssignCall>>,
    assignments: Mutex<Vec<Assigned>>,
    subscription_calls: Mutex<Vec<SubscriptionCall>>,
    panic_subscription: AtomicBool,
    violate_once: AtomicBool,
}

impl RoundRobinAssignor {
    fn new(tag: &[u8], protocol: AssignorProtocol, completion: Completion) -> Arc<Self> {
        Arc::new(RoundRobinAssignor {
            tag: tag.to_vec(),
            protocol,
            completion,
            assign_calls: Mutex::new(Vec::new()),
            assignments: Mutex::new(Vec::new()),
            subscription_calls: Mutex::new(Vec::new()),
            panic_subscription: AtomicBool::new(false),
            violate_once: AtomicBool::new(false),
        })
    }
}

const USERDATA_SUFFIX: &[u8] = b"|assigned\xE2\x9C\x93";

fn partitions_of(list: &TopicPartitionList) -> Vec<(String, i32)> {
    let mut out: Vec<_> = list
        .elements()
        .iter()
        .map(|e| (e.topic().to_owned(), e.partition()))
        .collect();
    out.sort();
    out
}

fn sorted(topics: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = topics.iter().map(|t| t.to_string()).collect();
    out.sort();
    out
}

impl PartitionAssignor for RoundRobinAssignor {
    fn name(&self) -> &str {
        ASSIGNOR_NAME
    }

    fn protocol(&self) -> AssignorProtocol {
        self.protocol
    }

    fn subscription_userdata(
        &self,
        topics: &[&str],
        owned_partitions: Option<&TopicPartitionList>,
    ) -> Vec<u8> {
        self.subscription_calls
            .lock()
            .unwrap()
            .push(SubscriptionCall {
                topics: sorted(topics),
                owned: owned_partitions.map(partitions_of),
            });
        if self.panic_subscription.load(Ordering::SeqCst) {
            panic!("the assignor panics while producing its subscription userdata");
        }
        self.tag.clone()
    }

    fn assign(&self, member_id: &str, metadata: &Metadata, task: AssignmentTask) {
        // The metadata is valid during the call only: copy the partition
        // counts before the task leaves the thread.
        let partition_counts: BTreeMap<String, i32> = metadata
            .topics()
            .iter()
            .filter(|t| t.error().is_none())
            .map(|t| (t.name().to_owned(), t.partitions().len() as i32))
            .collect();

        let n = task.member_count();
        let mut members: Vec<(String, usize)> = (0..n)
            .map(|i| (task.member(i).id().to_owned(), i))
            .collect();
        members.sort();

        let mut call = AssignCall {
            leader_id: member_id.to_owned(),
            member_ids: Vec::new(),
            instance_ids: Vec::new(),
            rack_ids: Vec::new(),
            subscriptions: Vec::new(),
            owned: Vec::new(),
            userdata: Vec::new(),
        };
        let mut owner_of: BTreeMap<(String, i32), usize> = BTreeMap::new();
        for (rank, (id, idx)) in members.iter().enumerate() {
            let member = task.member(*idx);
            let owned = member.owned_partitions().map(|l| partitions_of(&l));
            if let Some(owned) = &owned {
                for tp in owned {
                    owner_of.insert(tp.clone(), rank);
                }
            }
            call.member_ids.push(id.clone());
            call.instance_ids
                .push(member.group_instance_id().map(str::to_owned));
            call.rack_ids.push(member.rack_id().map(str::to_owned));
            call.subscriptions.push(sorted(&member.subscription()));
            call.owned.push(owned);
            call.userdata.push(member.userdata().to_vec());
        }
        let subscriptions = call.subscriptions.clone();
        self.assign_calls.lock().unwrap().push(call);

        let mut plan = Vec::new();
        let mut any_withheld = false;
        for (topic, count) in &partition_counts {
            let subscribers: Vec<usize> = (0..n)
                .filter(|rank| subscriptions[*rank].contains(topic))
                .collect();
            if subscribers.is_empty() {
                continue;
            }
            for p in 0..*count {
                let target = subscribers[(p as usize) % subscribers.len()];
                let withheld = self.protocol == AssignorProtocol::Cooperative
                    && owner_of
                        .get(&(topic.clone(), p))
                        .is_some_and(|owner| *owner != target);
                any_withheld |= withheld;
                plan.push((topic.clone(), p, target, withheld));
            }
        }
        let violate = any_withheld && self.violate_once.swap(false, Ordering::SeqCst);
        let mut per_member: Vec<TopicPartitionList> =
            (0..n).map(|_| TopicPartitionList::new()).collect();
        for (topic, p, target, withheld) in plan {
            if !withheld || violate {
                per_member[target].add_partition(&topic, p);
            }
        }

        let finish = move |mut task: AssignmentTask| {
            for (rank, (_, idx)) in members.iter().enumerate() {
                let mut userdata: Vec<u8> =
                    task.member(*idx).userdata().iter().rev().copied().collect();
                userdata.extend_from_slice(USERDATA_SUFFIX);
                task.set_assignment(*idx, &per_member[rank]);
                task.set_userdata(*idx, &userdata);
            }
            task.complete();
        };
        match self.completion {
            Completion::Inline => finish(task),
            Completion::Thread => {
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(300));
                    finish(task);
                });
            }
        }
    }

    fn on_assignment(
        &self,
        assignment: &TopicPartitionList,
        userdata: &[u8],
        group: &AssignedGroup,
    ) {
        self.assignments.lock().unwrap().push(Assigned {
            partitions: partitions_of(assignment),
            userdata: userdata.to_vec(),
            group: group.clone(),
        });
    }
}

struct AssignorContext<A: PartitionAssignor> {
    assignor: Arc<A>,
    /// librdkafka log lines, for pinning what the library reported.
    log_lines: Mutex<Vec<String>>,
}

impl<A: PartitionAssignor> AssignorContext<A> {
    fn new(assignor: Arc<A>) -> Self {
        AssignorContext {
            assignor,
            log_lines: Mutex::new(Vec::new()),
        }
    }

    fn log_lines_containing(&self, needle: &str) -> Vec<String> {
        self.log_lines
            .lock()
            .unwrap()
            .iter()
            .filter(|l| l.contains(needle))
            .cloned()
            .collect()
    }
}

impl<A: PartitionAssignor> ClientContext for AssignorContext<A> {
    fn log(&self, _level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.log_lines
            .lock()
            .unwrap()
            .push(format!("{} {}", fac, log_message));
    }
}

impl<A: PartitionAssignor> ConsumerContext for AssignorContext<A> {
    fn assignor(&self) -> Option<&dyn PartitionAssignor> {
        Some(&*self.assignor)
    }
}

/// Session 6000 ms and heartbeat 2000 ms: a pending assignment is given up
/// 4000 ms after the JoinGroup response. Log level `Info` so that the
/// library's warnings reach the context.
fn consumer_config(bootstrap_servers: &str, group_id: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("group.id", group_id)
        .set("bootstrap.servers", bootstrap_servers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("heartbeat.interval.ms", "2000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", ASSIGNOR_NAME)
        .set_log_level(RDKafkaLogLevel::Info);
    config
}

fn create_consumer<A: PartitionAssignor + 'static>(
    config: &ClientConfig,
    assignor: Arc<A>,
) -> BaseConsumer<AssignorContext<A>> {
    config
        .create_with_context(AssignorContext::new(assignor))
        .expect("could not create consumer with an assignor")
}

/// Polls every consumer until `done` holds or `timeout` passes.
fn poll_until<C: ConsumerContext>(
    consumers: &[&BaseConsumer<C>],
    timeout: Duration,
    mut done: impl FnMut() -> bool,
    what: &str,
) {
    let deadline = Instant::now() + timeout;
    loop {
        for consumer in consumers {
            consumer.poll(Duration::from_millis(100));
        }
        if done() {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {}", what);
    }
}

fn assignment_count<C: ConsumerContext>(consumer: &BaseConsumer<C>) -> usize {
    consumer.assignment().unwrap().count()
}

async fn create_topic(kafka_context: &KafkaContext, name: &str, partitions: i32) {
    let admin_client = admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    admin_client
        .create_topics(
            &admin::new_topic_vec(name, Some(partitions)),
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");
}

/// Waits until the consumer's metadata shows the topic with `partitions`
/// partitions, so that the first assignment round sees them all.
fn wait_for_partitions<C: ConsumerContext>(
    consumer: &BaseConsumer<C>,
    topic: &str,
    partitions: usize,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(5))
            .expect("metadata fetch failed");
        if metadata
            .topics()
            .iter()
            .any(|t| t.name() == topic && t.partitions().len() == partitions)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} never showed {} partitions",
            topic,
            partitions
        );
    }
}

fn expected_userdata(tag: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = tag.iter().rev().copied().collect();
    out.extend_from_slice(USERDATA_SUFFIX);
    out
}

fn topics_only(calls: &[SubscriptionCall]) -> Vec<Vec<String>> {
    calls.iter().map(|c| c.topics.clone()).collect()
}

fn owned_only(calls: &[SubscriptionCall]) -> Vec<Option<Vec<(String, i32)>>> {
    calls.iter().map(|c| c.owned.clone()).collect()
}

/// The owned lists of the subscription calls after the member first held an
/// assignment. A member's first join takes several JoinGroup requests (the
/// coordinator first answers with the member id to use), each with a
/// subscription call and no owned list; how many is the broker's business.
fn owned_once_assigned(calls: &[SubscriptionCall]) -> Vec<Vec<(String, i32)>> {
    let first = calls
        .iter()
        .position(|c| c.owned.is_some())
        .unwrap_or(calls.len());
    assert!(first > 0, "subscription calls: {:?}", calls);
    calls[first..]
        .iter()
        .map(|c| {
            c.owned
                .clone()
                .expect("an assigned member reports what it owns")
        })
        .collect()
}

const VIOLATION: &str = "revoked from one member and assigned to another in the same rebalance";
const GIVE_UP: &str = "did not complete the assignment";
const DROPPED: &str = "assignment task dropped without completing";

/// The registration is validated by `rd_kafka_new`: a name the strategy
/// parser could never select, a built-in's name, and a heartbeat interval
/// that leaves no room for a pending assignment each fail consumer creation
/// without a broker.
#[test]
fn test_assignor_registration_is_validated() {
    struct Named(&'static str);
    impl PartitionAssignor for Named {
        fn name(&self) -> &str {
            self.0
        }
        fn protocol(&self) -> AssignorProtocol {
            AssignorProtocol::Eager
        }
        fn assign(&self, _: &str, _: &Metadata, task: AssignmentTask) {
            task.complete();
        }
    }

    let creation_error = |config: &ClientConfig, assignor: Named| -> String {
        match config
            .create_with_context::<_, BaseConsumer<_>>(AssignorContext::new(Arc::new(assignor)))
        {
            Ok(_) => panic!("consumer creation should have failed"),
            Err(KafkaError::ClientCreation(msg)) => msg,
            Err(other) => panic!("unexpected error {:?}", other),
        }
    };

    // The registered name is validated before the strategy is parsed, so the
    // strategy may name the built-in `range` here.
    let mut config = ClientConfig::new();
    config
        .set("group.id", "validated")
        .set("bootstrap.servers", "localhost:1")
        .set("partition.assignment.strategy", "range");
    let msg = creation_error(&config, Named("a,b"));
    assert!(
        msg.contains("\"a,b\"") && msg.contains("commas"),
        "unexpected message: {}",
        msg
    );

    let msg = creation_error(&config, Named(""));
    assert!(msg.contains("non-empty"), "unexpected message: {}", msg);
    let msg = creation_error(&config, Named("a b"));
    assert!(msg.contains("whitespace"), "unexpected message: {}", msg);

    let msg = creation_error(&config, Named("range"));
    assert!(
        msg.contains("Failed to register application assignor \"range\"")
            && msg.contains("Conflicting"),
        "unexpected message: {}",
        msg
    );

    // A registration the strategy names alongside a built-in of the other
    // protocol fails the strategy's protocol check.
    struct Cooperative;
    impl PartitionAssignor for Cooperative {
        fn name(&self) -> &str {
            "coop"
        }
        fn protocol(&self) -> AssignorProtocol {
            AssignorProtocol::Cooperative
        }
        fn assign(&self, _: &str, _: &Metadata, task: AssignmentTask) {
            task.complete();
        }
    }
    let mut mixed = config.clone();
    mixed.set("partition.assignment.strategy", "coop,range");
    match mixed
        .create_with_context::<_, BaseConsumer<_>>(AssignorContext::new(Arc::new(Cooperative)))
    {
        Err(KafkaError::ClientCreation(msg)) => assert!(
            msg.contains("must have the same protocol type"),
            "unexpected message: {}",
            msg
        ),
        other => panic!("expected a creation error, got {:?}", other.map(|_| ())),
    }

    config
        .set("session.timeout.ms", "6000")
        .set("heartbeat.interval.ms", "6000");
    let msg = creation_error(&config, Named("ok"));
    assert!(
        msg.contains("must be less than session.timeout.ms (6000)"),
        "unexpected message: {}",
        msg
    );

    // A name with a NUL cannot reach librdkafka at all.
    match config.create_with_context::<_, BaseConsumer<_>>(AssignorContext::new(Arc::new(Named(
        "nul\0name",
    )))) {
        Err(KafkaError::Nul(_)) => {}
        other => panic!("expected a Nul error, got {:?}", other.map(|_| ())),
    }
}

/// Two members under the EAGER protocol, the second subscribed to a second
/// topic as well: the leader sees both subscriptions' topics, userdata, owned
/// partitions, rack and group instance ids, completes the assignment from a
/// second thread, and both members receive the partitions and userdata it
/// set, with the group identity of the generation.
#[tokio::test]
async fn test_assignor_two_members_eager() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic = rand_test_topic("test_assignor_two_members_eager");
    let topic_b = rand_test_topic("test_assignor_two_members_eager_b");
    create_topic(&kafka_context, &topic, 4).await;
    create_topic(&kafka_context, &topic_b, 1).await;
    let group = rand_test_group();
    let config = consumer_config(&kafka_context.bootstrap_servers, &group);

    let tag1 = b"alpha-\xF0\x9F\x8D\x8E".to_vec();
    let tag2 = b"be\0ta-\xC3\xA9\xC3\xA8".to_vec();
    let assignor1 = RoundRobinAssignor::new(&tag1, AssignorProtocol::Eager, Completion::Thread);
    let assignor2 = RoundRobinAssignor::new(&tag2, AssignorProtocol::Eager, Completion::Thread);

    let consumer1 = create_consumer(&config, assignor1.clone());
    wait_for_partitions(&consumer1, &topic, 4);
    wait_for_partitions(&consumer1, &topic_b, 1);
    consumer1.subscribe(&[topic.as_str()]).unwrap();
    poll_until(
        &[&consumer1],
        Duration::from_secs(30),
        || assignment_count(&consumer1) == 4,
        "consumer1 to own all four partitions",
    );

    let mut config2 = config.clone();
    config2
        .set("client.rack", "rack-\u{fc}2")
        .set("group.instance.id", "inst-\u{df}2");
    let consumer2 = create_consumer(&config2, assignor2.clone());
    consumer2
        .subscribe(&[topic_b.as_str(), topic.as_str()])
        .unwrap();
    poll_until(
        &[&consumer1, &consumer2],
        Duration::from_secs(30),
        || assignment_count(&consumer1) == 2 && assignment_count(&consumer2) == 3,
        "two partitions for consumer1, three for consumer2",
    );

    let member_id1 = consumer1.member_id().unwrap();
    let member_id2 = consumer2.member_id().unwrap();
    let two_topics = sorted(&[&topic, &topic_b]);

    // The first member led every round.
    let calls1 = assignor1.assign_calls.lock().unwrap().clone();
    assert!(
        assignor2.assign_calls.lock().unwrap().is_empty(),
        "consumer2 should not have led"
    );
    assert!(
        calls1.iter().all(|c| c.leader_id == member_id1),
        "{:?}",
        calls1
    );
    let alone = &calls1[0];
    assert_eq!(alone.member_ids, vec![member_id1.clone()]);
    assert_eq!(alone.subscriptions, vec![vec![topic.clone()]]);
    assert_eq!(alone.userdata, vec![tag1.clone()]);
    assert_eq!(alone.owned, vec![Some(Vec::new())]);
    assert_eq!(alone.instance_ids, vec![None]);
    assert_eq!(alone.rack_ids, vec![None]);
    let both = calls1.last().unwrap();
    assert_eq!(both.member_ids.len(), 2, "last call: {:?}", both);
    let rank1 = both
        .member_ids
        .iter()
        .position(|m| *m == member_id1)
        .unwrap();
    let rank2 = both
        .member_ids
        .iter()
        .position(|m| *m == member_id2)
        .unwrap();
    assert_eq!(both.subscriptions[rank1], vec![topic.clone()]);
    assert_eq!(both.subscriptions[rank2], two_topics);
    assert_eq!(both.userdata[rank1], tag1);
    assert_eq!(both.userdata[rank2], tag2);
    assert_eq!(both.instance_ids[rank1], None);
    assert_eq!(both.instance_ids[rank2], Some("inst-\u{df}2".to_owned()));
    assert_eq!(both.rack_ids[rank1], None);
    assert_eq!(both.rack_ids[rank2], Some("rack-\u{fc}2".to_owned()));
    // EAGER: the members' owned partitions were revoked before they rejoined.
    // (`owned_partitions() == None` needs a peer with pre-owned-partitions
    // metadata, which no current librdkafka sends: not observable here.)
    assert_eq!(both.owned[rank1], Some(Vec::new()));
    assert_eq!(both.owned[rank2], Some(Vec::new()));

    // Every subscription call named the member's topics. Under EAGER a member
    // joins owning nothing, which the callback sees as no list; the leader
    // reads the same member's owned partitions back as an empty list, because
    // the subscription encoder writes an empty array for no list.
    let subs1 = assignor1.subscription_calls.lock().unwrap().clone();
    let subs2 = assignor2.subscription_calls.lock().unwrap().clone();
    assert!(subs1.len() >= 2, "subscription calls: {:?}", subs1);
    assert_eq!(topics_only(&subs1), vec![vec![topic.clone()]; subs1.len()]);
    assert_eq!(topics_only(&subs2), vec![two_topics.clone(); subs2.len()]);
    assert_eq!(owned_only(&subs1), vec![None; subs1.len()]);
    assert_eq!(owned_only(&subs2), vec![None; subs2.len()]);

    let expected_partitions = |rank: usize| -> Vec<(String, i32)> {
        let mut out: Vec<(String, i32)> = (0..4)
            .filter(|p| (*p as usize) % 2 == rank)
            .map(|p| (topic.clone(), p))
            .collect();
        if rank == rank2 {
            out.push((topic_b.clone(), 0));
        }
        out.sort();
        out
    };
    let assigned1 = assignor1.assignments.lock().unwrap().clone();
    let assigned2 = assignor2.assignments.lock().unwrap().clone();
    assert_eq!(
        assigned1[0].partitions,
        (0..4).map(|p| (topic.clone(), p)).collect::<Vec<_>>()
    );
    assert_eq!(assigned1[0].userdata, expected_userdata(&tag1));
    let last1 = assigned1.last().unwrap();
    let last2 = assigned2.last().unwrap();
    assert_eq!(last1.partitions, expected_partitions(rank1));
    assert_eq!(last2.partitions, expected_partitions(rank2));
    assert_eq!(last1.userdata, expected_userdata(&tag1));
    assert_eq!(last2.userdata, expected_userdata(&tag2));
    assert_eq!(last1.group.group_id, group);
    assert_eq!(last2.group.group_id, group);
    assert_eq!(last1.group.member_id, member_id1);
    assert_eq!(last2.group.member_id, member_id2);
    assert_eq!(last1.group.generation_id, last2.group.generation_id);
    assert!(last1.group.generation_id > assigned1[0].group.generation_id);
    assert_eq!(last1.group.group_instance_id, None);
    assert_eq!(
        last2.group.group_instance_id,
        Some("inst-\u{df}2".to_owned())
    );
    assert!(matches!(
        consumer1.rebalance_protocol(),
        RebalanceProtocol::Eager
    ));
}

/// Under COOPERATIVE the fork applies no adjustment to the application's
/// assignment: moving an owned partition in one round is rejected and the
/// group rejoins; a partition the assignor withholds is revoked from its owner
/// and reaches its new owner at the follow-up round. consumer2's
/// `subscription_userdata` panics on every call, so it joins without
/// userdata throughout.
#[tokio::test]
async fn test_assignor_cooperative_handover() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic = rand_test_topic("test_assignor_cooperative_handover");
    create_topic(&kafka_context, &topic, 2).await;
    let group = rand_test_group();
    let config = consumer_config(&kafka_context.bootstrap_servers, &group);

    let assignor1 = RoundRobinAssignor::new(
        b"eins-\xE2\x82\xAC",
        AssignorProtocol::Cooperative,
        Completion::Inline,
    );
    let assignor2 = RoundRobinAssignor::new(
        b"zw\xC3\xB6lf",
        AssignorProtocol::Cooperative,
        Completion::Inline,
    );
    assignor1.violate_once.store(true, Ordering::SeqCst);
    assignor2.panic_subscription.store(true, Ordering::SeqCst);

    let consumer1 = create_consumer(&config, assignor1.clone());
    wait_for_partitions(&consumer1, &topic, 2);
    consumer1.subscribe(&[topic.as_str()]).unwrap();
    poll_until(
        &[&consumer1],
        Duration::from_secs(30),
        || assignment_count(&consumer1) == 2,
        "consumer1 to own both partitions",
    );

    let consumer2 = create_consumer(&config, assignor2.clone());
    consumer2.subscribe(&[topic.as_str()]).unwrap();
    poll_until(
        &[&consumer1, &consumer2],
        Duration::from_secs(30),
        || {
            assignment_count(&consumer1) == 1
                && assignment_count(&consumer2) == 1
                && assignor1.assignments.lock().unwrap().len() >= 3
                && assignor2.assignments.lock().unwrap().len() >= 2
        },
        "one partition per consumer, with both members' assignments recorded",
    );

    let member_id1 = consumer1.member_id().unwrap();
    let member_id2 = consumer2.member_id().unwrap();
    let calls1 = assignor1.assign_calls.lock().unwrap().clone();
    assert!(assignor2.assign_calls.lock().unwrap().is_empty());
    assert!(
        calls1.iter().all(|c| c.leader_id == member_id1),
        "{:?}",
        calls1
    );
    // Three two-member rounds after however many solo ones.
    let first_two = calls1
        .iter()
        .position(|c| c.member_ids.len() == 2)
        .expect("a two-member round");
    assert!(calls1[..first_two].iter().all(|c| c.member_ids.len() == 1));
    let rounds = &calls1[first_two..];
    assert_eq!(rounds.len(), 3, "expected three rounds, saw {:?}", rounds);
    let both = |call: &AssignCall| -> (usize, usize) {
        (
            call.member_ids
                .iter()
                .position(|m| *m == member_id1)
                .unwrap(),
            call.member_ids
                .iter()
                .position(|m| *m == member_id2)
                .unwrap(),
        )
    };
    let owned_both = Some(vec![(topic.clone(), 0), (topic.clone(), 1)]);
    // Round 1: consumer1 owned both partitions when consumer2 joined; the
    // leader moved one in the same round and the library rejected the
    // assignment.
    let (rank1, rank2) = both(&rounds[0]);
    assert_eq!(rounds[0].owned[rank1], owned_both);
    assert_eq!(rounds[0].owned[rank2], Some(Vec::new()));
    assert_eq!(rounds[0].userdata[rank1], b"eins-\xE2\x82\xAC");
    assert_eq!(rounds[0].userdata[rank2], b"");
    let violations = consumer1.context().log_lines_containing(VIOLATION);
    assert_eq!(violations.len(), 1, "log lines: {:?}", violations);
    // Round 2: the same ownership, the moved partition now withheld.
    assert_eq!(both(&rounds[1]), (rank1, rank2));
    assert_eq!(rounds[1].owned[rank1], owned_both);
    assert_eq!(rounds[1].owned[rank2], Some(Vec::new()));
    assert_eq!(rounds[1].userdata[rank2], b"");
    // Round 3: the withheld partition had been revoked.
    assert_eq!(both(&rounds[2]), (rank1, rank2));
    assert_eq!(
        rounds[2].owned[rank1],
        Some(vec![(topic.clone(), rank1 as i32)])
    );
    assert_eq!(rounds[2].owned[rank2], Some(Vec::new()));

    let subs1 = assignor1.subscription_calls.lock().unwrap().clone();
    let subs2 = assignor2.subscription_calls.lock().unwrap().clone();
    assert_eq!(topics_only(&subs1), vec![vec![topic.clone()]; subs1.len()]);
    assert_eq!(topics_only(&subs2), vec![vec![topic.clone()]; subs2.len()]);
    // consumer1 rejoined owning both partitions for the rejected and the
    // withholding rounds, then one; consumer2 first owned (nothing) after the
    // withholding round.
    let both_list = owned_both.clone().unwrap();
    assert_eq!(
        owned_once_assigned(&subs1),
        vec![
            both_list.clone(),
            both_list,
            vec![(topic.clone(), rank1 as i32)],
        ],
        "subscription calls: {:?}",
        subs1
    );
    assert_eq!(
        owned_once_assigned(&subs2),
        vec![Vec::new()],
        "subscription calls: {:?}",
        subs2
    );

    let assigned1 = assignor1.assignments.lock().unwrap().clone();
    let assigned2 = assignor2.assignments.lock().unwrap().clone();
    let partitions = |a: &[Assigned]| -> Vec<Vec<i32>> {
        a.iter()
            .map(|a| a.partitions.iter().map(|(_, p)| *p).collect())
            .collect()
    };
    assert_eq!(
        partitions(&assigned1),
        vec![vec![0, 1], vec![rank1 as i32], vec![rank1 as i32]]
    );
    // consumer2 received nothing in the round its partition was withheld.
    assert_eq!(partitions(&assigned2), vec![vec![], vec![rank2 as i32]]);
    assert_eq!(assigned2[0].userdata, USERDATA_SUFFIX);
    assert!(matches!(
        consumer1.rebalance_protocol(),
        RebalanceProtocol::Cooperative
    ));
}

/// An assignment the leader drops, panics on, fails or holds past the budget
/// is a failed round: the group rejoins and the next round's assignment goes
/// through; no assignment reaches the members for the failed rounds, the
/// late completion of the held round is discarded, a panic after the task
/// was handed off does not fail it, and a panicking `on_assignment` leaves
/// the assignment applied.
#[tokio::test]
async fn test_assignor_failed_rounds_rejoin() {
    init_test_logger();

    struct FlakyAssignor {
        calls: AtomicUsize,
        assignments: Mutex<Vec<Assigned>>,
        /// The userdata the final round saw on the member.
        userdata_seen: Mutex<Option<Vec<u8>>>,
        /// Whether `member(1)` of one member panicked in the final round.
        out_of_range_panicked: AtomicBool,
        held: Mutex<Option<JoinHandle<()>>>,
        panic_on_assignment_once: AtomicBool,
    }
    impl PartitionAssignor for FlakyAssignor {
        fn name(&self) -> &str {
            ASSIGNOR_NAME
        }
        fn protocol(&self) -> AssignorProtocol {
            AssignorProtocol::Eager
        }
        // The default `subscription_userdata`: no userdata.
        fn assign(&self, _member_id: &str, metadata: &Metadata, mut task: AssignmentTask) {
            match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => drop(task),
                1 => panic!("the assignor panics on its second call"),
                2 => task.fail("the assignor \u{2718} de\0clines its third call"),
                3 => {
                    // Held past the 4000 ms budget: the library gives the
                    // round up, and this completion arrives stale.
                    *self.held.lock().unwrap() = Some(thread::spawn(move || {
                        thread::sleep(Duration::from_millis(6000));
                        task.complete();
                    }));
                }
                _ => {
                    let mut list = TopicPartitionList::new();
                    for t in metadata.topics() {
                        for p in t.partitions() {
                            list.add_partition(t.name(), p.id());
                        }
                    }
                    *self.userdata_seen.lock().unwrap() = Some(task.member(0).userdata().to_vec());
                    let out_of_range = panic::catch_unwind(AssertUnwindSafe(|| {
                        task.member(1);
                    }));
                    self.out_of_range_panicked
                        .store(out_of_range.is_err(), Ordering::SeqCst);
                    task.set_userdata(0, &[]);
                    // Handed off, then a panic: the round still goes through.
                    thread::spawn(move || {
                        thread::sleep(Duration::from_millis(300));
                        task.set_assignment(0, &list);
                        task.complete();
                    });
                    panic!("the assignor panics after handing its task off");
                }
            }
        }
        fn on_assignment(
            &self,
            assignment: &TopicPartitionList,
            userdata: &[u8],
            group: &AssignedGroup,
        ) {
            self.assignments.lock().unwrap().push(Assigned {
                partitions: partitions_of(assignment),
                userdata: userdata.to_vec(),
                group: group.clone(),
            });
            if self.panic_on_assignment_once.swap(false, Ordering::SeqCst) {
                panic!("the assignor panics on its first on_assignment");
            }
        }
    }

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic = rand_test_topic("test_assignor_failed_rounds_rejoin");
    create_topic(&kafka_context, &topic, 2).await;
    let group = rand_test_group();
    let config = consumer_config(&kafka_context.bootstrap_servers, &group);

    let assignor = Arc::new(FlakyAssignor {
        calls: AtomicUsize::new(0),
        assignments: Mutex::new(Vec::new()),
        userdata_seen: Mutex::new(None),
        out_of_range_panicked: AtomicBool::new(false),
        held: Mutex::new(None),
        panic_on_assignment_once: AtomicBool::new(true),
    });
    let consumer = create_consumer(&config, assignor.clone());
    wait_for_partitions(&consumer, &topic, 2);
    consumer.subscribe(&[topic.as_str()]).unwrap();
    poll_until(
        &[&consumer],
        Duration::from_secs(60),
        || assignment_count(&consumer) == 2,
        "the fifth assignment to go through",
    );

    assert_eq!(assignor.calls.load(Ordering::SeqCst), 5);
    let assigned = assignor.assignments.lock().unwrap().clone();
    assert_eq!(assigned.len(), 1, "assignments: {:?}", assigned);
    assert_eq!(
        assigned[0].partitions,
        vec![(topic.clone(), 0), (topic.clone(), 1)]
    );
    assert_eq!(assigned[0].userdata, b"");
    assert_eq!(assigned[0].group.member_id, consumer.member_id().unwrap());
    assert_eq!(*assignor.userdata_seen.lock().unwrap(), Some(Vec::new()));
    assert!(assignor.out_of_range_panicked.load(Ordering::SeqCst));
    // The dropped and the panicking rounds were completed by the task's drop,
    // the third by `fail` with its reason (the NUL stripped), the fourth given
    // up by the library; the fifth round's panic, after the hand-off, failed
    // nothing.
    let context = consumer.context();
    assert_eq!(context.log_lines_containing(DROPPED).len(), 2);
    let failed = context.log_lines_containing("the assignor \u{2718} declines its third call");
    assert_eq!(failed.len(), 1, "log lines: {:?}", failed);
    let given_up = context.log_lines_containing(GIVE_UP);
    assert_eq!(given_up.len(), 1, "log lines: {:?}", given_up);

    // The held round's completion arrives after the group moved on: it is
    // discarded, and nothing changes. (The group is stable by then, so this
    // exercises the state check; the pending-id check is the C suite's.) A
    // completion applied anyway would send a SyncGroup and run on_assignment
    // again, which the record count catches.
    let held = assignor.held.lock().unwrap().take().unwrap();
    while !held.is_finished() {
        consumer.poll(Duration::from_millis(100));
    }
    held.join().unwrap();
    let settle = Instant::now() + Duration::from_secs(2);
    while Instant::now() < settle {
        consumer.poll(Duration::from_millis(100));
    }
    assert_eq!(assignor.calls.load(Ordering::SeqCst), 5);
    assert_eq!(assignment_count(&consumer), 2);
    assert_eq!(assignor.assignments.lock().unwrap().len(), 1);
}

/// A task completed, or dropped, after its consumer was dropped is a safe
/// no-op: the pending handle outlives the client.
#[tokio::test]
async fn test_assignor_completion_after_consumer_dropped() {
    init_test_logger();

    struct HoldingAssignor {
        held: Mutex<Option<AssignmentTask>>,
    }
    impl PartitionAssignor for HoldingAssignor {
        fn name(&self) -> &str {
            ASSIGNOR_NAME
        }
        fn protocol(&self) -> AssignorProtocol {
            AssignorProtocol::Eager
        }
        fn assign(&self, _: &str, _: &Metadata, task: AssignmentTask) {
            *self.held.lock().unwrap() = Some(task);
        }
    }

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic = rand_test_topic("test_assignor_completion_after_consumer_dropped");
    create_topic(&kafka_context, &topic, 1).await;
    let group = rand_test_group();
    let config = consumer_config(&kafka_context.bootstrap_servers, &group);

    for complete in [true, false] {
        let assignor = Arc::new(HoldingAssignor {
            held: Mutex::new(None),
        });
        let consumer = create_consumer(&config, assignor.clone());
        consumer.subscribe(&[topic.as_str()]).unwrap();
        poll_until(
            &[&consumer],
            Duration::from_secs(30),
            || assignor.held.lock().unwrap().is_some(),
            "the assignor to hold a task",
        );
        drop(consumer);
        let task = assignor.held.lock().unwrap().take().unwrap();
        if complete {
            task.complete();
        } else {
            drop(task);
        }
    }
}

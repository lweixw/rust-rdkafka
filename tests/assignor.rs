//! Test the application partition assignor against a broker.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
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

/// What the leader saw in one `assign` call.
#[derive(Clone, Debug)]
struct AssignCall {
    member_ids: Vec<String>,
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

/// Assigns partition `p` to the member at index `p % n` (members in id
/// order); under COOPERATIVE a partition owned by a member other than its
/// target is withheld for that round. The userdata assigned to a member is
/// its subscription userdata reversed, with a suffix.
struct RoundRobinAssignor {
    tag: Vec<u8>,
    protocol: AssignorProtocol,
    completion: Completion,
    assign_calls: Mutex<Vec<AssignCall>>,
    assignments: Mutex<Vec<Assigned>>,
    subscription_calls: AtomicUsize,
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

impl PartitionAssignor for RoundRobinAssignor {
    fn name(&self) -> &str {
        ASSIGNOR_NAME
    }

    fn protocol(&self) -> AssignorProtocol {
        self.protocol
    }

    fn subscription_userdata(
        &self,
        _topics: &[&str],
        _owned_partitions: Option<&TopicPartitionList>,
    ) -> Vec<u8> {
        self.subscription_calls.fetch_add(1, Ordering::SeqCst);
        self.tag.clone()
    }

    fn assign(&self, _member_id: &str, metadata: &Metadata, mut task: AssignmentTask) {
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
            member_ids: Vec::new(),
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
            call.subscriptions.push(member.subscription());
            call.owned.push(owned);
            call.userdata.push(member.userdata().to_vec());
        }
        self.assign_calls.lock().unwrap().push(call);

        let mut per_member: Vec<TopicPartitionList> =
            (0..n).map(|_| TopicPartitionList::new()).collect();
        for (topic, count) in &partition_counts {
            for p in 0..*count {
                let target = (p as usize) % n;
                let withheld = self.protocol == AssignorProtocol::Cooperative
                    && owner_of
                        .get(&(topic.clone(), p))
                        .is_some_and(|owner| *owner != target);
                if !withheld {
                    per_member[target].add_partition(topic, p);
                }
            }
        }

        let finish = move |mut task: AssignmentTask| {
            for (rank, (_, idx)) in members.iter().enumerate() {
                let mut member = task.member(*idx);
                let mut userdata: Vec<u8> = member.userdata().iter().rev().copied().collect();
                userdata.extend_from_slice(USERDATA_SUFFIX);
                member.set_assignment(&per_member[rank]);
                member.set_userdata(&userdata);
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

struct AssignorContext<A: PartitionAssignor + Send + Sync> {
    assignor: Arc<A>,
    /// librdkafka log lines, for pinning what the library reported.
    log_lines: Mutex<Vec<String>>,
}

impl<A: PartitionAssignor + Send + Sync> AssignorContext<A> {
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

impl<A: PartitionAssignor + Send + Sync> ClientContext for AssignorContext<A> {
    fn log(&self, _level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.log_lines
            .lock()
            .unwrap()
            .push(format!("{} {}", fac, log_message));
    }
}

impl<A: PartitionAssignor + Send + Sync> ConsumerContext for AssignorContext<A> {
    fn assignor(&self) -> Option<&dyn PartitionAssignor> {
        Some(&*self.assignor)
    }
}

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
        .set("partition.assignment.strategy", ASSIGNOR_NAME);
    config
}

fn create_consumer<A: PartitionAssignor + Send + Sync + 'static>(
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

fn expected_userdata(tag: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = tag.iter().rev().copied().collect();
    out.extend_from_slice(USERDATA_SUFFIX);
    out
}

/// The registration is validated by `rd_kafka_new`: a name the strategy
/// parser could never select, and a heartbeat interval that leaves no room
/// for a pending assignment, both fail consumer creation without a broker.
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

    let mut config = ClientConfig::new();
    config
        .set("group.id", "validated")
        .set("bootstrap.servers", "localhost:1")
        .set("partition.assignment.strategy", "a,b");
    let msg = creation_error(&config, Named("a,b"));
    assert!(msg.contains("commas"), "unexpected message: {}", msg);

    let msg = creation_error(&config, Named("range"));
    assert!(
        msg.contains("\"range\"") && msg.contains("Conflicting"),
        "unexpected message: {}",
        msg
    );

    config
        .set("partition.assignment.strategy", "ok")
        .set("session.timeout.ms", "6000")
        .set("heartbeat.interval.ms", "6000");
    let msg = creation_error(&config, Named("ok"));
    assert!(
        msg.contains("heartbeat.interval.ms"),
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

/// Two members under the EAGER protocol: the leader sees both subscriptions'
/// userdata and owned partitions, completes the assignment from a second
/// thread, and both members receive the partitions and userdata it set.
#[tokio::test]
async fn test_assignor_two_members_eager() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic = rand_test_topic("test_assignor_two_members_eager");
    create_topic(&kafka_context, &topic, 4).await;
    let group = rand_test_group();
    let config = consumer_config(&kafka_context.bootstrap_servers, &group);

    let tag1 = b"alpha-\xF0\x9F\x8D\x8E".to_vec();
    let tag2 = b"beta-\xC3\xA9\xC3\xA8".to_vec();
    let assignor1 = Arc::new(RoundRobinAssignor {
        tag: tag1.clone(),
        protocol: AssignorProtocol::Eager,
        completion: Completion::Thread,
        assign_calls: Mutex::new(Vec::new()),
        assignments: Mutex::new(Vec::new()),
        subscription_calls: AtomicUsize::new(0),
    });
    let assignor2 = Arc::new(RoundRobinAssignor {
        tag: tag2.clone(),
        protocol: AssignorProtocol::Eager,
        completion: Completion::Thread,
        assign_calls: Mutex::new(Vec::new()),
        assignments: Mutex::new(Vec::new()),
        subscription_calls: AtomicUsize::new(0),
    });

    let consumer1 = create_consumer(&config, assignor1.clone());
    consumer1.subscribe(&[topic.as_str()]).unwrap();
    poll_until(
        &[&consumer1],
        Duration::from_secs(30),
        || assignment_count(&consumer1) == 4,
        "consumer1 to own all four partitions",
    );

    let consumer2 = create_consumer(&config, assignor2.clone());
    consumer2.subscribe(&[topic.as_str()]).unwrap();
    poll_until(
        &[&consumer1, &consumer2],
        Duration::from_secs(30),
        || assignment_count(&consumer1) == 2 && assignment_count(&consumer2) == 2,
        "two partitions per consumer",
    );

    // The first member led both rounds.
    let calls1 = assignor1.assign_calls.lock().unwrap().clone();
    assert!(
        assignor2.assign_calls.lock().unwrap().is_empty(),
        "consumer2 should not have led"
    );
    let alone = &calls1[0];
    assert_eq!(alone.member_ids.len(), 1);
    assert_eq!(alone.subscriptions, vec![vec![topic.clone()]]);
    assert_eq!(alone.userdata, vec![tag1.clone()]);
    assert_eq!(alone.owned, vec![Some(Vec::new())]);
    let both = calls1.last().unwrap();
    assert_eq!(both.member_ids.len(), 2, "last call: {:?}", both);
    assert_eq!(both.subscriptions, vec![vec![topic.clone()]; 2]);
    let member_id1 = consumer1.member_id().unwrap();
    let member_id2 = consumer2.member_id().unwrap();
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
    assert_eq!(both.userdata[rank1], tag1);
    assert_eq!(both.userdata[rank2], tag2);
    // EAGER: the members' owned partitions were revoked before they rejoined.
    assert_eq!(both.owned[rank2], Some(Vec::new()));
    assert_eq!(both.owned[rank1], Some(Vec::new()));
    assert!(both.member_ids.windows(2).all(|w| w[0] < w[1]));

    let expected_partitions = |rank: usize| -> Vec<(String, i32)> {
        (0..4)
            .filter(|p| (*p as usize) % 2 == rank)
            .map(|p| (topic.clone(), p))
            .collect()
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
    assert!(assignor1.subscription_calls.load(Ordering::SeqCst) >= 2);
    assert!(assignor2.subscription_calls.load(Ordering::SeqCst) >= 1);
}

/// Under COOPERATIVE the fork applies no adjustment to the application's
/// assignment: a partition the assignor withholds is revoked from its owner
/// and reaches its new owner at the follow-up rebalance.
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

    let new_assignor = |tag: &[u8]| {
        Arc::new(RoundRobinAssignor {
            tag: tag.to_vec(),
            protocol: AssignorProtocol::Cooperative,
            completion: Completion::Inline,
            assign_calls: Mutex::new(Vec::new()),
            assignments: Mutex::new(Vec::new()),
            subscription_calls: AtomicUsize::new(0),
        })
    };
    let assignor1 = new_assignor(b"one");
    let assignor2 = new_assignor(b"two");

    let consumer1 = create_consumer(&config, assignor1.clone());
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
        || assignment_count(&consumer1) == 1 && assignment_count(&consumer2) == 1,
        "one partition per consumer",
    );

    let member_id1 = consumer1.member_id().unwrap();
    let member_id2 = consumer2.member_id().unwrap();
    let calls1 = assignor1.assign_calls.lock().unwrap().clone();
    assert!(assignor2.assign_calls.lock().unwrap().is_empty());
    assert!(calls1.len() >= 3, "expected three rounds, saw {:?}", calls1);
    // Round 2: consumer1 still owned both partitions when consumer2 joined.
    let second = &calls1[1];
    let rank1 = second
        .member_ids
        .iter()
        .position(|m| *m == member_id1)
        .unwrap();
    let rank2 = second
        .member_ids
        .iter()
        .position(|m| *m == member_id2)
        .unwrap();
    assert_eq!(
        second.owned[rank1],
        Some(vec![(topic.clone(), 0), (topic.clone(), 1)])
    );
    assert_eq!(second.owned[rank2], Some(Vec::new()));
    // Round 3: the withheld partition had been revoked.
    let third = &calls1[2];
    assert_eq!(
        third.owned[rank1],
        Some(vec![(topic.clone(), rank1 as i32)])
    );
    assert_eq!(third.owned[rank2], Some(Vec::new()));

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
    assert_eq!(assigned2[0].userdata, expected_userdata(b"two"));
    assert!(matches!(
        consumer1.rebalance_protocol(),
        RebalanceProtocol::Cooperative
    ));
}

/// An assignment the leader drops or panics on fails, the group rejoins, and
/// the next round's assignment goes through; no assignment reaches the
/// members for the failed rounds.
#[tokio::test]
async fn test_assignor_dropped_and_panicking_assignments_rejoin() {
    init_test_logger();

    struct FlakyAssignor {
        calls: AtomicUsize,
        assignments: Mutex<Vec<Assigned>>,
    }
    impl PartitionAssignor for FlakyAssignor {
        fn name(&self) -> &str {
            ASSIGNOR_NAME
        }
        fn protocol(&self) -> AssignorProtocol {
            AssignorProtocol::Eager
        }
        fn assign(&self, _member_id: &str, metadata: &Metadata, mut task: AssignmentTask) {
            match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => drop(task),
                1 => panic!("the assignor panics on its second call"),
                _ => {
                    let mut list = TopicPartitionList::new();
                    for t in metadata.topics() {
                        for p in t.partitions() {
                            list.add_partition(t.name(), p.id());
                        }
                    }
                    task.member(0).set_assignment(&list);
                    task.complete();
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

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let topic = rand_test_topic("test_assignor_dropped_and_panicking_assignments_rejoin");
    create_topic(&kafka_context, &topic, 2).await;
    let group = rand_test_group();
    let config = consumer_config(&kafka_context.bootstrap_servers, &group);

    let assignor = Arc::new(FlakyAssignor {
        calls: AtomicUsize::new(0),
        assignments: Mutex::new(Vec::new()),
    });
    let consumer = create_consumer(&config, assignor.clone());
    consumer.subscribe(&[topic.as_str()]).unwrap();
    poll_until(
        &[&consumer],
        Duration::from_secs(60),
        || assignment_count(&consumer) == 2,
        "the third assignment to go through",
    );

    assert_eq!(assignor.calls.load(Ordering::SeqCst), 3);
    let assigned = assignor.assignments.lock().unwrap().clone();
    assert_eq!(assigned.len(), 1, "assignments: {:?}", assigned);
    assert_eq!(
        assigned[0].partitions,
        vec![(topic.clone(), 0), (topic.clone(), 1)]
    );
    assert_eq!(assigned[0].userdata, b"");
    assert_eq!(assigned[0].group.member_id, consumer.member_id().unwrap());
    // Both failed rounds were completed by the task's drop, not by the
    // library giving the assignment up.
    let dropped = consumer
        .context()
        .log_lines_containing("assignment task dropped without completing");
    assert_eq!(dropped.len(), 2, "log lines: {:?}", dropped);
    assert!(consumer
        .context()
        .log_lines_containing("did not complete the assignment")
        .is_empty());
}

use std::time::{Duration, Instant};

use crate::utils::consumer;
use crate::utils::containers::KafkaContext;
use crate::utils::logging::init_test_logger;
use crate::utils::rand::{rand_test_group, rand_test_topic};
use rdkafka::admin::{
    AdminOptions, ConsumerGroupDescription, GroupResult, NewTopic, TopicReplication,
};
use rdkafka::consumer::Consumer;
use rdkafka_sys::RDKafkaErrorCode;

mod utils;

/// Verify that a valid group can be deleted.
#[tokio::test]
pub async fn test_consumer_groups_deletion() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    // Create admin client
    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");

    // Create consumer_client
    let group_name = rand_test_group();
    let topic_name = rand_test_topic("test_topic");
    let consumer_client = utils::consumer::create_unsubscribed_base_consumer(
        &kafka_context.bootstrap_servers,
        Some(&group_name),
    )
    .await
    .expect("could not create subscribed base consumer");

    admin_client
        .create_topics(
            &[NewTopic {
                name: &topic_name,
                num_partitions: 1,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }],
            &AdminOptions::default(),
        )
        .await
        .expect("topic creation failed");

    utils::consumer::create_consumer_group_on_topic(&consumer_client, &topic_name)
        .await
        .expect("could not create group");
    let res = admin_client
        .delete_groups(&[&group_name], &AdminOptions::default())
        .await
        .expect("could not delete groups");
    assert_eq!(res, [Ok(group_name.to_string())]);
}

/// Verify that attempting to delete an unknown group returns a "group not
/// found" error.
#[tokio::test]
pub async fn test_delete_unknown_group() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    // Create admin client
    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");

    let unknown_group_name = rand_test_group();
    let res = admin_client
        .delete_groups(&[&unknown_group_name], &AdminOptions::default())
        .await
        .expect("delete_groups call failed");
    // The broker reports GroupIdNotFound once the consumer-group coordinator
    // has been initialised (any prior test in the binary that touched a
    // group is enough), and NotCoordinator on a cold broker. Both indicate
    // the same thing for this test: the group does not exist.
    let group_result: &GroupResult = res.first().expect("expected one result");
    let (returned_name, code) = group_result
        .as_ref()
        .expect_err("expected an error for an unknown group");
    assert_eq!(returned_name, &unknown_group_name);
    assert!(
        matches!(
            code,
            RDKafkaErrorCode::GroupIdNotFound | RDKafkaErrorCode::NotCoordinator
        ),
        "unexpected error code: {:?}",
        code
    );
}

// `delete_groups` cannot remove a group while it still has an active member.
// This test subscribes a consumer to a topic, drives it until it has actually
// joined the group, calls `delete_groups`, and asserts the per-group result
// is `NonEmptyGroup`. It then drops the consumer (which sends LeaveGroup),
// retries `delete_groups`, and asserts the second call succeeds. A binding
// regression that misclassified the per-group error or that lost the
// active-membership signal in the second-call retry would fail one of those
// assertions.
#[tokio::test]
pub async fn test_delete_non_empty_consumer_group() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");

    let group_name = rand_test_group();
    let topic_name = rand_test_topic("test_delete_non_empty_group");

    admin_client
        .create_topics(
            &[NewTopic {
                name: &topic_name,
                num_partitions: 1,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }],
            &AdminOptions::default(),
        )
        .await
        .expect("topic creation failed");

    let consumer_client = utils::consumer::create_subscribed_base_consumer(
        &kafka_context.bootstrap_servers,
        Some(&group_name),
        &topic_name,
    )
    .await
    .expect("could not create subscribed consumer");

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        consumer_client.poll(Duration::from_millis(200));
        if consumer_client.assignment().unwrap().count() > 0 {
            break;
        }
        if Instant::now() > deadline {
            panic!("consumer never joined the group");
        }
    }

    let res = admin_client
        .delete_groups(&[&group_name], &AdminOptions::default())
        .await
        .expect("delete_groups call should not itself fail");
    let first: &GroupResult = res.first().expect("expected one result");
    let (returned_name, code) = first
        .as_ref()
        .expect_err("delete_groups on a non-empty group should be an error");
    assert_eq!(returned_name, &group_name);
    assert_eq!(
        *code,
        RDKafkaErrorCode::NonEmptyGroup,
        "expected NonEmptyGroup while the consumer is still active, got {:?}",
        code
    );

    drop(consumer_client);

    let deadline = Instant::now() + Duration::from_secs(30);
    let last_err: RDKafkaErrorCode = loop {
        let res = admin_client
            .delete_groups(&[&group_name], &AdminOptions::default())
            .await
            .expect("delete_groups call should not itself fail");
        match res.first().expect("expected one result") {
            Ok(name) => {
                assert_eq!(name, &group_name);
                return;
            }
            Err((_, code)) => {
                if Instant::now() > deadline {
                    break *code;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    panic!(
        "delete_groups never converged to success after consumer drop (last error: {:?})",
        last_err
    );
}

/// Verify that deleting a valid and invalid group results in a mixed result
/// set.
#[tokio::test]
pub async fn test_consumer_group_action_mixed_results() {
    init_test_logger();

    // Get Kafka container context.
    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");

    // Create admin client
    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");

    // Create consumer_client
    let group_name = rand_test_group();
    let topic_name = rand_test_topic("test_topic");
    let consumer_client = utils::consumer::create_unsubscribed_base_consumer(
        &kafka_context.bootstrap_servers,
        Some(&group_name),
    )
    .await
    .expect("could not create subscribed base consumer");

    admin_client
        .create_topics(
            &[NewTopic {
                name: &topic_name,
                num_partitions: 1,
                replication: TopicReplication::Fixed(1),
                config: vec![],
            }],
            &AdminOptions::default(),
        )
        .await
        .expect("topic creation failed");

    let unknown_group_name = rand_test_group();
    consumer::create_consumer_group_on_topic(&consumer_client, &topic_name)
        .await
        .expect("could not create group");
    let res = admin_client
        .delete_groups(
            &[&group_name, &unknown_group_name],
            &AdminOptions::default(),
        )
        .await;
    assert_eq!(
        res,
        Ok(vec![
            Ok(group_name.to_string()),
            Err((
                unknown_group_name.to_string(),
                RDKafkaErrorCode::GroupIdNotFound
            ))
        ])
    );
}

/// Two members of one group, each holding one partition of a two-partition
/// topic, are described back with their ids, hosts and assignments; an unknown
/// group in the same request is described with no members.
#[tokio::test]
async fn test_describe_consumer_groups() {
    init_test_logger();

    let kafka_context = KafkaContext::shared()
        .await
        .expect("could not create kafka context");
    let admin_client = utils::admin::create_admin_client(&kafka_context.bootstrap_servers)
        .await
        .expect("could not create admin client");
    let topic_name = rand_test_topic("test_describe_consumer_groups");
    let group_name = rand_test_group();
    let unknown_group_name = rand_test_group();
    admin_client
        .create_topics(
            &[NewTopic::new(&topic_name, 2, TopicReplication::Fixed(1))],
            &AdminOptions::default(),
        )
        .await
        .expect("could not create topic");

    // One address family, so both members connect from the same host.
    let create_member = || {
        consumer::create_base_consumer(
            &kafka_context.bootstrap_servers,
            &group_name,
            Some(&[("broker.address.family", "v4")]),
        )
        .expect("could not create base consumer")
    };
    let members = [create_member(), create_member()];
    for member in &members {
        member.subscribe(&[topic_name.as_str()]).unwrap();
    }
    // Poll until the rebalance has handed each member one partition.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for member in &members {
            let _ = member.poll(Duration::from_millis(100));
        }
        let assigned = members
            .iter()
            .map(|m| m.assignment().unwrap().count())
            .collect::<Vec<_>>();
        if assigned == [1, 1] {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "members never reached one partition each: {assigned:?}"
        );
    }

    let res = admin_client
        .describe_consumer_groups(
            [group_name.as_str(), unknown_group_name.as_str()],
            &AdminOptions::default(),
        )
        .await
        .expect("describe failed");
    assert_eq!(res.len(), 2);

    let described = res[0].as_ref().expect("the live group is described");
    assert_eq!(described.group_id, group_name);
    let mut described_members = described
        .members
        .iter()
        .map(|m| {
            let partitions = m
                .assignment
                .elements()
                .iter()
                .map(|e| (e.topic().to_owned(), e.partition()))
                .collect::<Vec<_>>();
            (m.consumer_id.clone(), m.host.clone(), partitions)
        })
        .collect::<Vec<_>>();
    described_members.sort();
    let mut expected = members
        .iter()
        .map(|m| {
            let partitions = m
                .assignment()
                .unwrap()
                .elements()
                .iter()
                .map(|e| (e.topic().to_owned(), e.partition()))
                .collect::<Vec<_>>();
            (m.member_id().unwrap(), partitions)
        })
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(
        described_members
            .iter()
            .map(|(id, _, partitions)| (id.clone(), partitions.clone()))
            .collect::<Vec<_>>(),
        expected
    );
    // Both members run in this process, so the coordinator reports one host
    // for them, and it is not a member id.
    let hosts = described_members
        .iter()
        .map(|(_, host, _)| host.as_str())
        .collect::<Vec<_>>();
    assert!(
        !hosts[0].is_empty(),
        "the coordinator reports each member's host"
    );
    assert_eq!(hosts[0], hosts[1]);
    assert!(described_members
        .iter()
        .all(|(id, _, _)| !hosts.contains(&id.as_str())));

    // The coordinator describes a group it does not know with no members and
    // no error.
    assert_eq!(
        res[1],
        Ok(ConsumerGroupDescription {
            group_id: unknown_group_name,
            members: vec![],
        })
    );
}

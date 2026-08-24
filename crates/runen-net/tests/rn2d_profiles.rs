mod support;

use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};

use runen_net::delivery::{
    DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryScopeLimits,
    FlowDirection, FlowResourcePolicy, OutboundPressureBehavior, ReceiverPressureBehavior,
    SubmissionOutcome,
};
use runen_net::identity::{ConnectionHandle, ParticipantId, SessionId, SimulationTick};
use runen_net::protocol::{
    CodecId, CompatibilityOffer, ConnectionNegotiationTermination, NegotiatedContract,
    NegotiationManager, NegotiationManagerLimits, NegotiationRequirements, NegotiationStatus,
    OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision, RequirementLevel, SchemaContractId,
    SchemaContractOffer, SchemaId, SchemaOffer, SelectedSchema,
};
use runen_net::replication::{
    AccountedState, AuthorityAckOutcome, AuthorityAggregateLimits, AuthorityRecoveryReason,
    AuthorityReplicationSession, AuthorityReplicationState, ClientAggregateLimits,
    ClientRecoveryReason, ClientReplicationSet, ClientReplicationState, ClientSnapshotOutcome,
    DeltaSnapshot, FullSnapshot, ReplicationCursor, ReplicationLineageKey,
    ReplicationRetentionLimits,
};
use runen_net::session::{
    ConnectionLossOutcome, RetentionPolicy, Session, SessionLimits,
};

use support::FaultStage;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn delivery_limits() -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(16), nz(64), nz(1024))
}

fn reliable_policy() -> FlowResourcePolicy {
    FlowResourcePolicy::new(
        nz(32),
        nz(16),
        nz(256),
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::TerminateReliable,
    )
}

fn flow(connection: ConnectionHandle, direction: FlowDirection, handle: u64) -> DeliveryFlowKey {
    DeliveryFlowKey::new(connection, direction, DeliveryFlowHandle::new(handle))
}

fn protocol_contract() -> ProtocolContract {
    ProtocolContract::new(ProtocolId::new(10), ProtocolRevision::new(20))
}

fn schema_id() -> SchemaId {
    SchemaId::new(30)
}

fn schema_binding() -> SelectedSchema {
    SelectedSchema::new(SchemaContractId::new(40), CodecId::new(50))
}

fn compatibility_offer() -> CompatibilityOffer {
    CompatibilityOffer::new(
        vec![protocol_contract()],
        vec![],
        vec![SchemaOffer::new(
            schema_id(),
            RequirementLevel::Required,
            vec![SchemaContractOffer::new(
                schema_binding().contract_id,
                vec![schema_binding().codec_id],
            )],
        )],
        None,
    )
}

fn negotiation_manager() -> NegotiationManager {
    NegotiationManager::new(OfferLimits::default(), NegotiationManagerLimits::default()).unwrap()
}

fn establish_negotiation(
    manager: &mut NegotiationManager,
    connection: ConnectionHandle,
) -> NegotiatedContract {
    let offer = compatibility_offer();
    manager.start(connection, offer.clone(), offer).unwrap();

    let mut contract = NegotiatedContract::new(protocol_contract());
    contract.bind_schema(schema_id(), schema_binding()).unwrap();
    manager
        .propose(
            connection,
            contract.clone(),
            &NegotiationRequirements::default(),
        )
        .unwrap();
    assert_ne!(
        manager.validate_authority(connection, &contract).unwrap(),
        NegotiationStatus::Established
    );
    assert_eq!(
        manager.validate_peer(connection, &contract).unwrap(),
        NegotiationStatus::Established
    );
    contract
}

fn session_limits() -> SessionLimits {
    SessionLimits::new(nz(16), nz(8)).unwrap()
}

fn retention_limits() -> ReplicationRetentionLimits {
    ReplicationRetentionLimits::new(nz(64), nz(8), nz(256), nz(64), nz(8)).unwrap()
}

fn state(value: i32) -> AccountedState<BTreeMap<&'static str, i32>> {
    AccountedState::new(BTreeMap::from([("value", value)]), 8)
}

fn assert_reliable_exposure(
    endpoint: &mut DeliveryEndpoint,
    key: DeliveryFlowKey,
    expected: &[&[u8]],
) {
    for payload in expected {
        let exposed = endpoint
            .poll_exposure(key)
            .unwrap()
            .expect("expected complete reliable message exposure");
        assert_eq!(exposed.payload(), *payload);
    }
    assert!(endpoint.poll_exposure(key).unwrap().is_none());
}

#[test]
fn core_profile_composes_negotiation_session_delivery_and_replacement() {
    let session_id = SessionId::new(100);
    let participant = ParticipantId::new(7);
    let first_connection = ConnectionHandle::new(1);
    let replacement = ConnectionHandle::new(2);
    let mut session = Session::new(session_id, session_limits());

    let aggregate = delivery_limits();
    let policy = reliable_policy();
    let first_outbound = flow(first_connection, FlowDirection::Outbound, 1);
    let first_inbound = flow(first_connection, FlowDirection::Inbound, 1);
    let mut sender = DeliveryEndpoint::new(aggregate);
    let mut receiver = DeliveryEndpoint::new(aggregate);
    sender
        .establish_flow(first_outbound, DeliveryMode::ReliableOrdered, policy, aggregate)
        .unwrap();
    receiver
        .establish_flow(first_inbound, DeliveryMode::ReliableOrdered, policy, aggregate)
        .unwrap();

    // Pre-admission delivery does not create participant membership or authority.
    assert!(matches!(
        sender.submit(first_outbound, b"bootstrap-a".to_vec()).unwrap(),
        SubmissionOutcome::Accepted { .. }
    ));
    assert!(matches!(
        sender.submit(first_outbound, b"bootstrap-b".to_vec()).unwrap(),
        SubmissionOutcome::Accepted { .. }
    ));
    assert_eq!(session.live_memberships(), 0);
    assert!(!session.is_authorized(participant, first_connection));

    let mut negotiation = negotiation_manager();
    let offer = compatibility_offer();
    negotiation
        .start(first_connection, offer.clone(), offer)
        .unwrap();
    assert!(negotiation.established(first_connection).is_err());
    assert_eq!(session.live_memberships(), 0);

    let mut contract = NegotiatedContract::new(protocol_contract());
    contract.bind_schema(schema_id(), schema_binding()).unwrap();
    negotiation
        .propose(
            first_connection,
            contract.clone(),
            &NegotiationRequirements::default(),
        )
        .unwrap();
    negotiation
        .validate_authority(first_connection, &contract)
        .unwrap();
    assert!(negotiation.established(first_connection).is_err());
    assert_eq!(
        negotiation
            .validate_peer(first_connection, &contract)
            .unwrap(),
        NegotiationStatus::Established
    );

    let established = negotiation.established(first_connection).unwrap();
    assert_eq!(established.contract().protocol(), protocol_contract());
    assert_eq!(
        established.contract().schema_binding(schema_id()),
        Some(schema_binding())
    );
    assert_eq!(
        established.contract().schema_binding(SchemaId::new(999)),
        None
    );
    session.admit_new(participant, established).unwrap();
    assert!(session.is_authorized(participant, first_connection));

    // The same fixed ReliableOrdered contract survives deterministic reordering
    // and duplication without changing delivery intent.
    let mut stage = FaultStage::new(4, 128);
    assert!(stage.take(&mut sender, first_outbound, first_inbound));
    assert!(stage.take(&mut sender, first_outbound, first_inbound));
    stage.swap(0, 1);
    assert!(stage.duplicate(0));
    assert_eq!(
        sender.flow_contract(first_outbound).unwrap().0,
        DeliveryMode::ReliableOrdered
    );
    stage.deliver(0, &mut receiver);
    stage.deliver(1, &mut receiver);
    stage.deliver(0, &mut receiver);
    assert_reliable_exposure(
        &mut receiver,
        first_inbound,
        &[b"bootstrap-a", b"bootstrap-b"],
    );

    let outcome = session
        .connection_lost(
            participant,
            first_connection,
            RetentionPolicy::RetainForRecovery {
                duration: NonZeroU64::new(10).unwrap(),
            },
        )
        .unwrap();
    assert_eq!(outcome, ConnectionLossOutcome::Retained { expires_at: 10 });
    assert!(!session.is_authorized(participant, first_connection));
    sender.terminate_connection(first_connection);
    receiver.terminate_connection(first_connection);
    assert!(sender.flow_contract(first_outbound).is_none());
    assert_eq!(
        negotiation.terminate(first_connection).unwrap(),
        ConnectionNegotiationTermination::EstablishedContractEnded
    );
    assert!(negotiation.established(first_connection).is_err());

    let replacement_contract = establish_negotiation(&mut negotiation, replacement);
    assert_eq!(
        replacement_contract.schema_binding(schema_id()),
        Some(schema_binding())
    );
    session
        .bind_replacement(participant, negotiation.established(replacement).unwrap())
        .unwrap();
    assert!(session.is_authorized(participant, replacement));
    assert!(!session.is_authorized(participant, first_connection));

    let replacement_outbound = flow(replacement, FlowDirection::Outbound, 1);
    sender
        .establish_flow(
            replacement_outbound,
            DeliveryMode::ReliableOrdered,
            policy,
            aggregate,
        )
        .unwrap();
    assert_eq!(
        sender.submit(replacement_outbound, b"fresh".to_vec()).unwrap(),
        SubmissionOutcome::Accepted {
            accepted_index: 0,
            local_pressure_drops: 0,
        }
    );
}

#[test]
fn authoritative_replication_profile_composes_delivery_ack_recovery_and_replacement() {
    let session_id = SessionId::new(200);
    let participant = ParticipantId::new(9);
    let first_connection = ConnectionHandle::new(11);
    let replacement = ConnectionHandle::new(12);

    let mut negotiation = negotiation_manager();
    establish_negotiation(&mut negotiation, first_connection);
    let mut session = Session::new(session_id, session_limits());
    session
        .admit_new(
            participant,
            negotiation.established(first_connection).unwrap(),
        )
        .unwrap();

    let key = ReplicationLineageKey::new(session_id, participant);
    let mut client = ClientReplicationSet::new(ClientAggregateLimits::new(nz(4), nz(16), nz(512)));
    client.add_lineage(key, retention_limits()).unwrap();
    let mut authority = AuthorityReplicationSession::<BTreeMap<&'static str, i32>, i32>::new(
        session_id,
        AuthorityAggregateLimits::new(nz(4), nz(512), nz(16), nz(512), nz(16)),
    );
    assert_eq!(authority.add_lineage(participant, retention_limits()).unwrap(), key);

    let aggregate = delivery_limits();
    let policy = reliable_policy();
    let first_outbound = flow(first_connection, FlowDirection::Outbound, 7);
    let first_inbound = flow(first_connection, FlowDirection::Inbound, 7);
    let mut sender = DeliveryEndpoint::new(aggregate);
    let mut receiver = DeliveryEndpoint::new(aggregate);
    sender
        .establish_flow(first_outbound, DeliveryMode::ReliableOrdered, policy, aggregate)
        .unwrap();
    receiver
        .establish_flow(first_inbound, DeliveryMode::ReliableOrdered, policy, aggregate)
        .unwrap();
    let mut stage = FaultStage::new(8, 256);

    let full_one = FullSnapshot::new(
        ReplicationCursor::new(1),
        SimulationTick::new(1),
        state(1),
    );
    authority
        .prepare_full(participant, full_one.clone(), true)
        .unwrap();

    let rejected = sender.submit(first_outbound, vec![0; 33]).unwrap();
    assert_eq!(rejected, SubmissionOutcome::RejectedTooLarge);
    assert_eq!(
        authority
            .record_delivery_submission(participant, rejected)
            .unwrap(),
        None
    );
    assert_eq!(
        authority.lineage(participant).unwrap().greatest_emitted_cursor(),
        None
    );

    let accepted_full = sender.submit(first_outbound, b"full-1".to_vec()).unwrap();
    authority
        .record_delivery_submission(participant, accepted_full)
        .unwrap()
        .expect("RN1B acceptance records snapshot emission");
    assert!(stage.take(&mut sender, first_outbound, first_inbound));
    stage.deliver(0, &mut receiver);
    assert_reliable_exposure(&mut receiver, first_inbound, &[b"full-1"]);
    assert_eq!(
        client
            .apply_full(key, full_one, |_| Ok::<_, ()>(()))
            .unwrap(),
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(1))
    );
    assert_eq!(
        authority
            .acknowledge_authorized(
                &session,
                first_connection,
                participant,
                ReplicationCursor::new(1),
            )
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    assert_eq!(
        authority.lineage(participant).unwrap().replication_state(),
        AuthorityReplicationState::DeltaEligible(ReplicationCursor::new(1))
    );

    // Loss of a newer delta does not move the client and does not make the
    // authority pick a different historical base while confirmation is unchanged.
    let delta_two = authority
        .prepare_delta(
            participant,
            ReplicationCursor::new(2),
            SimulationTick::new(2),
            state(2),
            1,
            0,
        )
        .unwrap();
    assert_eq!(delta_two.base_cursor, Some(ReplicationCursor::new(1)));
    let accepted_delta_two = sender.submit(first_outbound, b"delta-2".to_vec()).unwrap();
    authority
        .record_delivery_submission(participant, accepted_delta_two)
        .unwrap();
    assert!(stage.take(&mut sender, first_outbound, first_inbound));
    stage.drop_at(0);
    assert_eq!(
        client.lineage(key).unwrap().current_cursor(),
        Some(ReplicationCursor::new(1))
    );

    let delta_three = authority
        .prepare_delta(
            participant,
            ReplicationCursor::new(3),
            SimulationTick::new(3),
            state(3),
            2,
            0,
        )
        .unwrap();
    assert_eq!(delta_three.base_cursor, Some(ReplicationCursor::new(1)));
    let accepted_delta_three = sender.submit(first_outbound, b"delta-3".to_vec()).unwrap();
    authority
        .record_delivery_submission(participant, accepted_delta_three)
        .unwrap();
    assert!(stage.take(&mut sender, first_outbound, first_inbound));
    stage.deliver(0, &mut receiver);
    assert_reliable_exposure(&mut receiver, first_inbound, &[b"delta-3"]);

    let mut reconstructed_from = None;
    assert_eq!(
        client
            .apply_delta(
                key,
                DeltaSnapshot::new(
                    ReplicationCursor::new(1),
                    ReplicationCursor::new(3),
                    SimulationTick::new(3),
                    2,
                ),
                |base, delta, _| {
                    reconstructed_from = base.get("value").copied();
                    Ok(AccountedState::new(
                        BTreeMap::from([(
                            "value",
                            base.get("value").copied().unwrap() + *delta,
                        )]),
                        8,
                    ))
                },
                |_| Ok::<_, ()>(()),
            )
            .unwrap(),
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(3))
    );
    assert_eq!(reconstructed_from, Some(1));
    assert_eq!(client.lineage(key).unwrap().current_state(), Some(&BTreeMap::from([("value", 3)])));
    assert_eq!(
        authority
            .acknowledge_authorized(
                &session,
                first_connection,
                participant,
                ReplicationCursor::new(3),
            )
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    assert_eq!(
        authority.lineage(participant).unwrap().replication_state(),
        AuthorityReplicationState::DeltaEligible(ReplicationCursor::new(3))
    );

    // Evicting the exact latest-confirmed baseline crosses into full recovery.
    assert!(
        authority
            .evict_retained_state(participant, ReplicationCursor::new(3))
            .unwrap()
    );
    let generation_before_replacement = match authority
        .lineage(participant)
        .unwrap()
        .replication_state()
    {
        AuthorityReplicationState::FullSnapshotRequired { generation, .. } => generation,
        other => panic!("expected full recovery after baseline eviction, got {other:?}"),
    };

    let full_four = FullSnapshot::new(
        ReplicationCursor::new(4),
        SimulationTick::new(4),
        state(4),
    );
    authority
        .prepare_full(participant, full_four.clone(), true)
        .unwrap();
    let accepted_full_four = sender.submit(first_outbound, b"full-4".to_vec()).unwrap();
    authority
        .record_delivery_submission(participant, accepted_full_four)
        .unwrap();
    assert!(stage.take(&mut sender, first_outbound, first_inbound));
    stage.deliver(0, &mut receiver);
    assert_reliable_exposure(&mut receiver, first_inbound, &[b"full-4"]);
    assert_eq!(
        client
            .apply_full(key, full_four, |_| Ok::<_, ()>(()))
            .unwrap(),
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(4))
    );

    session
        .connection_lost(
            participant,
            first_connection,
            RetentionPolicy::RetainForRecovery {
                duration: NonZeroU64::new(10).unwrap(),
            },
        )
        .unwrap();
    sender.terminate_connection(first_connection);
    receiver.terminate_connection(first_connection);
    negotiation.terminate(first_connection).unwrap();

    establish_negotiation(&mut negotiation, replacement);
    session
        .bind_replacement(participant, negotiation.established(replacement).unwrap())
        .unwrap();
    client.require_connection_replacement_full(key).unwrap();
    authority
        .connection_replaced(&session, replacement, participant)
        .unwrap();

    let generation_after_replacement = match authority
        .lineage(participant)
        .unwrap()
        .replication_state()
    {
        AuthorityReplicationState::FullSnapshotRequired {
            reason: AuthorityRecoveryReason::ConnectionReplacement,
            generation,
            ..
        } => generation,
        other => panic!("expected replacement recovery generation, got {other:?}"),
    };
    assert_ne!(generation_after_replacement, generation_before_replacement);
    assert_eq!(
        client.lineage(key).unwrap().replication_state(),
        ClientReplicationState::FullSnapshotRequired(ClientRecoveryReason::ConnectionReplacement)
    );

    // A truthful ACK for the old recovery generation may advance confirmation,
    // but it cannot satisfy the newer replacement generation.
    assert_eq!(
        authority
            .acknowledge_authorized(
                &session,
                replacement,
                participant,
                ReplicationCursor::new(4),
            )
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    assert!(matches!(
        authority.lineage(participant).unwrap().replication_state(),
        AuthorityReplicationState::FullSnapshotRequired {
            reason: AuthorityRecoveryReason::ConnectionReplacement,
            ..
        }
    ));

    let replacement_outbound = flow(replacement, FlowDirection::Outbound, 7);
    let replacement_inbound = flow(replacement, FlowDirection::Inbound, 7);
    sender
        .establish_flow(
            replacement_outbound,
            DeliveryMode::ReliableOrdered,
            policy,
            aggregate,
        )
        .unwrap();
    receiver
        .establish_flow(
            replacement_inbound,
            DeliveryMode::ReliableOrdered,
            policy,
            aggregate,
        )
        .unwrap();

    let full_five = FullSnapshot::new(
        ReplicationCursor::new(5),
        SimulationTick::new(5),
        state(5),
    );
    authority
        .prepare_full(participant, full_five.clone(), true)
        .unwrap();
    let accepted_full_five = sender
        .submit(replacement_outbound, b"full-5".to_vec())
        .unwrap();
    assert_eq!(
        accepted_full_five,
        SubmissionOutcome::Accepted {
            accepted_index: 0,
            local_pressure_drops: 0,
        }
    );
    authority
        .record_delivery_submission(participant, accepted_full_five)
        .unwrap();
    assert!(stage.take(
        &mut sender,
        replacement_outbound,
        replacement_inbound
    ));
    stage.deliver(0, &mut receiver);
    assert_reliable_exposure(&mut receiver, replacement_inbound, &[b"full-5"]);
    assert_eq!(
        client
            .apply_full(key, full_five, |_| Ok::<_, ()>(()))
            .unwrap(),
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(5))
    );
    assert_eq!(
        authority
            .acknowledge_authorized(
                &session,
                replacement,
                participant,
                ReplicationCursor::new(5),
            )
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    assert_eq!(
        authority.lineage(participant).unwrap().replication_state(),
        AuthorityReplicationState::DeltaEligible(ReplicationCursor::new(5))
    );
}

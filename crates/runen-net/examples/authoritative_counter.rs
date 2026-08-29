//! Plain-Rust standalone authoritative replication proof.
//!
//! This example intentionally uses only the public `runen-net` API and
//! standard-library host state. The marker bytes below are illustrative local
//! delivery payloads, not a standardized RunenNet wire encoding.

use std::num::NonZeroUsize;

use runen_net::delivery::{
    DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryScopeLimits,
    FlowDirection, FlowResourcePolicy, OutboundPressureBehavior, ReceiveOutcome,
    ReceiverPressureBehavior, SubmissionOutcome,
};
use runen_net::identity::{ConnectionHandle, ParticipantId, SessionId, SimulationTick};
use runen_net::protocol::{
    CodecId, CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerLimits,
    NegotiationRequirements, NegotiationStatus, OfferLimits, ProtocolContract, ProtocolId,
    ProtocolRevision, RequirementLevel, SchemaContractId, SchemaContractOffer, SchemaId,
    SchemaOffer, SelectedSchema,
};
use runen_net::replication::{
    AccountedState, AuthorityAckOutcome, AuthorityAggregateLimits, AuthorityReplicationSession,
    AuthorityReplicationState, ClientAggregateLimits, ClientReplicationSet, ClientSnapshotOutcome,
    DeltaReconstructionError, DeltaSnapshot, FullSnapshot, ReplicationCursor,
    ReplicationLineageKey, ReplicationRetentionLimits,
};
use runen_net::session::{Session, SessionLimits};

#[derive(Debug, Clone, PartialEq, Eq)]
struct CounterState {
    value: i32,
}

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn accounted(state: &CounterState) -> AccountedState<CounterState> {
    AccountedState::new(state.clone(), size_of::<CounterState>())
}

fn protocol_contract() -> ProtocolContract {
    ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(1))
}

fn schema_id() -> SchemaId {
    SchemaId::new(1)
}

fn schema_binding() -> SelectedSchema {
    SelectedSchema::new(SchemaContractId::new(1), CodecId::new(1))
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
        manager.validate_authority(connection).unwrap(),
        NegotiationStatus::Established
    );
    assert_eq!(
        manager.validate_peer(connection).unwrap(),
        NegotiationStatus::Established
    );
    contract
}

fn transfer_one(
    sender: &mut DeliveryEndpoint,
    outbound: DeliveryFlowKey,
    receiver: &mut DeliveryEndpoint,
    inbound: DeliveryFlowKey,
    expected_payload: &[u8],
) {
    let preview = sender
        .peek_outbound(outbound)
        .unwrap()
        .expect("accepted message remains in sender custody");
    let transfer = sender
        .commit_outbound_custody(outbound, preview.accepted_index())
        .unwrap();
    assert_eq!(
        receiver.receive(inbound, transfer).unwrap(),
        ReceiveOutcome::Buffered {
            local_pressure_drops: 0
        }
    );
    let exposed = receiver
        .poll_exposure(inbound)
        .unwrap()
        .expect("direct realization exposes the complete message");
    assert_eq!(exposed.payload(), expected_payload);
}

fn run() {
    let session_id = SessionId::new(1);
    let participant = ParticipantId::new(1);
    let connection = ConnectionHandle::new(1);

    // Compatibility is established before session admission.
    let mut negotiation =
        NegotiationManager::new(OfferLimits::default(), NegotiationManagerLimits::default())
            .unwrap();
    establish_negotiation(&mut negotiation, connection);
    let established = negotiation.established(connection).unwrap();
    assert_eq!(established.contract().protocol(), protocol_contract());
    assert_eq!(
        established.contract().schema_binding(schema_id()),
        Some(schema_binding())
    );

    let mut session = Session::new(session_id, SessionLimits::new(nz(4), nz(2)).unwrap());
    session.admit_new(participant, established).unwrap();
    assert!(session.is_authorized(participant, connection));

    // The example explicitly selects one delivery contract. Payload size does
    // not choose or alter this mode.
    let delivery_limits = DeliveryScopeLimits::new(nz(2), nz(16), nz(512));
    let delivery_policy = FlowResourcePolicy::new(
        nz(64),
        nz(8),
        nz(256),
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::TerminateReliable,
    );
    let outbound = DeliveryFlowKey::new(
        connection,
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(1),
    );
    let inbound = DeliveryFlowKey::new(
        connection,
        FlowDirection::Inbound,
        DeliveryFlowHandle::new(1),
    );
    let mut sender = DeliveryEndpoint::new(delivery_limits);
    let mut receiver = DeliveryEndpoint::new(delivery_limits);
    sender
        .establish_flow(
            outbound,
            DeliveryMode::ReliableOrdered,
            delivery_policy,
            delivery_limits,
        )
        .unwrap();
    receiver
        .establish_flow(
            inbound,
            DeliveryMode::ReliableOrdered,
            delivery_policy,
            delivery_limits,
        )
        .unwrap();

    let retention = ReplicationRetentionLimits::new(nz(64), nz(4), nz(256), nz(64), nz(8)).unwrap();
    let lineage = ReplicationLineageKey::new(session_id, participant);
    let mut authority = AuthorityReplicationSession::<CounterState, i32>::new(
        session_id,
        AuthorityAggregateLimits::new(nz(2), nz(512), nz(8), nz(512), nz(8)),
    );
    authority.add_lineage(participant, retention).unwrap();
    let mut client = ClientReplicationSet::new(ClientAggregateLimits::new(nz(2), nz(8), nz(512)));
    client.add_lineage(lineage, retention).unwrap();

    // Server and client application state are independently owned ordinary
    // Rust values. RunenNet owns only its protocol/retention state.
    let mut server_state = CounterState { value: 10 };
    let mut client_state = CounterState { value: 0 };

    // Establish the initial full baseline. Building the snapshot is not enough:
    // only delivery Accepted makes it acknowledgement-eligible.
    let full = FullSnapshot::new(
        ReplicationCursor::new(1),
        SimulationTick::new(1),
        accounted(&server_state),
    );
    authority
        .prepare_full(participant, full.clone(), true)
        .unwrap();
    let full_submission = sender.submit(outbound, b"full:1".to_vec()).unwrap();
    assert!(matches!(
        full_submission,
        SubmissionOutcome::Accepted { .. }
    ));
    authority
        .record_delivery_acceptance(participant, full_submission.acceptance())
        .unwrap()
        .expect("accepted delivery records full-snapshot emission");
    transfer_one(&mut sender, outbound, &mut receiver, inbound, b"full:1");
    assert_eq!(
        client
            .apply_full(lineage, full, |state| {
                client_state = state.clone();
                Ok::<_, ()>(())
            })
            .unwrap(),
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(1))
    );
    assert_eq!(client_state, server_state);
    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(1),)
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    assert_eq!(
        authority.lineage(participant).unwrap().replication_state(),
        AuthorityReplicationState::DeltaEligible(ReplicationCursor::new(1))
    );

    // Advance independent server state, then construct the delta from the
    // authority's exact latest-confirmed retained baseline.
    server_state.value += 5;
    let prepared_delta = authority
        .prepare_delta(
            participant,
            ReplicationCursor::new(2),
            SimulationTick::new(2),
            accounted(&server_state),
            5,
            0,
        )
        .unwrap();
    assert_eq!(prepared_delta.base_cursor, Some(ReplicationCursor::new(1)));
    let delta_submission = sender.submit(outbound, b"delta:2".to_vec()).unwrap();
    assert!(matches!(
        delta_submission,
        SubmissionOutcome::Accepted { .. }
    ));
    authority
        .record_delivery_acceptance(participant, delta_submission.acceptance())
        .unwrap()
        .expect("accepted delivery records delta emission");
    transfer_one(&mut sender, outbound, &mut receiver, inbound, b"delta:2");

    // Reconstruction receives the exact declared retained baseline. The host
    // commit callback installs the complete candidate into separate client state.
    let mut reconstructed_from = None;
    assert_eq!(
        client
            .apply_delta(
                lineage,
                DeltaSnapshot::new(
                    ReplicationCursor::new(1),
                    ReplicationCursor::new(2),
                    SimulationTick::new(2),
                    5,
                ),
                |base, delta, _candidate_limit| {
                    reconstructed_from = Some(base.value);
                    Ok::<_, DeltaReconstructionError>(AccountedState::new(
                        CounterState {
                            value: base.value + *delta,
                        },
                        size_of::<CounterState>(),
                    ))
                },
                |state| {
                    client_state = state.clone();
                    Ok::<_, ()>(())
                },
            )
            .unwrap(),
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(2))
    );
    assert_eq!(reconstructed_from, Some(10));
    assert_eq!(client_state, server_state);
    assert_eq!(
        client.lineage(lineage).unwrap().current_cursor(),
        Some(ReplicationCursor::new(2))
    );
    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(2),)
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    assert_eq!(
        authority.lineage(participant).unwrap().replication_state(),
        AuthorityReplicationState::DeltaEligible(ReplicationCursor::new(2))
    );

    println!(
        "standalone authoritative counter synchronized at value {}",
        client_state.value
    );
}

fn main() {
    run();
}

#[cfg(test)]
mod tests {
    #[test]
    fn standalone_authoritative_counter_runs() {
        super::run();
    }
}

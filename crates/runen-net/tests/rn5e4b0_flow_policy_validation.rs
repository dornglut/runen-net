use std::num::NonZeroUsize;

use runen_net::{
    delivery::{
        DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryPolicyError,
        DeliveryScopeLimits, FlowDirection, FlowEstablishmentError, FlowResourcePolicy,
        OutboundPressureBehavior, ReceiverPressureBehavior,
    },
    identity::ConnectionHandle,
};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn limits() -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(4), nz(8), nz(1024))
}

fn policy(
    outbound: OutboundPressureBehavior,
    receiver: ReceiverPressureBehavior,
) -> FlowResourcePolicy {
    FlowResourcePolicy::new(nz(256), nz(4), nz(1024), outbound, receiver)
}

#[test]
fn flow_policy_validation_exposes_the_existing_mode_rules_without_mutation() {
    let reliable = policy(
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::TerminateReliable,
    );
    assert_eq!(
        reliable.validate_for_mode(DeliveryMode::ReliableOrdered),
        Ok(())
    );

    let reliable_evicts = policy(
        OutboundPressureBehavior::EvictOldestUnreliable,
        ReceiverPressureBehavior::TerminateReliable,
    );
    assert_eq!(
        reliable_evicts.validate_for_mode(DeliveryMode::ReliableOrdered),
        Err(DeliveryPolicyError::ReliableOutboundMustRejectNew)
    );

    let reliable_drops = policy(
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    assert_eq!(
        reliable_drops.validate_for_mode(DeliveryMode::ReliableOrdered),
        Err(DeliveryPolicyError::ReliableReceiverMustTerminate)
    );

    let unreliable_terminates = policy(
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::TerminateReliable,
    );
    assert_eq!(
        unreliable_terminates.validate_for_mode(DeliveryMode::UnreliableUnordered),
        Err(DeliveryPolicyError::UnreliableReceiverPolicyRequired)
    );
}

#[test]
fn read_only_validation_does_not_establish_or_reserve_a_flow() {
    let limits = limits();
    let mut endpoint = DeliveryEndpoint::new(limits);
    let valid_key = DeliveryFlowKey::new(
        ConnectionHandle::new(1),
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(1),
    );
    let invalid_key = DeliveryFlowKey::new(
        ConnectionHandle::new(1),
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(2),
    );
    let valid = policy(
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::TerminateReliable,
    );
    let invalid = policy(
        OutboundPressureBehavior::EvictOldestUnreliable,
        ReceiverPressureBehavior::TerminateReliable,
    );

    assert_eq!(endpoint.active_flows(), 0);
    assert_eq!(
        valid.validate_for_mode(DeliveryMode::ReliableOrdered),
        Ok(())
    );
    assert_eq!(endpoint.active_flows(), 0);
    assert_eq!(endpoint.flow_contract(valid_key), None);

    endpoint
        .establish_flow(valid_key, DeliveryMode::ReliableOrdered, valid, limits)
        .unwrap();
    assert_eq!(endpoint.active_flows(), 1);
    assert_eq!(
        endpoint.flow_contract(valid_key),
        Some((DeliveryMode::ReliableOrdered, valid))
    );

    assert_eq!(
        invalid.validate_for_mode(DeliveryMode::ReliableOrdered),
        Err(DeliveryPolicyError::ReliableOutboundMustRejectNew)
    );
    assert_eq!(endpoint.active_flows(), 1);
    assert_eq!(endpoint.flow_contract(invalid_key), None);
    assert_eq!(
        endpoint.establish_flow(invalid_key, DeliveryMode::ReliableOrdered, invalid, limits,),
        Err(FlowEstablishmentError::InvalidPolicy(
            DeliveryPolicyError::ReliableOutboundMustRejectNew
        ))
    );
    assert_eq!(endpoint.active_flows(), 1);
    assert_eq!(endpoint.flow_contract(invalid_key), None);
}

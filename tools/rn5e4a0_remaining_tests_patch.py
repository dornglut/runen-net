from pathlib import Path


def replace_once(path_name: str, old: str, new: str, label: str) -> None:
    path = Path(path_name)
    text = path.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match, found {count}")
    path.write_text(text.replace(old, new, 1))


replace_once(
    "crates/runen-net/tests/rn2d_profiles.rs",
    """    manager\n        .propose(\n            connection,\n            contract.clone(),\n            &NegotiationRequirements::default(),\n        )\n        .unwrap();\n    assert_ne!(\n        manager.validate_authority(connection, &contract).unwrap(),\n        NegotiationStatus::Established\n    );\n    assert_eq!(\n        manager.validate_peer(connection, &contract).unwrap(),\n        NegotiationStatus::Established\n    );\n""",
    """    manager\n        .propose(\n            connection,\n            contract.clone(),\n            &NegotiationRequirements::default(),\n        )\n        .unwrap();\n    assert_ne!(\n        manager.validate_authority(connection).unwrap(),\n        NegotiationStatus::Established\n    );\n    assert_eq!(\n        manager.validate_peer(connection).unwrap(),\n        NegotiationStatus::Established\n    );\n""",
    "rn2d negotiation helper",
)

replace_once(
    "crates/runen-net/tests/rn4c_recovery_pressure.rs",
    """    let contract = NegotiatedContract::new(protocol());\n    manager\n        .propose(\n            connection,\n            contract.clone(),\n            &NegotiationRequirements::default(),\n        )\n        .unwrap();\n    manager.validate_authority(connection, &contract).unwrap();\n    manager.validate_peer(connection, &contract).unwrap();\n""",
    """    manager\n        .propose(\n            connection,\n            NegotiatedContract::new(protocol()),\n            &NegotiationRequirements::default(),\n        )\n        .unwrap();\n    manager.validate_authority(connection).unwrap();\n    manager.validate_peer(connection).unwrap();\n""",
    "rn4c authorized-session helper",
)

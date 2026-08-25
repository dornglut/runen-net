from pathlib import Path

path = Path("crates/runen-net/tests/rn2c_replication.rs")
text = path.read_text()
old = """    let contract = NegotiatedContract::new(protocol());\n    manager\n        .propose(\n            connection,\n            contract.clone(),\n            &NegotiationRequirements::default(),\n        )\n        .unwrap();\n    manager.validate_authority(connection, &contract).unwrap();\n    manager.validate_peer(connection, &contract).unwrap();\n"""
new = """    manager\n        .propose(\n            connection,\n            NegotiatedContract::new(protocol()),\n            &NegotiationRequirements::default(),\n        )\n        .unwrap();\n    manager.validate_authority(connection).unwrap();\n    manager.validate_peer(connection).unwrap();\n"""
count = text.count(old)
if count != 1:
    raise SystemExit(f"rn2c replication establish helper: expected one match, found {count}")
path.write_text(text.replace(old, new, 1))

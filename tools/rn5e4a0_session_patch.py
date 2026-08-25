from pathlib import Path

path = Path("crates/runen-net/src/session.rs")
text = path.read_text()
old = """        let contract = NegotiatedContract::new(protocol());\n        manager\n            .propose(\n                connection,\n                contract.clone(),\n                &NegotiationRequirements::default(),\n            )\n            .unwrap();\n        assert_ne!(\n            manager.validate_authority(connection, &contract).unwrap(),\n            NegotiationStatus::Established\n        );\n        assert_eq!(\n            manager.validate_peer(connection, &contract).unwrap(),\n            NegotiationStatus::Established\n        );\n"""
new = """        manager\n            .propose(\n                connection,\n                NegotiatedContract::new(protocol()),\n                &NegotiationRequirements::default(),\n            )\n            .unwrap();\n        assert_ne!(\n            manager.validate_authority(connection).unwrap(),\n            NegotiationStatus::Established\n        );\n        assert_eq!(\n            manager.validate_peer(connection).unwrap(),\n            NegotiationStatus::Established\n        );\n"""
count = text.count(old)
if count != 1:
    raise SystemExit(f"session establish helper: expected one match, found {count}")
path.write_text(text.replace(old, new, 1))

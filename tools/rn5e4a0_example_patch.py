from pathlib import Path

path = Path("crates/runen-net/examples/authoritative_counter.rs")
text = path.read_text()
old = """    assert_ne!(\n        manager.validate_authority(connection, &contract).unwrap(),\n        NegotiationStatus::Established\n    );\n    assert_eq!(\n        manager.validate_peer(connection, &contract).unwrap(),\n        NegotiationStatus::Established\n    );\n"""
new = """    assert_ne!(\n        manager.validate_authority(connection).unwrap(),\n        NegotiationStatus::Established\n    );\n    assert_eq!(\n        manager.validate_peer(connection).unwrap(),\n        NegotiationStatus::Established\n    );\n"""
count = text.count(old)
if count != 1:
    raise SystemExit(f"authoritative counter negotiation helper: expected one match, found {count}")
path.write_text(text.replace(old, new, 1))

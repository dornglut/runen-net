from pathlib import Path

path = Path("crates/runen-net-quic/src/datagram.rs")
text = path.read_text()
old = '''        assert_eq!(
            sender.drive_one(&mut endpoint, &registry, flow_id),
            Ok(DatagramSendProgress::BlockedNativeBuffer { .. })
        );
'''
new = '''        assert!(matches!(
            sender.drive_one(&mut endpoint, &registry, flow_id),
            Ok(DatagramSendProgress::BlockedNativeBuffer { .. })
        ));
'''
count = text.count(old)
if count != 1:
    raise SystemExit(f"blocked-buffer assertion: expected exactly one match, found {count}")
path.write_text(text.replace(old, new, 1))

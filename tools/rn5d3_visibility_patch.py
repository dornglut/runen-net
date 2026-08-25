from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


path = Path("crates/runen-net-quic/src/datagram.rs")
text = path.read_text()
for old, new, label in [
    ("struct DatagramSenderDiagnostics {", "pub(super) struct DatagramSenderDiagnostics {", "diagnostics visibility"),
    ("enum DatagramTransportError {", "pub(super) enum DatagramTransportError {", "transport error visibility"),
    ("trait DatagramSendTransport {", "pub(super) trait DatagramSendTransport {", "transport trait visibility"),
    ("enum DatagramSubmissionOutcome {", "pub(super) enum DatagramSubmissionOutcome {", "submission outcome visibility"),
    ("enum DatagramSubmissionError {", "pub(super) enum DatagramSubmissionError {", "submission error visibility"),
    ("enum DatagramSendProgress {", "pub(super) enum DatagramSendProgress {", "send progress visibility"),
    ("enum DatagramSendError {", "pub(super) enum DatagramSendError {", "send error visibility"),
    ("struct DatagramSender<T> {", "pub(super) struct DatagramSender<T> {", "sender visibility"),
    ("enum DatagramReceiveOutcome {", "pub(super) enum DatagramReceiveOutcome {", "receive outcome visibility"),
    ("enum DatagramReceiveError {", "pub(super) enum DatagramReceiveError {", "receive error visibility"),
]:
    text = replace_once(text, old, new, label)
text = replace_once(
    text,
    "    fn new_quinn(connection: Connection) -> Self {",
    "    pub(super) fn new_quinn(connection: Connection) -> Self {",
    "quinn constructor visibility",
)
text = replace_once(
    text,
    "    const fn diagnostics(&self) -> DatagramSenderDiagnostics {\n        self.diagnostics\n    }",
    "    pub(super) const fn outbound_transport_drops(&self) -> usize {\n        self.diagnostics.outbound_transport_drops\n    }",
    "diagnostic accessor",
)
text = replace_once(
    text,
    "    fn submit(\n",
    "    pub(super) fn submit(\n",
    "submission visibility",
)
text = replace_once(
    text,
    "    fn drive_one(\n",
    "    pub(super) fn drive_one(\n",
    "handoff visibility",
)
text = replace_once(
    text,
    "fn receive_datagram(\n",
    "pub(super) fn receive_datagram(\n",
    "receive visibility",
)
text = replace_once(
    text,
    "async fn read_quinn_datagram(connection: &Connection) -> Result<impl AsRef<[u8]>, ConnectionError> {",
    "pub(super) async fn read_quinn_datagram(\n    connection: &Connection,\n) -> Result<impl AsRef<[u8]>, ConnectionError> {",
    "quinn read visibility",
)
text = text.replace(
    "sender.diagnostics().outbound_transport_drops",
    "sender.outbound_transport_drops()",
)
path.write_text(text)

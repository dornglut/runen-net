from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


path = Path("crates/runen-net-quic/src/endpoint.rs")
text = path.read_text()

text = replace_once(
    text,
    'const QUIC_MAX_UDP_PAYLOAD: u16 = 65_527;\n',
    'const QUIC_MAX_UDP_PAYLOAD: u16 = 65_527;\nconst QUIC_MAX_STREAM_COUNT: u64 = 1 << 60;\n',
    'stream count constant',
)
text = replace_once(
    text,
    '    IncomingFlowsOutOfRange,\n',
    '    IncomingFlowsExceedQuicStreamLimit,\n',
    'stream count error',
)
text = replace_once(
    text,
    '''        let max_active_incoming_flows = VarInt::from_u64(self.max_active_incoming_flows)\n            .map_err(|_| EndpointResourceError::IncomingFlowsOutOfRange)?;\n''',
    '''        if self.max_active_incoming_flows > QUIC_MAX_STREAM_COUNT {\n            return Err(EndpointResourceError::IncomingFlowsExceedQuicStreamLimit);\n        }\n        let max_active_incoming_flows = VarInt::from_u64(self.max_active_incoming_flows)\n            .map_err(|_| EndpointResourceError::IncomingFlowsExceedQuicStreamLimit)?;\n''',
    'stream count validation',
)
text = text.replace('    let socket = UdpSocket::bind(bind_addr)?;\n', '    let socket = bind_udp_socket(bind_addr)?;\n')
if text.count('let socket = bind_udp_socket(bind_addr)?;') != 2:
    raise SystemExit('socket binding replacement: expected two call sites')

marker = '''fn build_endpoint_config(\n    resources: ValidatedEndpointResources,\n) -> Result<EndpointConfig, EndpointBuildError> {\n'''
helper = '''fn bind_udp_socket(bind_addr: SocketAddr) -> io::Result<UdpSocket> {\n    let socket = UdpSocket::bind(bind_addr)?;\n    socket.set_nonblocking(true)?;\n    Ok(socket)\n}\n\n'''
text = replace_once(text, marker, helper + marker, 'nonblocking helper')

text = replace_once(
    text,
    '''        let mut input = limits();\n        input.max_active_incoming_flows = (1u64 << 62) + 1;\n        assert_eq!(\n            input.validate(),\n            Err(EndpointResourceError::IncomingFlowsOutOfRange)\n        );\n''',
    '''        let mut input = limits();\n        input.max_active_incoming_flows = QUIC_MAX_STREAM_COUNT;\n        assert!(input.validate().is_ok());\n\n        input.max_active_incoming_flows = QUIC_MAX_STREAM_COUNT + 1;\n        assert_eq!(\n            input.validate(),\n            Err(EndpointResourceError::IncomingFlowsExceedQuicStreamLimit)\n        );\n''',
    'stream count boundary test',
)

insert_before = '''    #[test]\n    fn role_mapping_and_udp_ceiling_are_profile_owned() {\n'''
nonblocking_test = '''    #[test]\n    fn udp_socket_binding_is_nonblocking_before_tokio_wrapping() {\n        let socket = bind_udp_socket("127.0.0.1:0".parse().unwrap()).unwrap();\n        let mut byte = [0u8; 1];\n        let error = socket.recv_from(&mut byte).unwrap_err();\n        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);\n    }\n\n'''
text = replace_once(text, insert_before, nonblocking_test + insert_before, 'nonblocking test')

path.write_text(text)

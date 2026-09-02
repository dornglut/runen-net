use std::collections::TryReserveError;

use quinn::{
    Connection, ConnectionError, ReadExactError, RecvStream, SendStream, Side, WriteError,
    crypto::rustls::HandshakeData,
};

use crate::{
    endpoint::ValidatedEndpointResources,
    wire::{
        EncodedVarInt, MAX_VARINT, VarIntDecodeError, VarIntEncodeError, WireSide, decode_varint,
        encode_varint,
    },
};

const RUNENNET_ALPN: &[u8] = b"runennet/1";
const MAX_SETTINGS_BODY_BYTES: usize = 1 + (4 * 8);
const MIN_NEGOTIATION_OFFER_BODY_BYTES: usize = 1 + 32 + 1 + 1;
const QUIC_MAX_STREAM_COUNT: u64 = 1 << 60;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum SemanticRole {
    NonAuthority,
    Authority,
}

impl SemanticRole {
    const fn wire(self) -> u8 {
        match self {
            Self::NonAuthority => 0,
            Self::Authority => 1,
        }
    }

    const fn opposite(self) -> Self {
        match self {
            Self::NonAuthority => Self::Authority,
            Self::Authority => Self::NonAuthority,
        }
    }

    fn from_wire(value: u8) -> Result<Self, SettingsError> {
        match value {
            0 => Ok(Self::NonAuthority),
            1 => Ok(Self::Authority),
            value => Err(SettingsError::UnknownSemanticRole(value)),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) struct LocalControlLimits {
    pub(super) semantic_role: SemanticRole,
    pub(super) max_control_frame_bytes: usize,
    pub(super) max_negotiation_frame_bytes: usize,
    pub(super) max_incoming_message_bytes: u64,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum LocalControlLimitError {
    ControlFrameTooSmall,
    ControlFrameOutOfRange,
    NegotiationFrameTooSmall,
    NegotiationFrameOutOfRange,
    NegotiationExceedsControl,
    ZeroIncomingMessageBytes,
    IncomingMessageBytesOutOfRange,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) struct ValidatedControlProfile {
    resources: ValidatedEndpointResources,
    local_settings: Settings,
}

impl LocalControlLimits {
    pub(super) fn validate(
        self,
        resources: ValidatedEndpointResources,
    ) -> Result<ValidatedControlProfile, LocalControlLimitError> {
        if self.max_control_frame_bytes < MAX_SETTINGS_BODY_BYTES {
            return Err(LocalControlLimitError::ControlFrameTooSmall);
        }
        let max_control_frame_bytes = usize_to_wire(
            self.max_control_frame_bytes,
            LocalControlLimitError::ControlFrameOutOfRange,
        )?;

        if self.max_negotiation_frame_bytes < MIN_NEGOTIATION_OFFER_BODY_BYTES {
            return Err(LocalControlLimitError::NegotiationFrameTooSmall);
        }
        if self.max_negotiation_frame_bytes > self.max_control_frame_bytes {
            return Err(LocalControlLimitError::NegotiationExceedsControl);
        }
        let max_negotiation_frame_bytes = usize_to_wire(
            self.max_negotiation_frame_bytes,
            LocalControlLimitError::NegotiationFrameOutOfRange,
        )?;

        if self.max_incoming_message_bytes == 0 {
            return Err(LocalControlLimitError::ZeroIncomingMessageBytes);
        }
        if self.max_incoming_message_bytes > MAX_VARINT {
            return Err(LocalControlLimitError::IncomingMessageBytesOutOfRange);
        }

        let local_settings = Settings {
            semantic_role: self.semantic_role,
            max_control_frame_bytes,
            max_negotiation_frame_bytes,
            max_active_incoming_flows: resources.max_active_incoming_flows(),
            max_incoming_message_bytes: self.max_incoming_message_bytes,
        };

        Ok(ValidatedControlProfile {
            resources,
            local_settings,
        })
    }
}

fn usize_to_wire(
    value: usize,
    error: LocalControlLimitError,
) -> Result<u64, LocalControlLimitError> {
    let value = u64::try_from(value).map_err(|_| error)?;
    if value > MAX_VARINT {
        return Err(error);
    }
    Ok(value)
}

impl ValidatedControlProfile {
    pub(super) const fn resources(self) -> ValidatedEndpointResources {
        self.resources
    }

    pub(super) const fn local_settings(self) -> Settings {
        self.local_settings
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) struct Settings {
    pub(super) semantic_role: SemanticRole,
    pub(super) max_control_frame_bytes: u64,
    pub(super) max_negotiation_frame_bytes: u64,
    pub(super) max_active_incoming_flows: u64,
    pub(super) max_incoming_message_bytes: u64,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum SettingsError {
    EmptyBody,
    UnknownSemanticRole(u8),
    VarInt(VarIntDecodeError),
    ZeroControlFrameBytes,
    ControlFrameTooSmall,
    ZeroNegotiationFrameBytes,
    NegotiationFrameTooSmall,
    NegotiationExceedsControl,
    ZeroActiveIncomingFlows,
    ActiveIncomingFlowsExceedQuicStreamLimit,
    ZeroIncomingMessageBytes,
    TrailingBytes,
}

impl From<VarIntDecodeError> for SettingsError {
    fn from(error: VarIntDecodeError) -> Self {
        Self::VarInt(error)
    }
}

impl Settings {
    fn validate(self) -> Result<Self, SettingsError> {
        if self.max_control_frame_bytes == 0 {
            return Err(SettingsError::ZeroControlFrameBytes);
        }
        if self.max_control_frame_bytes < MAX_SETTINGS_BODY_BYTES as u64 {
            return Err(SettingsError::ControlFrameTooSmall);
        }
        if self.max_negotiation_frame_bytes == 0 {
            return Err(SettingsError::ZeroNegotiationFrameBytes);
        }
        if self.max_negotiation_frame_bytes < MIN_NEGOTIATION_OFFER_BODY_BYTES as u64 {
            return Err(SettingsError::NegotiationFrameTooSmall);
        }
        if self.max_negotiation_frame_bytes > self.max_control_frame_bytes {
            return Err(SettingsError::NegotiationExceedsControl);
        }
        if self.max_active_incoming_flows == 0 {
            return Err(SettingsError::ZeroActiveIncomingFlows);
        }
        if self.max_active_incoming_flows > QUIC_MAX_STREAM_COUNT {
            return Err(SettingsError::ActiveIncomingFlowsExceedQuicStreamLimit);
        }
        if self.max_incoming_message_bytes == 0 {
            return Err(SettingsError::ZeroIncomingMessageBytes);
        }
        Ok(self)
    }

    fn encode(self) -> EncodedSettings {
        let mut writer = SettingsWriter::new();
        writer.push_byte(self.semantic_role.wire());
        writer.push_varint(self.max_control_frame_bytes);
        writer.push_varint(self.max_negotiation_frame_bytes);
        writer.push_varint(self.max_active_incoming_flows);
        writer.push_varint(self.max_incoming_message_bytes);
        writer.finish()
    }

    fn decode(input: &[u8]) -> Result<Self, SettingsError> {
        let Some((&role, rest)) = input.split_first() else {
            return Err(SettingsError::EmptyBody);
        };
        let mut reader = SettingsReader::new(rest);
        let settings = Self {
            semantic_role: SemanticRole::from_wire(role)?,
            max_control_frame_bytes: reader.read_varint()?,
            max_negotiation_frame_bytes: reader.read_varint()?,
            max_active_incoming_flows: reader.read_varint()?,
            max_incoming_message_bytes: reader.read_varint()?,
        };
        reader.finish()?;
        settings.validate()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct EncodedSettings {
    bytes: [u8; MAX_SETTINGS_BODY_BYTES],
    len: usize,
}

impl EncodedSettings {
    fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

struct SettingsWriter {
    bytes: [u8; MAX_SETTINGS_BODY_BYTES],
    len: usize,
}

impl SettingsWriter {
    const fn new() -> Self {
        Self {
            bytes: [0; MAX_SETTINGS_BODY_BYTES],
            len: 0,
        }
    }

    fn push_byte(&mut self, value: u8) {
        self.bytes[self.len] = value;
        self.len += 1;
    }

    fn push_varint(&mut self, value: u64) {
        let encoded = encode_varint(value).expect("validated settings values fit QUIC varints");
        let end = self.len + encoded.len();
        debug_assert!(end <= self.bytes.len());
        self.bytes[self.len..end].copy_from_slice(encoded.as_slice());
        self.len = end;
    }

    const fn finish(self) -> EncodedSettings {
        EncodedSettings {
            bytes: self.bytes,
            len: self.len,
        }
    }
}

struct SettingsReader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> SettingsReader<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn read_varint(&mut self) -> Result<u64, SettingsError> {
        let (value, consumed) = decode_varint(&self.input[self.offset..])?;
        self.offset += consumed;
        Ok(value)
    }

    fn finish(self) -> Result<(), SettingsError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(SettingsError::TrailingBytes)
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum ControlFrameType {
    Settings,
    NegotiationOffer,
    NegotiationProposal,
    NegotiationValidated,
    NegotiationEstablished,
    NegotiationFailed,
    OpenFlow,
    FlowAccept,
    FlowReject,
    FlowTerminate,
}

impl ControlFrameType {
    const fn wire(self) -> u64 {
        match self {
            Self::Settings => 0,
            Self::NegotiationOffer => 1,
            Self::NegotiationProposal => 2,
            Self::NegotiationValidated => 3,
            Self::NegotiationEstablished => 4,
            Self::NegotiationFailed => 5,
            Self::OpenFlow => 6,
            Self::FlowAccept => 7,
            Self::FlowReject => 8,
            Self::FlowTerminate => 9,
        }
    }

    fn from_wire(value: u64) -> Result<Self, ControlFrameError> {
        match value {
            0 => Ok(Self::Settings),
            1 => Ok(Self::NegotiationOffer),
            2 => Ok(Self::NegotiationProposal),
            3 => Ok(Self::NegotiationValidated),
            4 => Ok(Self::NegotiationEstablished),
            5 => Ok(Self::NegotiationFailed),
            6 => Ok(Self::OpenFlow),
            7 => Ok(Self::FlowAccept),
            8 => Ok(Self::FlowReject),
            9 => Ok(Self::FlowTerminate),
            value => Err(ControlFrameError::UnknownFrameType(value)),
        }
    }

    const fn is_negotiation(self) -> bool {
        matches!(
            self,
            Self::NegotiationOffer
                | Self::NegotiationProposal
                | Self::NegotiationValidated
                | Self::NegotiationEstablished
                | Self::NegotiationFailed
        )
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ControlFrame {
    pub(super) frame_type: ControlFrameType,
    pub(super) body: Vec<u8>,
}

#[derive(Debug)]
pub(super) enum ControlFrameError {
    VarInt(VarIntDecodeError),
    VarIntEncode(VarIntEncodeError),
    UnknownFrameType(u64),
    BodyLengthOutOfRange,
    BodyTooLarge { received: u64, limit: u64 },
    NegotiationBodyTooLarge { received: u64, limit: u64 },
    Allocation(TryReserveError),
    EndOfStream,
    Read(ReadExactError),
    Write(WriteError),
    ZeroWriteProgress,
}

impl From<VarIntDecodeError> for ControlFrameError {
    fn from(error: VarIntDecodeError) -> Self {
        Self::VarInt(error)
    }
}

impl From<ReadExactError> for ControlFrameError {
    fn from(error: ReadExactError) -> Self {
        Self::Read(error)
    }
}

impl From<WriteError> for ControlFrameError {
    fn from(error: WriteError) -> Self {
        Self::Write(error)
    }
}

#[derive(Debug)]
pub(super) enum ProfileBootstrapError {
    Connection(ConnectionError),
    MissingHandshakeData,
    UnexpectedHandshakeDataType,
    WrongAlpn,
    DatagramUnsupported,
    WrongQuicSide {
        expected: WireSide,
        actual: WireSide,
    },
    ZeroRttControlStream,
    Frame(ControlFrameError),
    Settings(SettingsError),
    UnexpectedFrameBeforeReady(ControlFrameType),
    DuplicateSettings,
    SettingsAfterReady,
    SettingsOwnedByBootstrap,
    ControlChannelPoisoned,
    PeerRoleMismatch {
        expected: SemanticRole,
        received: SemanticRole,
    },
}

impl From<ConnectionError> for ProfileBootstrapError {
    fn from(error: ConnectionError) -> Self {
        Self::Connection(error)
    }
}

impl From<ControlFrameError> for ProfileBootstrapError {
    fn from(error: ControlFrameError) -> Self {
        Self::Frame(error)
    }
}

impl From<SettingsError> for ProfileBootstrapError {
    fn from(error: SettingsError) -> Self {
        Self::Settings(error)
    }
}

#[derive(Debug)]
pub(super) struct ConfirmedProfileTransport {
    connection: Connection,
    side: WireSide,
}

impl ConfirmedProfileTransport {
    pub(super) const fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(super) const fn side(&self) -> WireSide {
        self.side
    }
}

pub(super) fn confirm_profile_transport(
    connection: Connection,
) -> Result<ConfirmedProfileTransport, ProfileBootstrapError> {
    let handshake = connection
        .handshake_data()
        .ok_or(ProfileBootstrapError::MissingHandshakeData)?
        .downcast::<HandshakeData>()
        .map_err(|_| ProfileBootstrapError::UnexpectedHandshakeDataType)?;
    if handshake.protocol.as_deref() != Some(RUNENNET_ALPN) {
        return Err(ProfileBootstrapError::WrongAlpn);
    }
    if connection.max_datagram_size().is_none() {
        return Err(ProfileBootstrapError::DatagramUnsupported);
    }
    let side = match connection.side() {
        Side::Client => WireSide::Client,
        Side::Server => WireSide::Server,
    };
    Ok(ConfirmedProfileTransport { connection, side })
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct BootstrapState {
    local_settings: Settings,
    local_settings_sent: bool,
    peer_settings: Option<Settings>,
}

impl BootstrapState {
    const fn new(local_settings: Settings) -> Self {
        Self {
            local_settings,
            local_settings_sent: false,
            peer_settings: None,
        }
    }

    fn mark_local_settings_sent(&mut self) -> Result<(), ProfileBootstrapError> {
        if self.local_settings_sent {
            return Err(ProfileBootstrapError::DuplicateSettings);
        }
        self.local_settings_sent = true;
        Ok(())
    }

    fn receive_settings(&mut self, settings: Settings) -> Result<(), ProfileBootstrapError> {
        if self.peer_settings.is_some() {
            return Err(ProfileBootstrapError::DuplicateSettings);
        }
        let expected = self.local_settings.semantic_role.opposite();
        if settings.semantic_role != expected {
            return Err(ProfileBootstrapError::PeerRoleMismatch {
                expected,
                received: settings.semantic_role,
            });
        }
        self.peer_settings = Some(settings);
        Ok(())
    }

    const fn is_ready(self) -> bool {
        self.local_settings_sent && self.peer_settings.is_some()
    }

    fn peer_settings(self) -> Settings {
        self.peer_settings
            .expect("ProfileReady requires validated peer SETTINGS")
    }

    fn validate_inbound_type(
        self,
        frame_type: ControlFrameType,
    ) -> Result<(), ProfileBootstrapError> {
        if self.is_ready() {
            if frame_type == ControlFrameType::Settings {
                Err(ProfileBootstrapError::SettingsAfterReady)
            } else {
                Ok(())
            }
        } else if frame_type == ControlFrameType::Settings {
            Ok(())
        } else {
            Err(ProfileBootstrapError::UnexpectedFrameBeforeReady(
                frame_type,
            ))
        }
    }

    fn validate_outbound_type(
        self,
        frame_type: ControlFrameType,
    ) -> Result<(), ProfileBootstrapError> {
        if self.is_ready() {
            if frame_type == ControlFrameType::Settings {
                Err(ProfileBootstrapError::SettingsAfterReady)
            } else {
                Ok(())
            }
        } else if frame_type == ControlFrameType::Settings && !self.local_settings_sent {
            Ok(())
        } else if frame_type == ControlFrameType::Settings {
            Err(ProfileBootstrapError::DuplicateSettings)
        } else {
            Err(ProfileBootstrapError::UnexpectedFrameBeforeReady(
                frame_type,
            ))
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum DirectionIoState {
    Clean,
    InFlightOrPoisoned,
}

impl DirectionIoState {
    const fn new() -> Self {
        Self::Clean
    }

    fn begin(&mut self) -> Result<(), ProfileBootstrapError> {
        if *self != Self::Clean {
            return Err(ProfileBootstrapError::ControlChannelPoisoned);
        }
        *self = Self::InFlightOrPoisoned;
        Ok(())
    }

    fn complete(&mut self) {
        debug_assert_eq!(*self, Self::InFlightOrPoisoned);
        *self = Self::Clean;
    }
}

#[derive(Debug)]
struct BootstrapControl {
    send: SendStream,
    recv: RecvStream,
    state: BootstrapState,
}

impl BootstrapControl {
    fn new(send: SendStream, recv: RecvStream, local_settings: Settings) -> Self {
        Self {
            send,
            recv,
            state: BootstrapState::new(local_settings),
        }
    }

    async fn send_local_settings(&mut self) -> Result<(), ProfileBootstrapError> {
        self.state
            .validate_outbound_type(ControlFrameType::Settings)?;
        let body = self.state.local_settings.encode();
        // Bootstrap owns both streams and is never returned before readiness.
        // Cancellation or any error drops this bootstrap boundary rather than
        // exposing a partially progressed control stream for reuse.
        send_frame_raw(&mut self.send, ControlFrameType::Settings, body.as_slice()).await?;
        self.state.mark_local_settings_sent()?;
        Ok(())
    }

    async fn receive_peer_settings(&mut self) -> Result<(), ProfileBootstrapError> {
        let frame_type = receive_frame_type(&mut self.recv).await?;
        // Bootstrap state is checked before reading body_length or allocating a
        // body, so a non-SETTINGS frame cannot consume the local body budget.
        self.state.validate_inbound_type(frame_type)?;
        let frame = receive_frame_body(
            &mut self.recv,
            frame_type,
            self.state.local_settings.max_control_frame_bytes,
            self.state.local_settings.max_negotiation_frame_bytes,
        )
        .await?;
        let settings = Settings::decode(&frame.body)?;
        self.state.receive_settings(settings)
    }

    fn into_ready_parts(self) -> (Settings, ControlSender, ControlReceiver) {
        debug_assert!(self.state.is_ready());
        let peer_settings = self.state.peer_settings();
        let local_settings = self.state.local_settings;
        (
            peer_settings,
            ControlSender {
                send: self.send,
                local_settings,
                peer_settings,
                io_state: DirectionIoState::new(),
            },
            ControlReceiver {
                recv: self.recv,
                local_settings,
                io_state: DirectionIoState::new(),
            },
        )
    }
}

#[derive(Debug)]
pub(super) struct ControlSender {
    send: SendStream,
    local_settings: Settings,
    peer_settings: Settings,
    io_state: DirectionIoState,
}

impl ControlSender {
    #[cfg(test)]
    pub(super) async fn send_raw_bytes_for_test(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), ControlFrameError> {
        self.send.write_all(bytes).await?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn finish_for_test(&mut self) -> Result<(), ControlFrameError> {
        self.send.finish().map_err(WriteError::from)?;
        match self.send.stopped().await.map_err(WriteError::from)? {
            None => Ok(()),
            Some(error_code) => Err(ControlFrameError::Write(WriteError::Stopped(error_code))),
        }
    }

    pub(super) async fn send_frame(
        &mut self,
        frame_type: ControlFrameType,
        body: &[u8],
    ) -> Result<(), ProfileBootstrapError> {
        if frame_type == ControlFrameType::Settings {
            return Err(ProfileBootstrapError::SettingsOwnedByBootstrap);
        }
        validate_outbound_body(
            self.local_settings,
            self.peer_settings,
            frame_type,
            body.len(),
        )?;
        // Mark only the send direction dirty before its first transport await.
        // A cancelled/failed write poisons this sender without preventing the
        // independent receiver from observing the terminal peer/connection state.
        self.io_state.begin()?;
        send_frame_raw(&mut self.send, frame_type, body).await?;
        self.io_state.complete();
        Ok(())
    }

    pub(super) async fn send_terminal_negotiation_failure(
        &mut self,
        body: &[u8],
    ) -> Result<(), ProfileBootstrapError> {
        self.send_frame(ControlFrameType::NegotiationFailed, body)
            .await?;
        self.send
            .finish()
            .map_err(WriteError::from)
            .map_err(ControlFrameError::from)?;
        match self
            .send
            .stopped()
            .await
            .map_err(WriteError::from)
            .map_err(ControlFrameError::from)?
        {
            None => Ok(()),
            Some(error_code) => {
                Err(ControlFrameError::Write(WriteError::Stopped(error_code)).into())
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct ControlReceiver {
    recv: RecvStream,
    local_settings: Settings,
    io_state: DirectionIoState,
}

impl ControlReceiver {
    pub(super) async fn receive_frame(&mut self) -> Result<ControlFrame, ProfileBootstrapError> {
        // Mark only the receive direction dirty before its first transport await.
        self.io_state.begin()?;
        let frame_type = receive_frame_type(&mut self.recv).await?;
        if frame_type == ControlFrameType::Settings {
            return Err(ProfileBootstrapError::SettingsAfterReady);
        }
        let frame = receive_frame_body(
            &mut self.recv,
            frame_type,
            self.local_settings.max_control_frame_bytes,
            self.local_settings.max_negotiation_frame_bytes,
        )
        .await?;
        self.io_state.complete();
        Ok(frame)
    }
}

fn validate_outbound_body(
    local: Settings,
    peer: Settings,
    frame_type: ControlFrameType,
    body_len: usize,
) -> Result<(), ControlFrameError> {
    let body_len = u64::try_from(body_len).map_err(|_| ControlFrameError::BodyLengthOutOfRange)?;
    let control_limit = local
        .max_control_frame_bytes
        .min(peer.max_control_frame_bytes);
    if body_len > control_limit {
        return Err(ControlFrameError::BodyTooLarge {
            received: body_len,
            limit: control_limit,
        });
    }
    if frame_type.is_negotiation() {
        let negotiation_limit = local
            .max_negotiation_frame_bytes
            .min(peer.max_negotiation_frame_bytes);
        if body_len > negotiation_limit {
            return Err(ControlFrameError::NegotiationBodyTooLarge {
                received: body_len,
                limit: negotiation_limit,
            });
        }
    }
    Ok(())
}

#[derive(Debug)]
pub(super) struct ProfileReadyConnection {
    connection: Connection,
    side: WireSide,
    profile: ValidatedControlProfile,
    peer_settings: Settings,
    sender: ControlSender,
    receiver: ControlReceiver,
}

#[derive(Debug)]
pub(super) struct ProfileReadyParts {
    pub(super) connection: Connection,
    pub(super) side: WireSide,
    pub(super) profile: ValidatedControlProfile,
    pub(super) peer_settings: Settings,
    pub(super) sender: ControlSender,
    pub(super) receiver: ControlReceiver,
}

impl ProfileReadyConnection {
    pub(super) const fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(super) const fn side(&self) -> WireSide {
        self.side
    }

    pub(super) const fn local_profile(&self) -> ValidatedControlProfile {
        self.profile
    }

    pub(super) const fn peer_settings(&self) -> Settings {
        self.peer_settings
    }

    /// Consume the readiness gate and hand RN5E4/RN5E5 independently owned
    /// control directions. This permits a production loop to receive and send
    /// concurrently without cancellation of one direction being required to
    /// make progress on the other.
    pub(super) fn into_parts(self) -> ProfileReadyParts {
        ProfileReadyParts {
            connection: self.connection,
            side: self.side,
            profile: self.profile,
            peer_settings: self.peer_settings,
            sender: self.sender,
            receiver: self.receiver,
        }
    }
}

pub(super) async fn bootstrap_client_control(
    transport: ConfirmedProfileTransport,
    profile: ValidatedControlProfile,
) -> Result<ProfileReadyConnection, ProfileBootstrapError> {
    require_side(transport.side, WireSide::Client)?;
    let (send, recv) = transport.connection.open_bi().await?;
    if recv.is_0rtt() {
        return Err(ProfileBootstrapError::ZeroRttControlStream);
    }
    let mut control = BootstrapControl::new(send, recv, profile.local_settings);
    control.send_local_settings().await?;
    control.receive_peer_settings().await?;
    let (peer_settings, sender, receiver) = control.into_ready_parts();
    Ok(ProfileReadyConnection {
        connection: transport.connection,
        side: transport.side,
        profile,
        peer_settings,
        sender,
        receiver,
    })
}

pub(super) async fn bootstrap_server_control(
    transport: ConfirmedProfileTransport,
    profile: ValidatedControlProfile,
) -> Result<ProfileReadyConnection, ProfileBootstrapError> {
    require_side(transport.side, WireSide::Server)?;
    let (send, recv) = transport.connection.accept_bi().await?;
    if recv.is_0rtt() {
        return Err(ProfileBootstrapError::ZeroRttControlStream);
    }
    let mut control = BootstrapControl::new(send, recv, profile.local_settings);
    control.send_local_settings().await?;
    control.receive_peer_settings().await?;
    let (peer_settings, sender, receiver) = control.into_ready_parts();
    Ok(ProfileReadyConnection {
        connection: transport.connection,
        side: transport.side,
        profile,
        peer_settings,
        sender,
        receiver,
    })
}

fn require_side(actual: WireSide, expected: WireSide) -> Result<(), ProfileBootstrapError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ProfileBootstrapError::WrongQuicSide { expected, actual })
    }
}

async fn send_frame_raw(
    send: &mut SendStream,
    frame_type: ControlFrameType,
    body: &[u8],
) -> Result<(), ControlFrameError> {
    let body_len =
        u64::try_from(body.len()).map_err(|_| ControlFrameError::BodyLengthOutOfRange)?;
    let frame_type = encode_varint(frame_type.wire()).map_err(ControlFrameError::VarIntEncode)?;
    let body_length = encode_varint(body_len).map_err(ControlFrameError::VarIntEncode)?;
    write_slices(send, &[frame_type, body_length], body).await
}

async fn write_slices(
    send: &mut SendStream,
    header: &[EncodedVarInt; 2],
    body: &[u8],
) -> Result<(), ControlFrameError> {
    for part in [header[0].as_slice(), header[1].as_slice(), body] {
        let mut offset = 0;
        while offset < part.len() {
            let written = send.write(&part[offset..]).await?;
            if written == 0 {
                return Err(ControlFrameError::ZeroWriteProgress);
            }
            offset += written;
        }
    }
    Ok(())
}

async fn receive_frame_type(recv: &mut RecvStream) -> Result<ControlFrameType, ControlFrameError> {
    let mut first = [0u8; 1];
    match recv.read_exact(&mut first).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(0)) => return Err(ControlFrameError::EndOfStream),
        Err(error) => return Err(ControlFrameError::Read(error)),
    }
    ControlFrameType::from_wire(read_stream_varint_after_first(recv, first[0]).await?)
}

async fn receive_frame_body(
    recv: &mut RecvStream,
    frame_type: ControlFrameType,
    local_max_control_frame_bytes: u64,
    local_max_negotiation_frame_bytes: u64,
) -> Result<ControlFrame, ControlFrameError> {
    let body_len = read_stream_varint(recv).await?;
    let body_len = validate_inbound_body_length(
        frame_type,
        body_len,
        local_max_control_frame_bytes,
        local_max_negotiation_frame_bytes,
    )?;
    let mut body = Vec::new();
    body.try_reserve_exact(body_len)
        .map_err(ControlFrameError::Allocation)?;
    body.resize(body_len, 0);
    if !body.is_empty() {
        recv.read_exact(&mut body).await?;
    }
    Ok(ControlFrame { frame_type, body })
}

fn validate_inbound_body_length(
    frame_type: ControlFrameType,
    body_len: u64,
    local_max_control_frame_bytes: u64,
    local_max_negotiation_frame_bytes: u64,
) -> Result<usize, ControlFrameError> {
    if body_len > local_max_control_frame_bytes {
        return Err(ControlFrameError::BodyTooLarge {
            received: body_len,
            limit: local_max_control_frame_bytes,
        });
    }
    if frame_type.is_negotiation() && body_len > local_max_negotiation_frame_bytes {
        return Err(ControlFrameError::NegotiationBodyTooLarge {
            received: body_len,
            limit: local_max_negotiation_frame_bytes,
        });
    }
    usize::try_from(body_len).map_err(|_| ControlFrameError::BodyLengthOutOfRange)
}

async fn read_stream_varint(recv: &mut RecvStream) -> Result<u64, ControlFrameError> {
    let mut first = [0u8; 1];
    recv.read_exact(&mut first).await?;
    read_stream_varint_after_first(recv, first[0]).await
}

async fn read_stream_varint_after_first(
    recv: &mut RecvStream,
    first: u8,
) -> Result<u64, ControlFrameError> {
    let mut bytes = [0u8; 8];
    bytes[0] = first;
    let encoded_len = 1usize << (first >> 6);
    if encoded_len > 1 {
        recv.read_exact(&mut bytes[1..encoded_len]).await?;
    }
    let (value, consumed) = decode_varint(&bytes[..encoded_len])?;
    debug_assert_eq!(consumed, encoded_len);
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::EndpointResourceLimits;
    use std::time::Duration;

    fn resources() -> ValidatedEndpointResources {
        EndpointResourceLimits {
            max_connections: 4,
            max_active_incoming_flows: 17,
            udp_payload_ceiling: 1_452,
            stream_receive_window: 64 * 1024,
            connection_receive_window: 256 * 1024,
            send_window: 256 * 1024,
            crypto_buffer_bytes: 32 * 1024,
            datagram_receive_buffer_bytes: 64 * 1024,
            datagram_send_buffer_bytes: 64 * 1024,
            max_idle_timeout: Duration::from_secs(30),
        }
        .validate()
        .unwrap()
    }

    fn local_limits(role: SemanticRole) -> LocalControlLimits {
        LocalControlLimits {
            semantic_role: role,
            max_control_frame_bytes: 64 * 1024,
            max_negotiation_frame_bytes: 32 * 1024,
            max_incoming_message_bytes: 128 * 1024,
        }
    }

    fn settings(role: SemanticRole) -> Settings {
        local_limits(role)
            .validate(resources())
            .unwrap()
            .local_settings()
    }

    #[test]
    fn local_profile_reuses_endpoint_flow_limit() {
        let profile = local_limits(SemanticRole::Authority)
            .validate(resources())
            .unwrap();
        assert_eq!(profile.local_settings().max_active_incoming_flows, 17);
        assert_eq!(profile.resources().max_active_incoming_flows(), 17);
    }

    #[test]
    fn local_profile_rejects_invalid_minimums_and_relationships() {
        let mut limits = local_limits(SemanticRole::Authority);
        limits.max_control_frame_bytes = MAX_SETTINGS_BODY_BYTES - 1;
        assert_eq!(
            limits.validate(resources()),
            Err(LocalControlLimitError::ControlFrameTooSmall)
        );

        let mut limits = local_limits(SemanticRole::Authority);
        limits.max_negotiation_frame_bytes = MIN_NEGOTIATION_OFFER_BODY_BYTES - 1;
        assert_eq!(
            limits.validate(resources()),
            Err(LocalControlLimitError::NegotiationFrameTooSmall)
        );

        let mut limits = local_limits(SemanticRole::Authority);
        limits.max_negotiation_frame_bytes = limits.max_control_frame_bytes + 1;
        assert_eq!(
            limits.validate(resources()),
            Err(LocalControlLimitError::NegotiationExceedsControl)
        );

        let mut limits = local_limits(SemanticRole::Authority);
        limits.max_incoming_message_bytes = 0;
        assert_eq!(
            limits.validate(resources()),
            Err(LocalControlLimitError::ZeroIncomingMessageBytes)
        );

        let mut limits = local_limits(SemanticRole::Authority);
        limits.max_incoming_message_bytes = MAX_VARINT + 1;
        assert_eq!(
            limits.validate(resources()),
            Err(LocalControlLimitError::IncomingMessageBytesOutOfRange)
        );
    }

    #[test]
    fn settings_round_trip_and_preserve_boundary_varints() {
        let original = Settings {
            semantic_role: SemanticRole::Authority,
            max_control_frame_bytes: MAX_VARINT,
            max_negotiation_frame_bytes: 1 << 30,
            max_active_incoming_flows: QUIC_MAX_STREAM_COUNT,
            max_incoming_message_bytes: MAX_VARINT,
        };
        let encoded = original.encode();
        assert_eq!(Settings::decode(encoded.as_slice()), Ok(original));
        assert_eq!(encoded.as_slice()[0], 1);
        assert_eq!(encoded.as_slice().len(), MAX_SETTINGS_BODY_BYTES);
    }

    #[test]
    fn settings_reject_unknown_role_zero_invalid_relationships_and_stream_limit() {
        let valid = settings(SemanticRole::Authority).encode();
        let mut unknown = valid.as_slice().to_vec();
        unknown[0] = 2;
        assert_eq!(
            Settings::decode(&unknown),
            Err(SettingsError::UnknownSemanticRole(2))
        );

        let cases = [
            Settings {
                max_control_frame_bytes: 0,
                ..settings(SemanticRole::Authority)
            },
            Settings {
                max_negotiation_frame_bytes: 0,
                ..settings(SemanticRole::Authority)
            },
            Settings {
                max_active_incoming_flows: 0,
                ..settings(SemanticRole::Authority)
            },
            Settings {
                max_incoming_message_bytes: 0,
                ..settings(SemanticRole::Authority)
            },
        ];
        for case in cases {
            assert!(Settings::decode(case.encode().as_slice()).is_err());
        }

        let case = Settings {
            max_negotiation_frame_bytes: 65_536,
            max_control_frame_bytes: 65_535,
            ..settings(SemanticRole::Authority)
        };
        assert_eq!(
            Settings::decode(case.encode().as_slice()),
            Err(SettingsError::NegotiationExceedsControl)
        );

        let case = Settings {
            max_active_incoming_flows: QUIC_MAX_STREAM_COUNT + 1,
            ..settings(SemanticRole::Authority)
        };
        assert_eq!(
            Settings::decode(case.encode().as_slice()),
            Err(SettingsError::ActiveIncomingFlowsExceedQuicStreamLimit)
        );
    }

    #[test]
    fn settings_reject_trailing_truncated_and_non_minimal_data() {
        let encoded = settings(SemanticRole::Authority).encode();
        let mut trailing = encoded.as_slice().to_vec();
        trailing.push(0);
        assert_eq!(
            Settings::decode(&trailing),
            Err(SettingsError::TrailingBytes)
        );

        let truncated = &encoded.as_slice()[..encoded.as_slice().len() - 1];
        assert!(matches!(
            Settings::decode(truncated),
            Err(SettingsError::VarInt(VarIntDecodeError::Incomplete { .. }))
        ));

        let mut non_minimal = Settings {
            max_control_frame_bytes: 35,
            max_negotiation_frame_bytes: 35,
            ..settings(SemanticRole::Authority)
        }
        .encode()
        .as_slice()
        .to_vec();
        non_minimal.splice(1..2, [0x40, 0x23]);
        assert_eq!(
            Settings::decode(&non_minimal),
            Err(SettingsError::VarInt(VarIntDecodeError::NonMinimal))
        );
    }

    #[test]
    fn bootstrap_state_requires_opposite_role_and_exactly_one_settings() {
        let mut state = BootstrapState::new(settings(SemanticRole::Authority));
        assert!(!state.is_ready());
        state.mark_local_settings_sent().unwrap();
        assert!(matches!(
            state.mark_local_settings_sent(),
            Err(ProfileBootstrapError::DuplicateSettings)
        ));
        assert!(matches!(
            state.receive_settings(settings(SemanticRole::Authority)),
            Err(ProfileBootstrapError::PeerRoleMismatch {
                expected: SemanticRole::NonAuthority,
                received: SemanticRole::Authority,
            })
        ));
        state
            .receive_settings(settings(SemanticRole::NonAuthority))
            .unwrap();
        assert!(state.is_ready());
        assert!(matches!(
            state.receive_settings(settings(SemanticRole::NonAuthority)),
            Err(ProfileBootstrapError::DuplicateSettings)
        ));
    }

    #[test]
    fn bootstrap_state_blocks_non_settings_before_ready_and_settings_after_ready() {
        let mut state = BootstrapState::new(settings(SemanticRole::Authority));
        assert!(matches!(
            state.validate_inbound_type(ControlFrameType::NegotiationOffer),
            Err(ProfileBootstrapError::UnexpectedFrameBeforeReady(
                ControlFrameType::NegotiationOffer
            ))
        ));
        assert!(matches!(
            state.validate_outbound_type(ControlFrameType::OpenFlow),
            Err(ProfileBootstrapError::UnexpectedFrameBeforeReady(
                ControlFrameType::OpenFlow
            ))
        ));
        state.mark_local_settings_sent().unwrap();
        state
            .receive_settings(settings(SemanticRole::NonAuthority))
            .unwrap();
        assert!(
            state
                .validate_inbound_type(ControlFrameType::NegotiationOffer)
                .is_ok()
        );
        assert!(
            state
                .validate_outbound_type(ControlFrameType::FlowAccept)
                .is_ok()
        );
        assert!(matches!(
            state.validate_inbound_type(ControlFrameType::Settings),
            Err(ProfileBootstrapError::SettingsAfterReady)
        ));
        assert!(matches!(
            state.validate_outbound_type(ControlFrameType::Settings),
            Err(ProfileBootstrapError::SettingsAfterReady)
        ));
    }

    #[test]
    fn bootstrap_state_rejects_wrong_frame_type_before_body_processing() {
        let state = BootstrapState::new(settings(SemanticRole::Authority));
        assert!(matches!(
            state.validate_inbound_type(ControlFrameType::OpenFlow),
            Err(ProfileBootstrapError::UnexpectedFrameBeforeReady(
                ControlFrameType::OpenFlow
            ))
        ));

        let mut ready = state;
        ready.mark_local_settings_sent().unwrap();
        ready
            .receive_settings(settings(SemanticRole::NonAuthority))
            .unwrap();
        assert!(matches!(
            ready.validate_inbound_type(ControlFrameType::Settings),
            Err(ProfileBootstrapError::SettingsAfterReady)
        ));
    }

    #[test]
    fn frame_types_cover_exact_revision_one_domain() {
        let all = [
            ControlFrameType::Settings,
            ControlFrameType::NegotiationOffer,
            ControlFrameType::NegotiationProposal,
            ControlFrameType::NegotiationValidated,
            ControlFrameType::NegotiationEstablished,
            ControlFrameType::NegotiationFailed,
            ControlFrameType::OpenFlow,
            ControlFrameType::FlowAccept,
            ControlFrameType::FlowReject,
            ControlFrameType::FlowTerminate,
        ];
        for (value, frame_type) in all.into_iter().enumerate() {
            assert_eq!(
                ControlFrameType::from_wire(value as u64).unwrap(),
                frame_type
            );
        }
        assert!(matches!(
            ControlFrameType::from_wire(10),
            Err(ControlFrameError::UnknownFrameType(10))
        ));
    }

    #[test]
    fn inbound_body_limits_are_checked_before_allocation() {
        assert_eq!(
            validate_inbound_body_length(ControlFrameType::FlowAccept, 80, 80, 40).unwrap(),
            80
        );
        assert!(matches!(
            validate_inbound_body_length(ControlFrameType::FlowAccept, 81, 80, 40),
            Err(ControlFrameError::BodyTooLarge {
                received: 81,
                limit: 80
            })
        ));
        assert_eq!(
            validate_inbound_body_length(ControlFrameType::NegotiationOffer, 40, 80, 40).unwrap(),
            40
        );
        assert!(matches!(
            validate_inbound_body_length(ControlFrameType::NegotiationOffer, 41, 80, 40),
            Err(ControlFrameError::NegotiationBodyTooLarge {
                received: 41,
                limit: 40
            })
        ));
        assert_eq!(
            validate_inbound_body_length(ControlFrameType::NegotiationValidated, 0, 80, 40)
                .unwrap(),
            0
        );
    }

    #[test]
    fn cancelled_direction_is_poisoned_without_poisoning_the_other_direction() {
        let mut sender = DirectionIoState::new();
        let mut receiver = DirectionIoState::new();

        sender.begin().unwrap();
        assert!(matches!(
            sender.begin(),
            Err(ProfileBootstrapError::ControlChannelPoisoned)
        ));

        // Independent ownership is the RN5E4 composability guarantee: a
        // poisoned/cancelled sender does not prevent the receiver from running.
        receiver.begin().unwrap();
        receiver.complete();
        assert_eq!(receiver, DirectionIoState::Clean);
        assert!(matches!(
            sender.begin(),
            Err(ProfileBootstrapError::ControlChannelPoisoned)
        ));
    }

    #[test]
    fn completed_direction_operation_restores_clean_state() {
        let mut io = DirectionIoState::new();
        io.begin().unwrap();
        io.complete();
        assert_eq!(io, DirectionIoState::Clean);
        assert!(io.begin().is_ok());
    }

    #[test]
    fn outbound_body_limits_use_both_local_and_peer_settings() {
        let local = settings(SemanticRole::Authority);
        let mut peer = settings(SemanticRole::NonAuthority);
        peer.max_control_frame_bytes = 80;
        peer.max_negotiation_frame_bytes = 40;

        assert!(validate_outbound_body(local, peer, ControlFrameType::FlowAccept, 80).is_ok());
        assert!(matches!(
            validate_outbound_body(local, peer, ControlFrameType::FlowAccept, 81),
            Err(ControlFrameError::BodyTooLarge {
                received: 81,
                limit: 80
            })
        ));
        assert!(
            validate_outbound_body(local, peer, ControlFrameType::NegotiationOffer, 40).is_ok()
        );
        assert!(matches!(
            validate_outbound_body(local, peer, ControlFrameType::NegotiationOffer, 41),
            Err(ControlFrameError::NegotiationBodyTooLarge {
                received: 41,
                limit: 40
            })
        ));
    }

    #[test]
    fn semantic_role_policy_is_independent_of_quic_side() {
        let authority = local_limits(SemanticRole::Authority)
            .validate(resources())
            .unwrap();
        let non_authority = local_limits(SemanticRole::NonAuthority)
            .validate(resources())
            .unwrap();
        let authority_settings = authority.local_settings();
        let non_authority_settings = non_authority.local_settings();

        assert_eq!(authority_settings.semantic_role, SemanticRole::Authority);
        assert_eq!(
            non_authority_settings.semantic_role,
            SemanticRole::NonAuthority
        );
        assert_eq!(
            authority_settings.max_control_frame_bytes,
            non_authority_settings.max_control_frame_bytes
        );
        assert_eq!(
            authority_settings.max_negotiation_frame_bytes,
            non_authority_settings.max_negotiation_frame_bytes
        );
        assert_eq!(
            authority_settings.max_active_incoming_flows,
            non_authority_settings.max_active_incoming_flows
        );
        assert_eq!(
            authority_settings.max_incoming_message_bytes,
            non_authority_settings.max_incoming_message_bytes
        );
    }
}

from pathlib import Path

path = Path("crates/runen-net-quic/src/control.rs")
text = path.read_text()


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    text = text.replace(old, new, 1)


old_io = '''#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ControlIoState {
    Clean,
    SendInFlightOrPoisoned,
    ReceiveInFlightOrPoisoned,
}

impl ControlIoState {
    const fn new() -> Self {
        Self::Clean
    }

    fn begin_send(&mut self) -> Result<(), ProfileBootstrapError> {
        if *self != Self::Clean {
            return Err(ProfileBootstrapError::ControlChannelPoisoned);
        }
        *self = Self::SendInFlightOrPoisoned;
        Ok(())
    }

    fn complete_send(&mut self) {
        debug_assert_eq!(*self, Self::SendInFlightOrPoisoned);
        *self = Self::Clean;
    }

    fn begin_receive(&mut self) -> Result<(), ProfileBootstrapError> {
        if *self != Self::Clean {
            return Err(ProfileBootstrapError::ControlChannelPoisoned);
        }
        *self = Self::ReceiveInFlightOrPoisoned;
        Ok(())
    }

    fn complete_receive(&mut self) {
        debug_assert_eq!(*self, Self::ReceiveInFlightOrPoisoned);
        *self = Self::Clean;
    }
}
'''
new_io = '''#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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
'''
replace_once(old_io, new_io, "direction io state")

old_channel = '''#[derive(Debug)]
pub(super) struct ControlChannel {
    send: SendStream,
    recv: RecvStream,
    state: BootstrapState,
    io_state: ControlIoState,
}

impl ControlChannel {
    fn new(send: SendStream, recv: RecvStream, local_settings: Settings) -> Self {
        Self {
            send,
            recv,
            state: BootstrapState::new(local_settings),
            io_state: ControlIoState::new(),
        }
    }

    async fn send_local_settings(&mut self) -> Result<(), ProfileBootstrapError> {
        self.state
            .validate_outbound_type(ControlFrameType::Settings)?;
        let body = self.state.local_settings.encode();
        // Mark dirty before the first transport await. Cancellation or any
        // transport/protocol failure leaves the connection-control channel
        // poisoned instead of allowing a resume from an unknown wire offset.
        self.io_state.begin_send()?;
        send_frame_raw(&mut self.send, ControlFrameType::Settings, body.as_slice()).await?;
        self.state.mark_local_settings_sent()?;
        self.io_state.complete_send();
        Ok(())
    }

    async fn receive_peer_settings(&mut self) -> Result<(), ProfileBootstrapError> {
        self.io_state.begin_receive()?;
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
        self.state.receive_settings(settings)?;
        self.io_state.complete_receive();
        Ok(())
    }

    pub(super) async fn send_frame(
        &mut self,
        frame_type: ControlFrameType,
        body: &[u8],
    ) -> Result<(), ProfileBootstrapError> {
        if frame_type == ControlFrameType::Settings {
            return Err(ProfileBootstrapError::SettingsOwnedByBootstrap);
        }
        self.state.validate_outbound_type(frame_type)?;
        let peer = self.state.peer_settings();
        validate_outbound_body(self.state.local_settings, peer, frame_type, body.len())?;
        self.io_state.begin_send()?;
        send_frame_raw(&mut self.send, frame_type, body).await?;
        self.io_state.complete_send();
        Ok(())
    }

    pub(super) async fn receive_frame(&mut self) -> Result<ControlFrame, ProfileBootstrapError> {
        self.io_state.begin_receive()?;
        let frame_type = receive_frame_type(&mut self.recv).await?;
        self.state.validate_inbound_type(frame_type)?;
        let frame = receive_frame_body(
            &mut self.recv,
            frame_type,
            self.state.local_settings.max_control_frame_bytes,
            self.state.local_settings.max_negotiation_frame_bytes,
        )
        .await?;
        self.io_state.complete_receive();
        Ok(frame)
    }

    pub(super) fn peer_settings(&self) -> Settings {
        self.state.peer_settings()
    }
}
'''
new_channel = '''#[derive(Debug)]
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
'''
replace_once(old_channel, new_channel, "bootstrap and ready control split")

old_ready = '''#[derive(Debug)]
pub(super) struct ProfileReadyConnection {
    connection: Connection,
    side: WireSide,
    profile: ValidatedControlProfile,
    control: ControlChannel,
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

    pub(super) fn peer_settings(&self) -> Settings {
        self.control.peer_settings()
    }

    pub(super) fn control_mut(&mut self) -> &mut ControlChannel {
        &mut self.control
    }
}
'''
new_ready = '''#[derive(Debug)]
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
'''
replace_once(old_ready, new_ready, "profile ready split boundary")

old_client = '''    let mut control = ControlChannel::new(send, recv, profile.local_settings);
    control.send_local_settings().await?;
    control.receive_peer_settings().await?;
    debug_assert!(control.state.is_ready());
    Ok(ProfileReadyConnection {
        connection: transport.connection,
        side: transport.side,
        profile,
        control,
    })
'''
new_client = '''    let mut control = BootstrapControl::new(send, recv, profile.local_settings);
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
'''
if text.count(old_client) != 2:
    raise SystemExit(f"bootstrap construction: expected two matches, found {text.count(old_client)}")
text = text.replace(old_client, new_client)

old_tests = '''    #[test]
    fn incomplete_control_operation_poison_is_fail_closed() {
        let mut send = ControlIoState::new();
        send.begin_send().unwrap();
        assert!(matches!(
            send.begin_receive(),
            Err(ProfileBootstrapError::ControlChannelPoisoned)
        ));
        assert!(matches!(
            send.begin_send(),
            Err(ProfileBootstrapError::ControlChannelPoisoned)
        ));

        let mut receive = ControlIoState::new();
        receive.begin_receive().unwrap();
        assert!(matches!(
            receive.begin_send(),
            Err(ProfileBootstrapError::ControlChannelPoisoned)
        ));
        assert!(matches!(
            receive.begin_receive(),
            Err(ProfileBootstrapError::ControlChannelPoisoned)
        ));
    }

    #[test]
    fn completed_control_operations_restore_clean_state() {
        let mut io = ControlIoState::new();
        io.begin_send().unwrap();
        io.complete_send();
        io.begin_receive().unwrap();
        io.complete_receive();
        assert_eq!(io, ControlIoState::Clean);
        assert!(io.begin_send().is_ok());
    }
'''
new_tests = '''    #[test]
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
'''
replace_once(old_tests, new_tests, "direction poison tests")

path.write_text(text)

use std::mem;

use super::wire::{
    EncodedVarInt, MAX_VARINT, VarIntDecodeError, VarIntEncodeError, decode_varint, encode_varint,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum ReliableFrameError {
    InvalidLimit,
    LengthOutOfProfileRange,
    LengthDoesNotFitPlatform,
    MessageTooLarge { claimed: u64, max: u64 },
    StagingLimitExceeded { claimed: u64, max: u64 },
    AllocationFailed,
    VarInt(VarIntDecodeError),
    TruncatedFrame,
}

impl From<VarIntDecodeError> for ReliableFrameError {
    fn from(error: VarIntDecodeError) -> Self {
        Self::VarInt(error)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FrameConsume {
    pub(crate) consumed: usize,
    pub(crate) message: Option<Vec<u8>>,
}

#[derive(Debug)]
enum FrameState {
    Length {
        bytes: [u8; 8],
        filled: usize,
        needed: usize,
    },
    Payload {
        expected: usize,
        bytes: Vec<u8>,
    },
}

impl FrameState {
    const fn length() -> Self {
        Self::Length {
            bytes: [0; 8],
            filled: 0,
            needed: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReliableFrameDecoder {
    max_message_bytes: u64,
    max_staging_bytes: u64,
    state: FrameState,
}

impl ReliableFrameDecoder {
    pub(crate) fn new(
        max_message_bytes: usize,
        max_staging_bytes: usize,
    ) -> Result<Self, ReliableFrameError> {
        if max_message_bytes == 0 || max_staging_bytes == 0 {
            return Err(ReliableFrameError::InvalidLimit);
        }
        let max_message_bytes = u64::try_from(max_message_bytes)
            .map_err(|_| ReliableFrameError::LengthOutOfProfileRange)?;
        let max_staging_bytes = u64::try_from(max_staging_bytes)
            .map_err(|_| ReliableFrameError::LengthOutOfProfileRange)?;
        if max_message_bytes > MAX_VARINT || max_staging_bytes > MAX_VARINT {
            return Err(ReliableFrameError::LengthOutOfProfileRange);
        }

        Ok(Self {
            max_message_bytes,
            max_staging_bytes,
            state: FrameState::length(),
        })
    }

    pub(crate) fn consume(&mut self, input: &[u8]) -> Result<FrameConsume, ReliableFrameError> {
        let mut consumed = 0usize;

        loop {
            if matches!(&self.state, FrameState::Length { .. }) {
                let prefix_complete = {
                    let FrameState::Length {
                        bytes,
                        filled,
                        needed,
                    } = &mut self.state
                    else {
                        unreachable!("state checked immediately above");
                    };

                    if *filled == 0 {
                        let Some(&first) = input.get(consumed) else {
                            return Ok(FrameConsume {
                                consumed,
                                message: None,
                            });
                        };
                        *needed = 1usize << (first >> 6);
                    }

                    let available = input.len() - consumed;
                    let remaining = *needed - *filled;
                    let take = available.min(remaining);
                    if take > 0 {
                        bytes[*filled..*filled + take]
                            .copy_from_slice(&input[consumed..consumed + take]);
                        *filled += take;
                        consumed += take;
                    }

                    if *filled == *needed {
                        Some((*bytes, *needed))
                    } else {
                        None
                    }
                };

                let Some((prefix, prefix_len)) = prefix_complete else {
                    return Ok(FrameConsume {
                        consumed,
                        message: None,
                    });
                };

                let (claimed, decoded_len) = decode_varint(&prefix[..prefix_len])?;
                debug_assert_eq!(decoded_len, prefix_len);
                if claimed > self.max_message_bytes {
                    return Err(ReliableFrameError::MessageTooLarge {
                        claimed,
                        max: self.max_message_bytes,
                    });
                }
                if claimed > self.max_staging_bytes {
                    return Err(ReliableFrameError::StagingLimitExceeded {
                        claimed,
                        max: self.max_staging_bytes,
                    });
                }

                let expected = usize::try_from(claimed)
                    .map_err(|_| ReliableFrameError::LengthDoesNotFitPlatform)?;
                if expected == 0 {
                    self.state = FrameState::length();
                    return Ok(FrameConsume {
                        consumed,
                        message: Some(Vec::new()),
                    });
                }

                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(expected)
                    .map_err(|_| ReliableFrameError::AllocationFailed)?;
                self.state = FrameState::Payload { expected, bytes };
                continue;
            }

            let completed = {
                let FrameState::Payload { expected, bytes } = &mut self.state else {
                    unreachable!("length state handled above");
                };
                let remaining = *expected - bytes.len();
                let available = input.len() - consumed;
                let take = available.min(remaining);
                if take > 0 {
                    bytes.extend_from_slice(&input[consumed..consumed + take]);
                    consumed += take;
                }
                (bytes.len() == *expected).then(|| mem::take(bytes))
            };

            if let Some(message) = completed {
                self.state = FrameState::length();
                return Ok(FrameConsume {
                    consumed,
                    message: Some(message),
                });
            }

            return Ok(FrameConsume {
                consumed,
                message: None,
            });
        }
    }

    pub(crate) fn finish(&self) -> Result<(), ReliableFrameError> {
        match &self.state {
            FrameState::Length { filled: 0, .. } => Ok(()),
            FrameState::Length { .. } | FrameState::Payload { .. } => {
                Err(ReliableFrameError::TruncatedFrame)
            }
        }
    }

    #[cfg(test)]
    fn has_payload_allocation(&self) -> bool {
        matches!(&self.state, FrameState::Payload { .. })
    }
}

pub(crate) fn encode_payload_length(
    payload_len: usize,
    max_message_bytes: usize,
) -> Result<EncodedVarInt, ReliableFrameError> {
    if max_message_bytes == 0 {
        return Err(ReliableFrameError::InvalidLimit);
    }

    let payload_len =
        u64::try_from(payload_len).map_err(|_| ReliableFrameError::LengthOutOfProfileRange)?;
    let max_message_bytes = u64::try_from(max_message_bytes)
        .map_err(|_| ReliableFrameError::LengthOutOfProfileRange)?;
    if payload_len > MAX_VARINT || max_message_bytes > MAX_VARINT {
        return Err(ReliableFrameError::LengthOutOfProfileRange);
    }
    if payload_len > max_message_bytes {
        return Err(ReliableFrameError::MessageTooLarge {
            claimed: payload_len,
            max: max_message_bytes,
        });
    }

    encode_varint(payload_len).map_err(|error| match error {
        VarIntEncodeError::OutOfRange => ReliableFrameError::LengthOutOfProfileRange,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::encode_varint;

    fn frame(payload: &[u8]) -> Vec<u8> {
        let profile_max = usize::try_from(MAX_VARINT).unwrap_or(usize::MAX);
        let mut bytes = encode_payload_length(payload.len(), profile_max)
            .unwrap()
            .as_slice()
            .to_vec();
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn encoder_produces_header_without_copying_payload() {
        let header = encode_payload_length(64, 128).unwrap();
        assert_eq!(header.as_slice(), &[0x40, 0x40]);
        assert_eq!(
            encode_payload_length(129, 128),
            Err(ReliableFrameError::MessageTooLarge {
                claimed: 129,
                max: 128,
            })
        );
    }

    #[test]
    fn decoder_handles_payload_split_across_arbitrary_chunks() {
        let payload: Vec<u8> = (0..100).collect();
        let encoded = frame(&payload);
        let mut decoder = ReliableFrameDecoder::new(128, 128).unwrap();
        let mut offset = 0usize;
        let mut message = None;

        while offset < encoded.len() {
            let end = (offset + 3).min(encoded.len());
            let result = decoder.consume(&encoded[offset..end]).unwrap();
            assert!(result.consumed > 0);
            offset += result.consumed;
            if result.message.is_some() {
                message = result.message;
            }
        }

        assert_eq!(message, Some(payload));
        assert_eq!(decoder.finish(), Ok(()));
    }

    #[test]
    fn decoder_handles_multiple_frames_in_one_input_without_overconsuming() {
        let first = frame(b"one");
        let second = frame(b"two");
        let mut combined = first.clone();
        combined.extend_from_slice(&second);

        let mut decoder = ReliableFrameDecoder::new(16, 16).unwrap();
        let first_result = decoder.consume(&combined).unwrap();
        assert_eq!(first_result.message, Some(b"one".to_vec()));
        assert_eq!(first_result.consumed, first.len());

        let second_result = decoder.consume(&combined[first_result.consumed..]).unwrap();
        assert_eq!(second_result.message, Some(b"two".to_vec()));
        assert_eq!(second_result.consumed, second.len());
        assert_eq!(decoder.finish(), Ok(()));
    }

    #[test]
    fn decoder_accepts_zero_length_message() {
        let mut decoder = ReliableFrameDecoder::new(16, 16).unwrap();
        let result = decoder.consume(&[0]).unwrap();
        assert_eq!(result.consumed, 1);
        assert_eq!(result.message, Some(Vec::new()));
        assert_eq!(decoder.finish(), Ok(()));
    }

    #[test]
    fn decoder_handles_every_split_of_an_eight_octet_prefix_without_allocating_early() {
        let prefix = encode_varint(1 << 30).unwrap();
        assert_eq!(prefix.len(), 8);

        for split in 1..8 {
            let mut decoder = ReliableFrameDecoder::new(1024, 1024).unwrap();
            let first = decoder.consume(&prefix.as_slice()[..split]).unwrap();
            assert_eq!(first.consumed, split);
            assert_eq!(first.message, None);
            assert!(!decoder.has_payload_allocation());

            assert_eq!(
                decoder.consume(&prefix.as_slice()[split..]),
                Err(ReliableFrameError::MessageTooLarge {
                    claimed: 1 << 30,
                    max: 1024,
                })
            );
            assert!(!decoder.has_payload_allocation());
        }
    }

    #[test]
    fn decoder_rejects_non_minimal_length_before_payload_allocation() {
        let mut decoder = ReliableFrameDecoder::new(16, 16).unwrap();
        assert_eq!(
            decoder.consume(&[0x40, 0x01]),
            Err(ReliableFrameError::VarInt(VarIntDecodeError::NonMinimal))
        );
        assert!(!decoder.has_payload_allocation());
    }

    #[test]
    fn decoder_checks_flow_and_staging_limits_before_payload_allocation() {
        let prefix = encode_varint(9).unwrap();

        let mut flow_limited = ReliableFrameDecoder::new(8, 16).unwrap();
        assert_eq!(
            flow_limited.consume(prefix.as_slice()),
            Err(ReliableFrameError::MessageTooLarge { claimed: 9, max: 8 })
        );
        assert!(!flow_limited.has_payload_allocation());

        let mut staging_limited = ReliableFrameDecoder::new(16, 8).unwrap();
        assert_eq!(
            staging_limited.consume(prefix.as_slice()),
            Err(ReliableFrameError::StagingLimitExceeded { claimed: 9, max: 8 })
        );
        assert!(!staging_limited.has_payload_allocation());
    }

    #[test]
    fn finish_distinguishes_clean_boundary_from_truncation() {
        let mut clean = ReliableFrameDecoder::new(16, 16).unwrap();
        assert_eq!(clean.finish(), Ok(()));
        assert_eq!(
            clean.consume(&frame(b"ok")).unwrap().message,
            Some(b"ok".to_vec())
        );
        assert_eq!(clean.finish(), Ok(()));

        let mut truncated_prefix = ReliableFrameDecoder::new(16_384, 16_384).unwrap();
        assert_eq!(truncated_prefix.consume(&[0x40]).unwrap().message, None);
        assert_eq!(
            truncated_prefix.finish(),
            Err(ReliableFrameError::TruncatedFrame)
        );

        let mut truncated_payload = ReliableFrameDecoder::new(16, 16).unwrap();
        assert_eq!(truncated_payload.consume(&[3, b'a']).unwrap().message, None);
        assert_eq!(
            truncated_payload.finish(),
            Err(ReliableFrameError::TruncatedFrame)
        );
    }
}

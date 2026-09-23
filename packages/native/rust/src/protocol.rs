//! The JSON the bridge exchanges with its host: call requests and responses,
//! and the headers of transport events.

mod event;
mod request;
mod stats;

pub(crate) use event::{Event, LocalCandidate, connection_state};
pub(crate) use request::{
    BitrateRequest, Direction, DirectionRequest, Mapping, PrepareRequest, PrepareResponse,
    TrackKind,
};
pub(crate) use stats::{MediaSnapshot, stats_json};

use crate::error::{BridgeError, FailureClass};
use serde::de::DeserializeOwned;
use serde::{Serialize, Serializer};

/// A `u64` for the JavaScript host, which reads JSON numbers as doubles and
/// would round counters past 2^53, so it travels as a decimal string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecimalU64(pub(crate) u64);

impl Serialize for DecimalU64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

/// Decode a JSON request. Any failure is invalid input.
pub(crate) fn decode<T: DeserializeOwned>(request: &[u8]) -> Result<T, BridgeError> {
    serde_json::from_slice(request)
        .map_err(|error| BridgeError::invalid(format!("invalid JSON: {error}")))
}

/// Encode a JSON response or event header.
pub(crate) fn encode(value: &impl Serialize) -> Result<Vec<u8>, BridgeError> {
    serde_json::to_vec(value)
        .map_err(|error| BridgeError::new(FailureClass::Native, format!("serialize JSON: {error}")))
}

/// The response of a call that returns nothing.
pub(crate) fn empty_response() -> Vec<u8> {
    b"{}".to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn counters_beyond_double_precision_travel_as_exact_decimal_strings() {
        let counters = [
            DecimalU64(0),
            DecimalU64(9_007_199_254_740_993),
            DecimalU64(u64::MAX),
        ];
        assert_eq!(
            serde_json::to_value(counters).unwrap(),
            json!(["0", "9007199254740993", "18446744073709551615"])
        );
    }

    #[test]
    fn a_malformed_request_is_invalid_input() {
        let error = decode::<Vec<u8>>(b"{").unwrap_err();
        assert_eq!(error.class, FailureClass::InvalidInput);
        assert!(error.message.starts_with("invalid JSON: "), "{error}");
    }
}

//! Bridge failures and the ABI failure class each one reports.

use crate::abi::Status;
use std::fmt;

/// The failure class of a [`BridgeError`], which fixes the status the C ABI
/// reports for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureClass {
    Closed,
    InvalidInput,
    Native,
    Overflow,
    Protocol,
    SdpRejected,
    ChannelClosed,
}

impl FailureClass {
    /// The status an entry point returns for this class.
    pub(crate) const fn status(self) -> Status {
        match self {
            Self::Closed => Status::Closed,
            Self::InvalidInput => Status::InvalidInput,
            Self::Native => Status::Native,
            Self::Overflow => Status::Overflow,
            Self::Protocol => Status::Protocol,
            Self::SdpRejected => Status::SdpRejected,
            Self::ChannelClosed => Status::ChannelClosed,
        }
    }
}

/// A classified failure. Its message is diagnostic text for the host, which
/// never matches on it: the class is the failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BridgeError {
    pub(crate) class: FailureClass,
    pub(crate) message: String,
}

impl BridgeError {
    pub(crate) fn new(class: FailureClass, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(FailureClass::InvalidInput, message)
    }

    pub(crate) fn overflow(message: impl Into<String>) -> Self {
        Self::new(FailureClass::Overflow, message)
    }

    pub(crate) fn closed() -> Self {
        Self::new(FailureClass::Closed, "native peer is closed")
    }
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for BridgeError {}

/// Classifies reactor-webrtc failures.
///
/// reactor-webrtc reports every libwebrtc failure as an untyped string, so the
/// class comes from the operation that failed rather than from the error.
pub(crate) trait Classify<T> {
    /// Report a failure of `operation` as `class`, keeping libwebrtc's text.
    fn classify(self, class: FailureClass, operation: &str) -> Result<T, BridgeError>;
}

impl<T> Classify<T> for reactor_webrtc::Result<T> {
    fn classify(self, class: FailureClass, operation: &str) -> Result<T, BridgeError> {
        self.map_err(|error| BridgeError::new(class, format!("{operation}: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_classified_failure_names_its_operation_and_keeps_the_libwebrtc_text() {
        let failed: reactor_webrtc::Result<()> =
            Err(reactor_webrtc::Error::Webrtc("bad fingerprint".into()));
        let error = failed
            .classify(FailureClass::SdpRejected, "set_remote_description")
            .unwrap_err();
        assert_eq!(error.class, FailureClass::SdpRejected);
        assert_eq!(
            error.message,
            "set_remote_description: webrtc error: bad fingerprint"
        );
    }

    #[test]
    fn every_failure_class_reports_a_failure_status_except_closed() {
        let classes = [
            FailureClass::InvalidInput,
            FailureClass::Native,
            FailureClass::Overflow,
            FailureClass::Protocol,
            FailureClass::SdpRejected,
            FailureClass::ChannelClosed,
        ];
        for class in classes {
            assert!(class.status().code() < 0, "{class:?}");
        }
        // Closed is an outcome the host expects, not a failure of the call.
        assert_eq!(FailureClass::Closed.status(), Status::Closed);
    }
}

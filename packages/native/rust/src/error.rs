//! Bridge failures and the failure class each one reports.

use std::fmt;
use std::sync::mpsc::{RecvError, SendError};

/// The failure class of a [`BridgeError`]. The host maps each class to its own
/// error reason, and never reads the message to decide what failed.
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

/// A peer's channels to its owner thread disconnect only once that thread has
/// stopped, so a failed send or receive means the peer has closed.
impl<T> From<SendError<T>> for BridgeError {
    fn from(_: SendError<T>) -> Self {
        Self::closed()
    }
}

impl From<RecvError> for BridgeError {
    fn from(_: RecvError) -> Self {
        Self::closed()
    }
}

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
    use std::sync::mpsc;

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
    fn a_disconnected_owner_channel_reads_as_a_closed_peer() {
        let (sender, receiver) = mpsc::channel::<()>();
        drop(receiver);
        let unsent = sender.send(()).unwrap_err();
        assert_eq!(BridgeError::from(unsent), BridgeError::closed());

        let (sender, receiver) = mpsc::channel::<()>();
        drop(sender);
        let unreceived = receiver.recv().unwrap_err();
        assert_eq!(BridgeError::from(unreceived), BridgeError::closed());
    }
}

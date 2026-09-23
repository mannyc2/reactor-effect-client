//! Candidates in an answer: the bridge's C ABI has no call for remote
//! candidates, so the far peer's answer carries them, as Reactor's does.

use reactor_webrtc::IceCandidate;

/// Add each candidate to the m-section its `sdp_mline_index` names, then end
/// every m-section's candidates. A candidate without an index, or with one
/// past the last m-section, is left out.
pub(crate) fn with_candidates(sdp: &str, candidates: &[IceCandidate]) -> String {
    // The session section, then one per m-line.
    let mut sections: Vec<Vec<String>> = vec![Vec::new()];
    for line in sdp.split("\r\n").filter(|line| !line.is_empty()) {
        if line.starts_with("m=") {
            sections.push(Vec::new());
        }
        sections
            .last_mut()
            .expect("there is always a session section")
            .push(line.to_owned());
    }
    for candidate in candidates {
        let section = candidate
            .sdp_mline_index
            .and_then(|index| sections.get_mut(usize::from(index) + 1));
        if let Some(section) = section {
            let attribute = candidate.candidate.trim_start_matches("a=");
            section.push(format!("a={attribute}"));
        }
    }
    for section in sections.iter_mut().skip(1) {
        section.push("a=end-of-candidates".to_owned());
    }
    let mut sdp = sections.concat().join("\r\n");
    sdp.push_str("\r\n");
    sdp
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(line: &str, index: Option<u16>) -> IceCandidate {
        IceCandidate {
            candidate: line.to_owned(),
            sdp_mid: None,
            sdp_mline_index: index,
        }
    }

    const ANSWER: &str = "v=0\r\ns=-\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:1\r\n";

    #[test]
    fn each_candidate_joins_its_m_section_and_every_section_ends_its_candidates() {
        let sdp = with_candidates(
            ANSWER,
            &[
                candidate("candidate:1 1 udp 1 127.0.0.1 1000 typ host", Some(1)),
                candidate("a=candidate:2 1 udp 1 127.0.0.1 2000 typ host", Some(0)),
            ],
        );
        assert_eq!(
            sdp,
            "v=0\r\ns=-\r\n\
             m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\n\
             a=candidate:2 1 udp 1 127.0.0.1 2000 typ host\r\na=end-of-candidates\r\n\
             m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:1\r\n\
             a=candidate:1 1 udp 1 127.0.0.1 1000 typ host\r\na=end-of-candidates\r\n"
        );
    }

    #[test]
    fn a_candidate_without_a_matching_m_section_is_left_out() {
        let sdp = with_candidates(
            ANSWER,
            &[
                candidate("candidate:3", None),
                candidate("candidate:4", Some(2)),
            ],
        );
        assert!(!sdp.contains("candidate:3") && !sdp.contains("candidate:4"));
        assert_eq!(sdp.matches("a=end-of-candidates").count(), 2);
    }
}

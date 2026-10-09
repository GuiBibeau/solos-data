//! A line-based server-sent event reader (the WHATWG `text/event-stream` rules the lane needs):
//! bytes arrive in arbitrary chunks, lines end in LF, CRLF or a lone CR (even when the CR and
//! LF land in different chunks), comment lines (`: keep-alive`) are dropped, `data:` lines of
//! one event are joined with newlines, a blank line dispatches the event, and a partial line or
//! event waits for the next chunk. Lines are decoded only once complete, so a UTF-8 sequence
//! split across chunks stays intact.

/// One server-sent event.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SseFrame {
    /// `id:` line.
    pub id: Option<String>,
    /// `event:` line.
    pub event: Option<String>,
    /// `data:` lines joined by newlines.
    pub data: String,
}

/// The reader's state between chunks.
#[derive(Debug, Default)]
pub struct SseParser {
    line: Vec<u8>,
    after_cr: bool,
    frame: SseFrame,
    data: Vec<String>,
    started: bool,
    /// Comment lines seen (keep-alives).
    pub comments: u64,
}

impl SseParser {
    /// A reader at the start of a stream.
    #[must_use]
    pub fn new() -> SseParser {
        SseParser::default()
    }

    /// Feed one chunk; return the events it completed.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<SseFrame> {
        let mut frames = Vec::new();
        for &byte in chunk {
            if self.after_cr {
                self.after_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\n' | b'\r' => {
                    self.after_cr = byte == b'\r';
                    let line = std::mem::take(&mut self.line);
                    if let Some(frame) = self.line_done(&line) {
                        frames.push(frame);
                    }
                }
                _ => self.line.push(byte),
            }
        }
        frames
    }

    /// Whether a partial line or event is pending (the stream stopped mid-event).
    #[must_use]
    pub fn pending(&self) -> bool {
        !self.line.is_empty() || self.started
    }

    fn line_done(&mut self, line: &[u8]) -> Option<SseFrame> {
        if line.is_empty() {
            return self.dispatch();
        }
        let text = String::from_utf8_lossy(line);
        if text.starts_with(':') {
            self.comments += 1;
            return None;
        }
        let (field, value) = match text.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (text.as_ref(), ""),
        };
        self.started = true;
        match field {
            "id" if !value.contains('\0') => self.frame.id = Some(value.to_owned()),
            "event" => self.frame.event = Some(value.to_owned()),
            "data" => self.data.push(value.to_owned()),
            _ => {}
        }
        None
    }

    fn dispatch(&mut self) -> Option<SseFrame> {
        if !self.started {
            return None;
        }
        self.started = false;
        let mut frame = std::mem::take(&mut self.frame);
        frame.data = std::mem::take(&mut self.data).join("\n");
        (frame.id.is_some() || frame.event.is_some() || !frame.data.is_empty()).then_some(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(parser: &mut SseParser, chunks: &[&[u8]]) -> Vec<SseFrame> {
        chunks.iter().flat_map(|c| parser.push(c)).collect()
    }

    #[test]
    fn comments_line_endings_and_multi_line_data() {
        let mut parser = SseParser::new();
        let frames = feed(
            &mut parser,
            &[
                b": keep-alive\n\n: keep-alive\r\n\r\n",
                b"id: e1\r\nevent: notif",
                b"ication\r\ndata: {\"queryId\":\r",
                b"\ndata: \"q\"}\r\n\r",
                b"\nevent: end\rdata\r\r",
                b"data:{\"code\":\"USER_STREAM_CLOSED\"}\n\nid: partial",
            ],
        );
        assert_eq!(parser.comments, 2);
        assert_eq!(frames.len(), 3, "{frames:?}");
        assert_eq!(frames[0].id.as_deref(), Some("e1"));
        assert_eq!(frames[0].event.as_deref(), Some("notification"));
        assert_eq!(frames[0].data, "{\"queryId\":\n\"q\"}");
        let value: serde_json::Value = serde_json::from_str(&frames[0].data).unwrap();
        assert_eq!(value["queryId"], "q");
        assert_eq!(frames[1].event.as_deref(), Some("end"));
        assert_eq!(
            frames[1].data, "",
            "a bare `data` line is an empty data line"
        );
        assert_eq!(frames[2].data, "{\"code\":\"USER_STREAM_CLOSED\"}");
        assert!(parser.pending(), "the partial `id` line waits");
    }

    #[test]
    fn utf8_split_across_chunks_and_byte_at_a_time() {
        let text = "id: é\ndata: café ☕\n\n".as_bytes();
        let mut parser = SseParser::new();
        let frames: Vec<SseFrame> = text.iter().flat_map(|b| parser.push(&[*b])).collect();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].id.as_deref(), Some("é"));
        assert_eq!(frames[0].data, "café ☕");
        assert!(!parser.pending());
    }
}

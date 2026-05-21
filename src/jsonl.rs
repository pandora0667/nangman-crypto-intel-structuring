use crate::error::AppResult;
use crate::hash::sha256_prefixed;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonlRecordLocator {
    pub line_number: usize,
    pub byte_offset: usize,
    pub byte_length: usize,
    pub content_sha256: String,
}

pub fn build_jsonl_chunk<T: Serialize>(
    records: &[T],
) -> AppResult<(Vec<u8>, Vec<JsonlRecordLocator>)> {
    let mut bytes = Vec::new();
    let mut locators = Vec::with_capacity(records.len());
    for (index, record) in records.iter().enumerate() {
        let line = serde_json::to_vec(record)?;
        let byte_offset = bytes.len();
        let byte_length = line.len();
        let content_sha256 = sha256_prefixed(&line);
        bytes.extend_from_slice(&line);
        bytes.push(b'\n');
        locators.push(JsonlRecordLocator {
            line_number: index + 1,
            byte_offset,
            byte_length,
            content_sha256,
        });
    }
    Ok((bytes, locators))
}

pub fn extract_jsonl_line(bytes: &[u8], line_number: usize) -> AppResult<&[u8]> {
    let mut current_line = 1usize;
    let mut start = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            if current_line == line_number {
                return Ok(&bytes[start..index]);
            }
            current_line += 1;
            start = index + 1;
        }
    }
    if current_line == line_number && start < bytes.len() {
        return Ok(&bytes[start..]);
    }
    Err(crate::error::AppError::validation(format!(
        "jsonl line {line_number} not found"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Row {
        id: &'static str,
    }

    #[test]
    fn jsonl_chunk_keeps_byte_locators() {
        let (bytes, locators) = build_jsonl_chunk(&[Row { id: "a" }, Row { id: "b" }]).unwrap();
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            "{\"id\":\"a\"}\n{\"id\":\"b\"}\n"
        );
        assert_eq!(locators[1].byte_offset, "{\"id\":\"a\"}\n".len());
    }

    #[test]
    fn extracts_requested_jsonl_line() {
        let bytes = b"{\"id\":\"a\"}\n{\"id\":\"b\"}\n";
        assert_eq!(extract_jsonl_line(bytes, 2).unwrap(), b"{\"id\":\"b\"}");
    }
}

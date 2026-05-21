use crate::error::{AppError, AppResult};
use crate::models::constants::FORBIDDEN_OUTPUT_TERMS;
use serde::Serialize;

pub fn validate_no_forbidden_output<T: Serialize>(value: &T) -> AppResult<()> {
    let json = serde_json::to_value(value)?;
    scan_value(&json, "$")
}

pub fn redact_forbidden_output_terms(text: &str) -> String {
    let mut redacted = String::with_capacity(text.len());
    let mut token = String::new();
    let mut token_mode: Option<bool> = None;

    for ch in text.chars() {
        let is_token_char = ch.is_ascii_alphanumeric() || ch == '_';
        match token_mode {
            Some(current_mode) if current_mode == is_token_char => {
                token.push(ch);
            }
            Some(current_mode) => {
                flush_token(&mut redacted, &token, current_mode);
                token.clear();
                token.push(ch);
                token_mode = Some(is_token_char);
            }
            None => {
                token.push(ch);
                token_mode = Some(is_token_char);
            }
        }
    }

    if let Some(current_mode) = token_mode {
        flush_token(&mut redacted, &token, current_mode);
    }

    redacted
}

fn scan_value(value: &serde_json::Value, path: &str) -> AppResult<()> {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                let lowered_key = key.to_ascii_lowercase();
                if FORBIDDEN_OUTPUT_TERMS.contains(&lowered_key.as_str()) {
                    return Err(AppError::validation(format!(
                        "forbidden output key at {path}.{key}"
                    )));
                }
                scan_value(value, &format!("{path}.{key}"))?;
            }
        }
        serde_json::Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                scan_value(item, &format!("{path}[{index}]"))?;
            }
        }
        serde_json::Value::String(text) => {
            let lowered = text.to_ascii_lowercase();
            for term in FORBIDDEN_OUTPUT_TERMS {
                if lowered
                    .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                    .any(|part| part == *term)
                {
                    return Err(AppError::validation(format!(
                        "forbidden output term '{term}' at {path}"
                    )));
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn flush_token(output: &mut String, token: &str, is_token_char: bool) {
    if is_token_char
        && FORBIDDEN_OUTPUT_TERMS
            .iter()
            .any(|term| token.eq_ignore_ascii_case(term))
    {
        output.push_str("[blocked_output_term]");
    } else {
        output.push_str(token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn blocks_forbidden_terms() {
        assert!(validate_no_forbidden_output(&json!({"summary":"buy now"})).is_err());
        assert!(validate_no_forbidden_output(&json!({"summary":"observe only"})).is_ok());
    }

    #[test]
    fn redacts_forbidden_terms_from_error_text() {
        let reason = redact_forbidden_output_terms(
            "validation error: forbidden output term 'short' at $.reason",
        );

        assert!(!reason.contains("short"));
        assert!(
            validate_no_forbidden_output(&json!({
                "reason": reason
            }))
            .is_ok()
        );
    }
}

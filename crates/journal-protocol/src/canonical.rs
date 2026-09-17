use thiserror::Error;

use crate::domain;

#[derive(Debug, Error)]
pub enum CanonicalizationError {
    #[error(transparent)]
    Validation(#[from] domain::ValidationError),
    #[error("canonical append encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
}

/// Return the sole byte representation used for append idempotency equality.
///
/// Validation happens first. `attention` is the only set-valued append field,
/// so its order is normalized. Relation order and every string remain exact.
pub fn canonical_append(request: &domain::RecordInput) -> Result<Vec<u8>, CanonicalizationError> {
    request.validate()?;

    let mut canonical = request.clone();
    canonical.attention.sort_unstable();
    Ok(serde_json::to_vec(&canonical)?)
}

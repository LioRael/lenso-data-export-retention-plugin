//! Authoritative source for one Retention Participant.

use lenso_contract_authoring as lenso;

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyRetentionRequestMode {
    Delete,
    Anonymize,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ApplyRetentionRequest {
    pub action_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub subject: String,
    pub mode: ApplyRetentionRequestMode,
    pub reason: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ApplyRetentionResponse {
    pub receipt: String,
}

#[derive(lenso::DomainError)]
pub enum ApplyRetentionError {
    InvalidRequest,
    Forbidden,
    UnsupportedMode,
}

#[lenso::capability(
    id = "lenso.retention-participant",
    major = 1,
    version = "1.0.0",
    portable = true,
    cross_lane_transfer = true
)]
pub trait RetentionParticipant {
    async fn apply_retention(
        &self,
        context: lenso::Ctx<'_>,
        request: ApplyRetentionRequest,
    ) -> Result<ApplyRetentionResponse, ApplyRetentionError>;
}

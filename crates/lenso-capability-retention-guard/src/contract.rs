//! Authoritative source for the Retention Guard preflight role.

use lenso_contract_authoring as lenso;

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckRetentionRequestMode {
    Delete,
    Anonymize,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct CheckRetentionRequest {
    pub action_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub subject: String,
    pub mode: CheckRetentionRequestMode,
    pub reason: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct CheckRetentionResponse {
    pub allowed: bool,
    pub decision_id: String,
    pub reason_code: Option<String>,
}

#[derive(lenso::DomainError)]
pub enum CheckRetentionError {
    InvalidRequest,
    Forbidden,
    UnsupportedMode,
}

#[lenso::capability(
    id = "lenso.retention-guard",
    major = 1,
    version = "1.0.0",
    portable = true,
    cross_lane_transfer = true
)]
pub trait RetentionGuard {
    async fn check_retention(
        &self,
        context: lenso::Ctx<'_>,
        request: CheckRetentionRequest,
    ) -> Result<CheckRetentionResponse, CheckRetentionError>;
}

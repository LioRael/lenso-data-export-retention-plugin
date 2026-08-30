//! Authoritative source for the Data Retention coordinator contract.

use lenso_contract_authoring as lenso;

#[derive(serde::Deserialize)]
pub struct Nullable<T>(Option<T>);

impl<T: lenso::JsonSchema> lenso::JsonSchema for Nullable<T> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        format!("Nullable_{}", T::schema_name()).into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        format!("Nullable<{}>", T::schema_id()).into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        <Option<T> as lenso::JsonSchema>::json_schema(generator)
    }
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionMode {
    Delete,
    Anonymize,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionStatus {
    InProgress,
    Completed,
    Partial,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionParticipantStatus {
    Pending,
    Completed,
    Rejected,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ExecuteRetentionRequest {
    pub action_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub subject: String,
    pub mode: RetentionMode,
    pub reason: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ExecuteRetentionResponse {
    pub status: RetentionStatus,
    #[schemars(range(min = 0))]
    pub completed_count: i64,
    #[schemars(range(min = 0))]
    pub rejected_count: i64,
    #[schemars(range(min = 0))]
    pub pending_count: i64,
}

#[derive(lenso::DomainError)]
pub enum ExecuteRetentionError {
    InvalidRequest,
    IdempotencyConflict,
    Forbidden,
    BlockedByGuard,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReadRetentionRequest {
    pub action_id: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReadRetentionResponseResultsItem {
    pub provider_instance: String,
    pub status: RetentionParticipantStatus,
    pub receipt: Nullable<String>,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReadRetentionResponse {
    pub scope_kind: String,
    pub scope_id: String,
    pub subject: String,
    pub mode: RetentionMode,
    pub reason: String,
    pub status: RetentionStatus,
    pub results: Vec<ReadRetentionResponseResultsItem>,
}

#[derive(lenso::DomainError)]
pub enum ReadRetentionError {
    InvalidRequest,
    NotFound,
    Forbidden,
}

#[lenso::capability(
    id = "lenso.data-retention",
    major = 1,
    version = "1.1.0",
    portable = true,
    cross_lane_transfer = true
)]
pub trait DataRetention {
    async fn execute_retention(
        &self,
        context: lenso::Ctx<'_>,
        request: ExecuteRetentionRequest,
    ) -> Result<ExecuteRetentionResponse, ExecuteRetentionError>;

    async fn read_retention(
        &self,
        context: lenso::Ctx<'_>,
        request: ReadRetentionRequest,
    ) -> Result<ReadRetentionResponse, ReadRetentionError>;
}

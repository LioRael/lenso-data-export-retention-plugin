//! Authoritative source for the bounded Data Export Capability.

use lenso_contract_authoring as lenso;

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct CreateExportRequest {
    pub export_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub subject: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct CreateExportResponse {
    pub created: bool,
    #[schemars(range(min = 0))]
    pub source_count: i64,
    #[schemars(range(min = 0))]
    pub item_count: i64,
    /// Non-negative base-10 byte count encoded as a portable string.
    pub total_bytes: String,
}

#[derive(lenso::DomainError)]
pub enum CreateExportError {
    InvalidRequest,
    IdempotencyConflict,
    SourceRejected,
    ArtifactTooLarge,
    Forbidden,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReadExportRequest {
    pub export_id: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReadExportResponseItemsItem {
    pub provider_instance: String,
    pub item_name: String,
    pub media_type: String,
    #[schemars(extend("x-lenso-sensitive" = true))]
    pub payload: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReadExportResponse {
    pub scope_kind: String,
    pub scope_id: String,
    pub subject: String,
    pub items: Vec<ReadExportResponseItemsItem>,
    #[schemars(extend("format" = "date-time"))]
    pub created_at: String,
}

#[derive(lenso::DomainError)]
pub enum ReadExportError {
    InvalidRequest,
    NotFound,
    Forbidden,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PurgeExportRequest {
    pub export_id: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct PurgeExportResponse {
    pub changed: bool,
}

#[derive(lenso::DomainError)]
pub enum PurgeExportError {
    InvalidRequest,
    Forbidden,
}

#[lenso::capability(
    id = "lenso.data-export",
    major = 1,
    version = "1.0.0",
    portable = true,
    cross_lane_transfer = true
)]
pub trait DataExport {
    async fn create_export(
        &self,
        context: lenso::Ctx<'_>,
        request: CreateExportRequest,
    ) -> Result<CreateExportResponse, CreateExportError>;

    async fn read_export(
        &self,
        context: lenso::Ctx<'_>,
        request: ReadExportRequest,
    ) -> Result<ReadExportResponse, ReadExportError>;

    async fn purge_export(
        &self,
        context: lenso::Ctx<'_>,
        request: PurgeExportRequest,
    ) -> Result<PurgeExportResponse, PurgeExportError>;
}

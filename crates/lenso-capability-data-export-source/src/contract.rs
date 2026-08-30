//! Authoritative source for one Data Export Source contribution.

use lenso_contract_authoring as lenso;

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct CollectExportRequest {
    pub export_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub subject: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct CollectExportResponseItemsItem {
    pub item_name: String,
    pub media_type: String,
    #[schemars(extend("x-lenso-sensitive" = true))]
    pub payload: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct CollectExportResponse {
    pub items: Vec<CollectExportResponseItemsItem>,
}

#[derive(lenso::DomainError)]
pub enum CollectExportError {
    InvalidRequest,
    Forbidden,
}

#[lenso::capability(
    id = "lenso.data-export-source",
    major = 1,
    version = "1.0.0",
    portable = true,
    cross_lane_transfer = true
)]
pub trait DataExportSource {
    async fn collect_export(
        &self,
        context: lenso::Ctx<'_>,
        request: CollectExportRequest,
    ) -> Result<CollectExportResponse, CollectExportError>;
}

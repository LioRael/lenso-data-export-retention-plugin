//! Bounded inline Data Export and recoverable multi-Plugin Retention coordination.

mod operator;
mod schema;

use std::{cell::RefCell, collections::BTreeSet, fmt, rc::Rc, time::Duration};

use lenso::{ActivateContext, DeactivateContext, Lifecycle, ManyPort, Port, provides};
use lenso_capability_data_export as export;
use lenso_capability_data_export::{
    CreateExportError, CreateExportRequest, CreateExportResponse, DataExportCreateExport,
    DataExportPurgeExport, DataExportReadExport, PurgeExportError, PurgeExportRequest,
    PurgeExportResponse, ReadExportError, ReadExportRequest, ReadExportResponse,
    ReadExportResponseItemsItem,
};
use lenso_capability_data_export_source as source;
use lenso_capability_data_export_source::{CollectExportRequest, DataExportSourceInvocationError};
use lenso_capability_data_retention as retention;
use lenso_capability_data_retention::{
    DataRetentionExecuteRetention, DataRetentionReadRetention, ExecuteRetentionError,
    ExecuteRetentionRequest, ExecuteRetentionResponse, ReadRetentionError, ReadRetentionRequest,
    ReadRetentionResponse, ReadRetentionResponseResultsItem, RetentionMode,
    RetentionParticipantStatus, RetentionStatus,
};
use lenso_capability_retention_guard as guard;
use lenso_capability_retention_guard::{
    CheckRetentionRequest, CheckRetentionRequestMode, RetentionGuardInvocationError,
};
use lenso_capability_retention_participant as participant;
use lenso_capability_retention_participant::{
    ApplyRetentionRequest, ApplyRetentionRequestMode, RetentionParticipantInvocationError,
};
use lenso_capability_secrets as secrets;
use lenso_capability_secrets::{ResolveRequest, SecretsClient, SecretsInvocationError};
use lenso_kernel::{InvocationContext, NativeRequestFuture, RuntimeFailure};
use lenso_postgres_kit::OwnedPostgres;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use zeroize::Zeroizing;

use crate::schema::schema_plan;

pub use operator::{DataGovernanceOperator, DataGovernanceOperatorError};

const DEPENDENCY_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_GUARDS: usize = 16;

const fn default_max_guards() -> usize {
    DEFAULT_MAX_GUARDS
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DataGovernanceConfig {
    schema: String,
    database_url_secret: String,
    export_callers: Vec<String>,
    retention_callers: Vec<String>,
    max_sources: usize,
    #[serde(default = "default_max_guards")]
    max_guards: usize,
    max_participants: usize,
    max_items: usize,
    max_item_bytes: usize,
    max_total_bytes: usize,
}

impl DataGovernanceConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: impl Into<String>,
        database_url_secret: impl Into<String>,
        export_callers: Vec<String>,
        retention_callers: Vec<String>,
        max_sources: usize,
        max_participants: usize,
        max_items: usize,
        max_item_bytes: usize,
        max_total_bytes: usize,
    ) -> Result<Self, DataGovernanceConfigError> {
        let value = Self {
            schema: schema.into(),
            database_url_secret: database_url_secret.into(),
            export_callers,
            retention_callers,
            max_sources,
            max_guards: DEFAULT_MAX_GUARDS,
            max_participants,
            max_items,
            max_item_bytes,
            max_total_bytes,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), DataGovernanceConfigError> {
        schema_plan(self.schema.clone()).map_err(|_| DataGovernanceConfigError::InvalidSchema)?;
        if !valid_secret_reference(&self.database_url_secret) {
            return Err(DataGovernanceConfigError::InvalidSecretReference);
        }
        if self.export_callers.is_empty()
            || self.export_callers.iter().any(|caller| !valid_name(caller))
        {
            return Err(DataGovernanceConfigError::InvalidExportCallers);
        }
        if self.retention_callers.is_empty()
            || self
                .retention_callers
                .iter()
                .any(|caller| !valid_name(caller))
        {
            return Err(DataGovernanceConfigError::InvalidRetentionCallers);
        }
        if !(1..=64).contains(&self.max_sources)
            || self.max_guards > 64
            || !(1..=64).contains(&self.max_participants)
            || !(1..=512).contains(&self.max_items)
            || !(1..=1_048_576).contains(&self.max_item_bytes)
            || !(1..=16_777_216).contains(&self.max_total_bytes)
            || self.max_item_bytes > self.max_total_bytes
        {
            return Err(DataGovernanceConfigError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DataGovernanceConfigError {
    #[error("invalid owned PostgreSQL schema")]
    InvalidSchema,
    #[error("invalid database URL secret reference")]
    InvalidSecretReference,
    #[error("at least one valid Data Export caller is required")]
    InvalidExportCallers,
    #[error("at least one valid Retention caller is required")]
    InvalidRetentionCallers,
    #[error("bounded inline export limits are invalid")]
    InvalidLimits,
}

fn validate_config(config: &DataGovernanceConfig) -> Result<(), RuntimeFailure> {
    config
        .validate()
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: error.to_string(),
        })
}

#[lenso::plugin(
    lifecycle,
    configuration_schema = "configuration.schema.json",
    validate = validate_config
)]
#[derive(Clone)]
struct DataGovernancePlugin {
    #[config]
    config: DataGovernanceConfig,
    secrets: Port<secrets::SecretsClient>,
    sources: ManyPort<source::DataExportSourceClient>,
    guards: ManyPort<guard::RetentionGuardClient>,
    participants: ManyPort<participant::RetentionParticipantClient>,
    state: Rc<RefCell<Option<PreparedDataGovernance>>>,
}

#[derive(Clone)]
struct PreparedDataGovernance {
    postgres: OwnedPostgres,
}

impl fmt::Debug for PreparedDataGovernance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedDataGovernance")
            .field("schema", &self.postgres.schema())
            .finish()
    }
}

impl fmt::Debug for DataGovernancePlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DataGovernancePlugin")
            .field("prepared", &self.state.borrow().is_some())
            .field("sources_connected", &self.sources.is_connected())
            .field("guards_connected", &self.guards.is_connected())
            .field("participants_connected", &self.participants.is_connected())
            .finish_non_exhaustive()
    }
}

#[provides(export::DataExport, retention::DataRetention)]
impl DataGovernancePlugin {}

#[derive(Debug)]
struct ExportSummary {
    requester_instance: String,
    scope_kind: String,
    scope_id: String,
    subject: String,
    source_count: i64,
    item_count: i64,
    total_bytes: i64,
}

impl DataGovernancePlugin {
    fn prepared(&self) -> Result<PreparedDataGovernance, RuntimeFailure> {
        self.state
            .borrow()
            .clone()
            .ok_or(RuntimeFailure::PluginFailure {
                detail: "Data Export / Retention Plugin is not prepared".to_owned(),
            })
    }

    #[allow(clippy::too_many_lines)]
    fn create_export(
        &self,
        context: InvocationContext,
        request: CreateExportRequest,
    ) -> NativeRequestFuture<DataExportCreateExport> {
        let caller = allowed_caller(&context, &self.config.export_callers);
        let prepared = self.prepared();
        let sources = self.sources.clone();
        let config = self.config.clone();
        Box::pin(async move {
            let Some(caller) = caller else {
                return Ok(Err(CreateExportError::Forbidden));
            };
            if !valid_request_identity(
                &request.export_id,
                &request.scope_kind,
                &request.scope_id,
                &request.subject,
            ) {
                return Ok(Err(CreateExportError::InvalidRequest));
            }
            let prepared = prepared?;
            if let Some(existing) =
                load_export_summary(&prepared.postgres, &request.export_id).await?
            {
                return existing_export_response(&existing, &request, &caller);
            }
            if sources.is_empty() || sources.len() > config.max_sources {
                return Err(RuntimeFailure::InvalidResolvedPlan {
                    detail: "Data Export source cardinality is outside configured bounds"
                        .to_owned(),
                });
            }
            let mut items = Vec::new();
            let mut item_keys = BTreeSet::new();
            let mut total_bytes = 0_usize;
            for source_provider in sources.iter() {
                let provider_instance = source_provider.provider_instance();
                if !valid_name(provider_instance) {
                    return Err(RuntimeFailure::InvalidResolvedPlan {
                        detail: "Data Export Source has an invalid Instance key".to_owned(),
                    });
                }
                let response = match source_provider
                    .collect_export_with_context(
                        context.clone(),
                        CollectExportRequest {
                            export_id: request.export_id.clone(),
                            scope_kind: request.scope_kind.clone(),
                            scope_id: request.scope_id.clone(),
                            subject: request.subject.clone(),
                        },
                    )
                    .await
                {
                    Ok(response) => response,
                    Err(DataExportSourceInvocationError::Domain(_)) => {
                        return Ok(Err(CreateExportError::SourceRejected));
                    }
                    Err(DataExportSourceInvocationError::Runtime(error)) => return Err(error),
                };
                for item in response.items {
                    let payload_bytes = item.payload.len();
                    if !valid_item_name(&item.item_name)
                        || !valid_media_type(&item.media_type)
                        || item.payload.contains('\0')
                        || !item_keys.insert((provider_instance.to_owned(), item.item_name.clone()))
                    {
                        return Err(RuntimeFailure::ProtocolViolation {
                            capability: source::CAPABILITY_ID,
                        });
                    }
                    if payload_bytes > config.max_item_bytes {
                        return Ok(Err(CreateExportError::ArtifactTooLarge));
                    }
                    if items.len() == config.max_items {
                        return Ok(Err(CreateExportError::ArtifactTooLarge));
                    }
                    total_bytes = total_bytes.saturating_add(payload_bytes);
                    if total_bytes > config.max_total_bytes {
                        return Ok(Err(CreateExportError::ArtifactTooLarge));
                    }
                    items.push(ReadExportResponseItemsItem {
                        provider_instance: provider_instance.to_owned(),
                        item_name: item.item_name,
                        media_type: item.media_type,
                        payload: item.payload,
                    });
                }
            }
            let Some(items_json) = serialize_export_items(&items, config.max_total_bytes)? else {
                return Ok(Err(CreateExportError::ArtifactTooLarge));
            };
            let source_count = i64::try_from(sources.len()).expect("source bound validated");
            let item_count = i64::try_from(items.len()).expect("item bound validated");
            let total_bytes_i64 = i64::try_from(total_bytes).expect("byte bound validated");
            let inserted = sqlx::query("INSERT INTO data_exports(export_id,requester_instance,scope_kind,scope_id,subject,source_count,item_count,total_bytes,items) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING")
                .bind(&request.export_id)
                .bind(&caller)
                .bind(&request.scope_kind)
                .bind(&request.scope_id)
                .bind(&request.subject)
                .bind(source_count)
                .bind(item_count)
                .bind(total_bytes_i64)
                .bind(sqlx::types::Json(items_json))
                .execute(prepared.postgres.pool())
                .await
                .map_err(|source| runtime(DataGovernanceError::Database { operation: "store data export", source }))?;
            if inserted.rows_affected() == 0 {
                let existing = load_export_summary(&prepared.postgres, &request.export_id)
                    .await?
                    .ok_or(RuntimeFailure::PluginFailure {
                        detail: "concurrent Data Export disappeared".to_owned(),
                    })?;
                return existing_export_response(&existing, &request, &caller);
            }
            Ok(Ok(CreateExportResponse {
                created: true,
                source_count,
                item_count,
                total_bytes: total_bytes.to_string(),
            }))
        })
    }

    #[allow(clippy::needless_pass_by_value)]
    fn read_export(
        &self,
        context: InvocationContext,
        request: ReadExportRequest,
    ) -> NativeRequestFuture<DataExportReadExport> {
        let caller = allowed_caller(&context, &self.config.export_callers);
        let prepared = self.prepared();
        Box::pin(async move {
            let Some(caller) = caller else {
                return Ok(Err(ReadExportError::Forbidden));
            };
            if !valid_name(&request.export_id) {
                return Ok(Err(ReadExportError::InvalidRequest));
            }
            let prepared = prepared?;
            let row = sqlx::query("SELECT scope_kind,scope_id,subject,items,created_at FROM data_exports WHERE export_id=$1 AND requester_instance=$2")
                .bind(&request.export_id)
                .bind(&caller)
                .fetch_optional(prepared.postgres.pool())
                .await
                .map_err(|source| runtime(DataGovernanceError::Database { operation: "read data export", source }))?;
            let Some(row) = row else {
                return Ok(Err(ReadExportError::NotFound));
            };
            let items: sqlx::types::Json<serde_json::Value> =
                decode(&row, "items", "decode export artifact")?;
            let items = serde_json::from_value(items.0)
                .map_err(|error| runtime(DataGovernanceError::ArtifactSerialization(error)))?;
            let created_at: OffsetDateTime =
                decode(&row, "created_at", "decode export creation time")?;
            Ok(Ok(ReadExportResponse {
                scope_kind: decode(&row, "scope_kind", "decode export scope kind")?,
                scope_id: decode(&row, "scope_id", "decode export scope id")?,
                subject: decode(&row, "subject", "decode export subject")?,
                items,
                created_at: format_time(created_at)?,
            }))
        })
    }

    #[allow(clippy::needless_pass_by_value)]
    fn purge_export(
        &self,
        context: InvocationContext,
        request: PurgeExportRequest,
    ) -> NativeRequestFuture<DataExportPurgeExport> {
        let caller = allowed_caller(&context, &self.config.export_callers);
        let prepared = self.prepared();
        Box::pin(async move {
            let Some(caller) = caller else {
                return Ok(Err(PurgeExportError::Forbidden));
            };
            if !valid_name(&request.export_id) {
                return Ok(Err(PurgeExportError::InvalidRequest));
            }
            let prepared = prepared?;
            let changed = sqlx::query(
                "DELETE FROM data_exports WHERE export_id=$1 AND requester_instance=$2",
            )
            .bind(&request.export_id)
            .bind(&caller)
            .execute(prepared.postgres.pool())
            .await
            .map_err(|source| {
                runtime(DataGovernanceError::Database {
                    operation: "purge data export",
                    source,
                })
            })?
            .rows_affected()
                == 1;
            Ok(Ok(PurgeExportResponse { changed }))
        })
    }
}

impl DataGovernancePlugin {
    #[allow(clippy::too_many_lines)]
    fn execute_retention(
        &self,
        context: InvocationContext,
        request: ExecuteRetentionRequest,
    ) -> NativeRequestFuture<DataRetentionExecuteRetention> {
        let caller = allowed_caller(&context, &self.config.retention_callers);
        let prepared = self.prepared();
        let guards = self.guards.clone();
        let participants = self.participants.clone();
        let max_guards = self.config.max_guards;
        let max_participants = self.config.max_participants;
        Box::pin(async move {
            let Some(caller) = caller else {
                return Ok(Err(ExecuteRetentionError::Forbidden));
            };
            if !valid_request_identity(
                &request.action_id,
                &request.scope_kind,
                &request.scope_id,
                &request.subject,
            ) || request.reason.trim().is_empty()
                || request.reason.len() > 512
                || request.reason.chars().any(char::is_control)
            {
                return Ok(Err(ExecuteRetentionError::InvalidRequest));
            }
            if participants.is_empty() || participants.len() > max_participants {
                return Err(RuntimeFailure::InvalidResolvedPlan {
                    detail: "Retention participant cardinality is outside configured bounds"
                        .to_owned(),
                });
            }
            if guards.len() > max_guards {
                return Err(RuntimeFailure::InvalidResolvedPlan {
                    detail: "Retention guard cardinality is outside configured bounds".to_owned(),
                });
            }
            let guard_instances = guards
                .iter()
                .map(|provider| provider.provider_instance().to_owned())
                .collect::<Vec<_>>();
            if guard_instances.iter().any(|value| !valid_name(value))
                || guard_instances.iter().collect::<BTreeSet<_>>().len() != guard_instances.len()
            {
                return Err(RuntimeFailure::InvalidResolvedPlan {
                    detail: "Retention Guard Instance keys are invalid or duplicated".to_owned(),
                });
            }
            for provider in guards.iter() {
                let response = provider
                    .check_retention_with_context(
                        context.clone(),
                        CheckRetentionRequest {
                            action_id: request.action_id.clone(),
                            scope_kind: request.scope_kind.clone(),
                            scope_id: request.scope_id.clone(),
                            subject: request.subject.clone(),
                            mode: match &request.mode {
                                RetentionMode::Delete => CheckRetentionRequestMode::Delete,
                                RetentionMode::Anonymize => CheckRetentionRequestMode::Anonymize,
                            },
                            reason: request.reason.trim().to_owned(),
                        },
                    )
                    .await;
                match response {
                    Ok(response) => {
                        if !valid_guard_decision(
                            response.allowed,
                            &response.decision_id,
                            response
                                .reason_code
                                .as_ref()
                                .and_then(|reason_code| reason_code.as_deref()),
                        ) {
                            return Err(RuntimeFailure::ProtocolViolation {
                                capability: guard::CAPABILITY_ID,
                            });
                        }
                        if !response.allowed {
                            return Ok(Err(ExecuteRetentionError::BlockedByGuard));
                        }
                    }
                    Err(RetentionGuardInvocationError::Domain(_)) => {
                        return Err(RuntimeFailure::PluginFailure {
                            detail: format!(
                                "Retention Guard `{}` rejected a valid preflight request",
                                provider.provider_instance()
                            ),
                        });
                    }
                    Err(RetentionGuardInvocationError::Runtime(error)) => return Err(error),
                }
            }
            let prepared = prepared?;
            let participant_instances = participants
                .iter()
                .map(|provider| provider.provider_instance().to_owned())
                .collect::<Vec<_>>();
            if participant_instances.iter().any(|value| !valid_name(value))
                || participant_instances.iter().collect::<BTreeSet<_>>().len()
                    != participant_instances.len()
            {
                return Err(RuntimeFailure::InvalidResolvedPlan {
                    detail: "Retention participant Instance keys are invalid or duplicated"
                        .to_owned(),
                });
            }
            let mode = retention_mode_name(&request.mode);
            sqlx::query("INSERT INTO retention_actions(action_id,requester_instance,scope_kind,scope_id,subject,mode,reason,participant_instances) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING")
                .bind(&request.action_id)
                .bind(&caller)
                .bind(&request.scope_kind)
                .bind(&request.scope_id)
                .bind(&request.subject)
                .bind(mode)
                .bind(request.reason.trim())
                .bind(&participant_instances)
                .execute(prepared.postgres.pool())
                .await
                .map_err(|source| runtime(DataGovernanceError::Database { operation: "create retention action", source }))?;
            let identity = load_retention_identity(&prepared.postgres, &request.action_id, None)
                .await?
                .ok_or(RuntimeFailure::PluginFailure {
                    detail: "Retention action disappeared".to_owned(),
                })?;
            if !same_retention_intent(&identity, &request, &caller, mode) {
                return Ok(Err(ExecuteRetentionError::IdempotencyConflict));
            }
            let expected = identity
                .participant_instances
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let completed = completed_participants(&prepared.postgres, &request.action_id).await?;
            for provider in participants.iter() {
                let provider_instance = provider.provider_instance();
                if !expected.contains(provider_instance) || completed.contains(provider_instance) {
                    continue;
                }
                let result = provider
                    .apply_retention_with_context(
                        context.clone(),
                        ApplyRetentionRequest {
                            action_id: request.action_id.clone(),
                            scope_kind: request.scope_kind.clone(),
                            scope_id: request.scope_id.clone(),
                            subject: request.subject.clone(),
                            mode: match &request.mode {
                                RetentionMode::Delete => ApplyRetentionRequestMode::Delete,
                                RetentionMode::Anonymize => ApplyRetentionRequestMode::Anonymize,
                            },
                            reason: request.reason.trim().to_owned(),
                        },
                    )
                    .await;
                match result {
                    Ok(response) => {
                        if response.receipt.trim().is_empty()
                            || response.receipt.len() > 1024
                            || response.receipt.chars().any(char::is_control)
                        {
                            return Err(RuntimeFailure::ProtocolViolation {
                                capability: participant::CAPABILITY_ID,
                            });
                        }
                        store_retention_result(
                            &prepared.postgres,
                            &request.action_id,
                            provider_instance,
                            "completed",
                            Some(response.receipt.trim()),
                        )
                        .await?;
                    }
                    Err(RetentionParticipantInvocationError::Domain(_)) => {
                        store_retention_result(
                            &prepared.postgres,
                            &request.action_id,
                            provider_instance,
                            "rejected",
                            None,
                        )
                        .await?;
                    }
                    Err(RetentionParticipantInvocationError::Runtime(error)) => return Err(error),
                }
            }
            let response =
                load_retention_response(&prepared.postgres, &request.action_id, Some(&caller))
                    .await?
                    .ok_or(RuntimeFailure::PluginFailure {
                        detail: "Retention action disappeared after execution".to_owned(),
                    })?;
            let (completed_count, rejected_count, pending_count) = retention_counts(&response);
            Ok(Ok(ExecuteRetentionResponse {
                status: response.status,
                completed_count,
                rejected_count,
                pending_count,
            }))
        })
    }

    #[allow(clippy::needless_pass_by_value)]
    fn read_retention(
        &self,
        context: InvocationContext,
        request: ReadRetentionRequest,
    ) -> NativeRequestFuture<DataRetentionReadRetention> {
        let caller = allowed_caller(&context, &self.config.retention_callers);
        let prepared = self.prepared();
        Box::pin(async move {
            let Some(caller) = caller else {
                return Ok(Err(ReadRetentionError::Forbidden));
            };
            if !valid_name(&request.action_id) {
                return Ok(Err(ReadRetentionError::InvalidRequest));
            }
            let prepared = prepared?;
            let response =
                load_retention_response(&prepared.postgres, &request.action_id, Some(&caller))
                    .await?;
            match response {
                Some(response) => Ok(Ok(response)),
                None => Ok(Err(ReadRetentionError::NotFound)),
            }
        })
    }
}

impl Lifecycle for DataGovernancePlugin {
    async fn activate(&self, context: ActivateContext) -> Result<(), RuntimeFailure> {
        if self.sources.is_empty()
            || self.sources.len() > self.config.max_sources
            || self.guards.len() > self.config.max_guards
            || self.participants.is_empty()
            || self.participants.len() > self.config.max_participants
        {
            return Err(RuntimeFailure::InvalidResolvedPlan {
                detail: "Data Export Source or Retention Participant cardinality is invalid"
                    .to_owned(),
            });
        }
        let source_instances = self
            .sources
            .iter()
            .map(lenso::BoundCapabilityClient::provider_instance)
            .collect::<Vec<_>>();
        let guard_instances = self
            .guards
            .iter()
            .map(lenso::BoundCapabilityClient::provider_instance)
            .collect::<Vec<_>>();
        let participant_instances = self
            .participants
            .iter()
            .map(lenso::BoundCapabilityClient::provider_instance)
            .collect::<Vec<_>>();
        if source_instances.iter().any(|value| !valid_name(value))
            || source_instances.iter().collect::<BTreeSet<_>>().len() != source_instances.len()
            || guard_instances.iter().any(|value| !valid_name(value))
            || guard_instances.iter().collect::<BTreeSet<_>>().len() != guard_instances.len()
            || participant_instances.iter().any(|value| !valid_name(value))
            || participant_instances.iter().collect::<BTreeSet<_>>().len()
                != participant_instances.len()
        {
            return Err(RuntimeFailure::InvalidResolvedPlan {
                detail: "Data Export Source, Retention Guard, or Retention Participant Instance keys are invalid or duplicated"
                    .to_owned(),
            });
        }
        let dependencies = context.dependencies().clone();
        let database_url = resolve_secret(
            &self.secrets,
            &dependencies,
            context.cancellation(),
            &self.config.database_url_secret,
        )
        .await?;
        let postgres = OwnedPostgres::prepare(
            &database_url,
            schema_plan(self.config.schema.clone()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: error.to_string(),
                }
            })?,
        )
        .await
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: error.to_string(),
        })?;
        self.state
            .replace(Some(PreparedDataGovernance { postgres }));
        Ok(())
    }

    async fn deactivate(&self, _context: DeactivateContext) -> Result<(), RuntimeFailure> {
        let prepared = self.state.borrow_mut().take();
        if let Some(prepared) = prepared {
            prepared.postgres.pool().close().await;
        }
        Ok(())
    }
}

async fn load_export_summary(
    postgres: &OwnedPostgres,
    export_id: &str,
) -> Result<Option<ExportSummary>, RuntimeFailure> {
    let row = sqlx::query("SELECT requester_instance,scope_kind,scope_id,subject,source_count,item_count,total_bytes FROM data_exports WHERE export_id=$1")
        .bind(export_id)
        .fetch_optional(postgres.pool())
        .await
        .map_err(|source| runtime(DataGovernanceError::Database { operation: "read export idempotency key", source }))?;
    row.map(|row| {
        Ok(ExportSummary {
            requester_instance: decode(
                &row,
                "requester_instance",
                "decode export requester Instance",
            )?,
            scope_kind: decode(&row, "scope_kind", "decode export scope kind")?,
            scope_id: decode(&row, "scope_id", "decode export scope id")?,
            subject: decode(&row, "subject", "decode export subject")?,
            source_count: decode(&row, "source_count", "decode export source count")?,
            item_count: decode(&row, "item_count", "decode export item count")?,
            total_bytes: decode(&row, "total_bytes", "decode export byte count")?,
        })
    })
    .transpose()
}

#[allow(clippy::unnecessary_wraps)]
fn existing_export_response(
    existing: &ExportSummary,
    request: &CreateExportRequest,
    requester_instance: &str,
) -> Result<Result<CreateExportResponse, CreateExportError>, RuntimeFailure> {
    if existing.requester_instance != requester_instance
        || existing.scope_kind != request.scope_kind
        || existing.scope_id != request.scope_id
        || existing.subject != request.subject
    {
        return Ok(Err(CreateExportError::IdempotencyConflict));
    }
    Ok(Ok(CreateExportResponse {
        created: false,
        source_count: existing.source_count,
        item_count: existing.item_count,
        total_bytes: existing.total_bytes.to_string(),
    }))
}

fn serialize_export_items(
    items: &[ReadExportResponseItemsItem],
    max_total_bytes: usize,
) -> Result<Option<serde_json::Value>, RuntimeFailure> {
    let serialized = serde_json::to_vec(items)
        .map_err(|error| runtime(DataGovernanceError::ArtifactSerialization(error)))?;
    if serialized.len() > max_total_bytes {
        return Ok(None);
    }
    serde_json::from_slice(&serialized)
        .map(Some)
        .map_err(|error| runtime(DataGovernanceError::ArtifactSerialization(error)))
}

#[derive(Debug)]
struct RetentionIdentity {
    requester_instance: String,
    scope_kind: String,
    scope_id: String,
    subject: String,
    mode: String,
    reason: String,
    participant_instances: Vec<String>,
}

async fn load_retention_identity(
    postgres: &OwnedPostgres,
    action_id: &str,
    requester_constraint: Option<&str>,
) -> Result<Option<RetentionIdentity>, RuntimeFailure> {
    let row = sqlx::query("SELECT requester_instance,scope_kind,scope_id,subject,mode,reason,participant_instances FROM retention_actions WHERE action_id=$1 AND ($2::text IS NULL OR requester_instance=$2)")
        .bind(action_id)
        .bind(requester_constraint)
        .fetch_optional(postgres.pool())
        .await
        .map_err(|source| runtime(DataGovernanceError::Database { operation: "read retention identity", source }))?;
    row.map(|row| {
        Ok(RetentionIdentity {
            requester_instance: decode(
                &row,
                "requester_instance",
                "decode retention requester Instance",
            )?,
            scope_kind: decode(&row, "scope_kind", "decode retention scope kind")?,
            scope_id: decode(&row, "scope_id", "decode retention scope id")?,
            subject: decode(&row, "subject", "decode retention subject")?,
            mode: decode(&row, "mode", "decode retention mode")?,
            reason: decode(&row, "reason", "decode retention reason")?,
            participant_instances: decode(
                &row,
                "participant_instances",
                "decode retention participants",
            )?,
        })
    })
    .transpose()
}

fn same_retention_intent(
    identity: &RetentionIdentity,
    request: &ExecuteRetentionRequest,
    requester_instance: &str,
    mode: &str,
) -> bool {
    identity.requester_instance == requester_instance
        && identity.scope_kind == request.scope_kind
        && identity.scope_id == request.scope_id
        && identity.subject == request.subject
        && identity.mode == mode
        && identity.reason == request.reason.trim()
}

async fn completed_participants(
    postgres: &OwnedPostgres,
    action_id: &str,
) -> Result<BTreeSet<String>, RuntimeFailure> {
    let rows = sqlx::query(
        "SELECT provider_instance FROM retention_results WHERE action_id=$1 AND status='completed'",
    )
    .bind(action_id)
    .fetch_all(postgres.pool())
    .await
    .map_err(|source| {
        runtime(DataGovernanceError::Database {
            operation: "read completed retention participants",
            source,
        })
    })?;
    rows.into_iter()
        .map(|row| decode(&row, "provider_instance", "decode retention provider"))
        .collect()
}

async fn store_retention_result(
    postgres: &OwnedPostgres,
    action_id: &str,
    provider_instance: &str,
    status: &str,
    receipt: Option<&str>,
) -> Result<(), RuntimeFailure> {
    sqlx::query("INSERT INTO retention_results(action_id,provider_instance,status,receipt) VALUES($1,$2,$3,$4) ON CONFLICT(action_id,provider_instance) DO UPDATE SET status=CASE WHEN retention_results.status='completed' THEN retention_results.status ELSE EXCLUDED.status END,receipt=CASE WHEN retention_results.status='completed' THEN retention_results.receipt ELSE EXCLUDED.receipt END,attempted_at=transaction_timestamp()")
        .bind(action_id)
        .bind(provider_instance)
        .bind(status)
        .bind(receipt)
        .execute(postgres.pool())
        .await
        .map_err(|source| runtime(DataGovernanceError::Database { operation: "store retention result", source }))?;
    Ok(())
}

async fn load_retention_response(
    postgres: &OwnedPostgres,
    action_id: &str,
    requester_constraint: Option<&str>,
) -> Result<Option<ReadRetentionResponse>, RuntimeFailure> {
    let Some(identity) = load_retention_identity(postgres, action_id, requester_constraint).await?
    else {
        return Ok(None);
    };
    let rows = sqlx::query(
        "SELECT provider_instance,status,receipt FROM retention_results WHERE action_id=$1",
    )
    .bind(action_id)
    .fetch_all(postgres.pool())
    .await
    .map_err(|source| {
        runtime(DataGovernanceError::Database {
            operation: "read retention results",
            source,
        })
    })?;
    let mut stored = std::collections::BTreeMap::new();
    for row in rows {
        let provider_instance: String =
            decode(&row, "provider_instance", "decode retention provider")?;
        let status: String = decode(&row, "status", "decode retention result status")?;
        let receipt: Option<String> = decode(&row, "receipt", "decode retention receipt")?;
        stored.insert(provider_instance, (status, receipt));
    }
    let mut results = Vec::with_capacity(identity.participant_instances.len());
    for provider_instance in identity.participant_instances {
        let (status, receipt) = match stored.remove(&provider_instance) {
            Some((status, receipt)) if status == "completed" => {
                (RetentionParticipantStatus::Completed, receipt)
            }
            Some(_) => (RetentionParticipantStatus::Rejected, None),
            None => (RetentionParticipantStatus::Pending, None),
        };
        results.push(ReadRetentionResponseResultsItem {
            provider_instance,
            status,
            receipt,
        });
    }
    let completed = results
        .iter()
        .filter(|result| matches!(&result.status, RetentionParticipantStatus::Completed))
        .count();
    let rejected = results
        .iter()
        .filter(|result| matches!(&result.status, RetentionParticipantStatus::Rejected))
        .count();
    let status = if completed + rejected < results.len() {
        RetentionStatus::InProgress
    } else if rejected > 0 {
        RetentionStatus::Partial
    } else {
        RetentionStatus::Completed
    };
    Ok(Some(ReadRetentionResponse {
        scope_kind: identity.scope_kind,
        scope_id: identity.scope_id,
        subject: identity.subject,
        mode: parse_retention_mode(&identity.mode)?,
        reason: identity.reason,
        status,
        results,
    }))
}

fn retention_counts(response: &ReadRetentionResponse) -> (i64, i64, i64) {
    let completed = response
        .results
        .iter()
        .filter(|result| matches!(&result.status, RetentionParticipantStatus::Completed))
        .count();
    let rejected = response
        .results
        .iter()
        .filter(|result| matches!(&result.status, RetentionParticipantStatus::Rejected))
        .count();
    let pending = response.results.len() - completed - rejected;
    (
        i64::try_from(completed).expect("participant bound validated"),
        i64::try_from(rejected).expect("participant bound validated"),
        i64::try_from(pending).expect("participant bound validated"),
    )
}

fn retention_mode_name(mode: &RetentionMode) -> &'static str {
    match mode {
        RetentionMode::Delete => "delete",
        RetentionMode::Anonymize => "anonymize",
    }
}

fn parse_retention_mode(value: &str) -> Result<RetentionMode, RuntimeFailure> {
    match value {
        "delete" => Ok(RetentionMode::Delete),
        "anonymize" => Ok(RetentionMode::Anonymize),
        _ => Err(RuntimeFailure::PluginFailure {
            detail: "stored Retention mode is invalid".to_owned(),
        }),
    }
}

#[derive(Debug, Error)]
enum DataGovernanceError {
    #[error("PostgreSQL operation `{operation}` failed")]
    Database {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("export artifact serialization failed")]
    ArtifactSerialization(#[source] serde_json::Error),
}

fn runtime(error: impl fmt::Display) -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: error.to_string(),
    }
}

fn decode<T>(
    row: &sqlx::postgres::PgRow,
    column: &'static str,
    operation: &'static str,
) -> Result<T, RuntimeFailure>
where
    for<'row> T: sqlx::Decode<'row, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get(column)
        .map_err(|source| runtime(DataGovernanceError::Database { operation, source }))
}

fn allowed_caller(context: &InvocationContext, allowed: &[String]) -> Option<String> {
    context
        .caller_instance()
        .filter(|caller| allowed.iter().any(|candidate| candidate == caller))
        .map(ToOwned::to_owned)
}

fn valid_request_identity(id: &str, scope_kind: &str, scope_id: &str, subject: &str) -> bool {
    valid_name(id) && valid_dimension(scope_kind) && valid_name(scope_id) && valid_name(subject)
}

fn valid_guard_decision(allowed: bool, decision_id: &str, reason_code: Option<&str>) -> bool {
    valid_name(decision_id)
        && match (allowed, reason_code) {
            (true, None) => true,
            (false, Some(reason_code)) => valid_dimension(reason_code),
            _ => false,
        }
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}

fn valid_dimension(value: &str) -> bool {
    valid_name(value) && !value.starts_with('.') && !value.ends_with('.')
}

fn valid_item_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains("//")
        && value
            .split('/')
            .all(|segment| segment != "." && segment != "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
}

fn valid_media_type(value: &str) -> bool {
    let Some((kind, subtype)) = value.split_once('/') else {
        return false;
    };
    !kind.is_empty()
        && !subtype.is_empty()
        && !subtype.contains('/')
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#' | b'$' | b'&' | b'+' | b'-' | b'.' | b'/' | b'^' | b'_' | b'|'
                )
        })
}

fn valid_secret_reference(reference: &str) -> bool {
    !reference.is_empty()
        && reference.len() <= 256
        && !reference.starts_with('/')
        && !reference.ends_with('/')
        && !reference.contains("//")
        && reference
            .split('/')
            .all(|segment| segment != "." && segment != "..")
        && reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
}

fn format_time(value: OffsetDateTime) -> Result<String, RuntimeFailure> {
    value
        .format(&Rfc3339)
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: error.to_string(),
        })
}

async fn resolve_secret(
    secrets: &SecretsClient,
    dependencies: &lenso_kernel::PluginDependencies,
    cancellation: lenso_kernel::CancellationToken,
    reference: &str,
) -> Result<Zeroizing<String>, RuntimeFailure> {
    let context = dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation)?;
    secrets
        .resolve_with_context(
            context,
            ResolveRequest {
                reference: reference.to_owned(),
            },
        )
        .await
        .map(|value| Zeroizing::new(value.value))
        .map_err(|error| match error {
            SecretsInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                detail: format!("secret `{reference}` was rejected"),
            },
            SecretsInvocationError::Runtime(error) => error,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lenso_kernel::CancellationToken;

    fn plugin() -> DataGovernancePlugin {
        DataGovernancePlugin {
            config: DataGovernanceConfig::new(
                "data_governance",
                "data-governance/database",
                vec!["privacy-service".to_owned()],
                vec!["privacy-admin".to_owned()],
                8,
                8,
                64,
                262_144,
                1_048_576,
            )
            .unwrap(),
            secrets: Port::default(),
            sources: ManyPort::default(),
            guards: ManyPort::default(),
            participants: ManyPort::default(),
            state: Rc::new(RefCell::new(None)),
        }
    }

    #[test]
    fn configuration_rejects_unbounded_inline_artifacts() {
        let error = DataGovernanceConfig::new(
            "data_governance",
            "data-governance/database",
            vec!["privacy-service".to_owned()],
            vec!["privacy-admin".to_owned()],
            8,
            8,
            64,
            2_000_000,
            1_000_000,
        )
        .unwrap_err();
        assert_eq!(error, DataGovernanceConfigError::InvalidLimits);
    }

    #[test]
    fn serialized_export_envelope_is_part_of_the_total_bound() {
        let items = vec![ReadExportResponseItemsItem {
            provider_instance: "profile-source".to_owned(),
            item_name: "profile.json".to_owned(),
            media_type: "application/json".to_owned(),
            payload: "x".to_owned(),
        }];
        assert!(serialize_export_items(&items, 1).unwrap().is_none());
        let encoded_len = serde_json::to_vec(&items).unwrap().len();
        assert!(
            serialize_export_items(&items, encoded_len)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn export_idempotency_is_scoped_to_the_exact_requester_instance() {
        let request = CreateExportRequest {
            export_id: "exp_1".to_owned(),
            scope_kind: "organization".to_owned(),
            scope_id: "org_acme".to_owned(),
            subject: "usr_1".to_owned(),
        };
        let existing = ExportSummary {
            requester_instance: "privacy-service".to_owned(),
            scope_kind: request.scope_kind.clone(),
            scope_id: request.scope_id.clone(),
            subject: request.subject.clone(),
            source_count: 1,
            item_count: 1,
            total_bytes: 1,
        };
        assert_eq!(
            existing_export_response(&existing, &request, "other-privacy-service").unwrap(),
            Err(CreateExportError::IdempotencyConflict)
        );
    }

    #[test]
    fn retention_idempotency_is_scoped_to_the_exact_requester_instance() {
        let request = ExecuteRetentionRequest {
            action_id: "ret_1".to_owned(),
            scope_kind: "organization".to_owned(),
            scope_id: "org_acme".to_owned(),
            subject: "usr_1".to_owned(),
            mode: RetentionMode::Delete,
            reason: "account closure".to_owned(),
        };
        let identity = RetentionIdentity {
            requester_instance: "privacy-admin".to_owned(),
            scope_kind: request.scope_kind.clone(),
            scope_id: request.scope_id.clone(),
            subject: request.subject.clone(),
            mode: "delete".to_owned(),
            reason: request.reason.clone(),
            participant_instances: vec!["profile-store".to_owned()],
        };
        assert!(same_retention_intent(
            &identity,
            &request,
            "privacy-admin",
            "delete"
        ));
        assert!(!same_retention_intent(
            &identity,
            &request,
            "other-privacy-admin",
            "delete"
        ));
    }

    #[test]
    fn retention_guard_decisions_fail_closed_on_ambiguous_evidence() {
        assert!(valid_guard_decision(true, "guard_1:decision_1", None));
        assert!(valid_guard_decision(
            false,
            "guard_1:decision_2",
            Some("active_legal_hold")
        ));
        assert!(!valid_guard_decision(false, "guard_1:decision_3", None));
        assert!(!valid_guard_decision(
            true,
            "guard_1:decision_4",
            Some("unexpected_reason")
        ));
    }

    #[tokio::test]
    async fn untrusted_export_caller_is_rejected_before_ports_or_storage() {
        let context = InvocationContext::new(1, None, CancellationToken::new())
            .with_caller_instance("untrusted");
        let result = plugin()
            .create_export(
                context,
                CreateExportRequest {
                    export_id: "exp_1".to_owned(),
                    scope_kind: "organization".to_owned(),
                    scope_id: "org_acme".to_owned(),
                    subject: "usr_1".to_owned(),
                },
            )
            .await
            .unwrap();
        assert_eq!(result, Err(CreateExportError::Forbidden));
    }

    #[tokio::test]
    async fn malformed_retention_is_a_domain_error_before_ports_or_storage() {
        let context = InvocationContext::new(1, None, CancellationToken::new())
            .with_caller_instance("privacy-admin");
        let result = plugin()
            .execute_retention(
                context,
                ExecuteRetentionRequest {
                    action_id: "ret_1".to_owned(),
                    scope_kind: "organization".to_owned(),
                    scope_id: "org_acme".to_owned(),
                    subject: "usr_1".to_owned(),
                    mode: RetentionMode::Delete,
                    reason: String::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(result, Err(ExecuteRetentionError::InvalidRequest));
    }
}

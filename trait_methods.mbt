///|
pub extend Lsn with Eq::{equal, not_equal}

///|
pub extend Lsn with Compare::{compare, op_lt, op_le, op_gt, op_ge}

///|
pub extend Lsn with Debug::{to_repr}

///|
pub extend Lsn with ToJson::{to_json}

///|
pub extend Value with Eq::{equal, not_equal}

///|
pub extend Value with Debug::{to_repr}

///|
pub extend Value with ToJson::{to_json}

///|
pub extend Column with Eq::{equal, not_equal}

///|
pub extend Column with Debug::{to_repr}

///|
pub extend Column with ToJson::{to_json}

///|
pub extend Relation with Eq::{equal, not_equal}

///|
pub extend Relation with Debug::{to_repr}

///|
pub extend Relation with ToJson::{to_json}

///|
pub extend TypeInfo with Eq::{equal, not_equal}

///|
pub extend TypeInfo with Debug::{to_repr}

///|
pub extend TypeInfo with ToJson::{to_json}

///|
pub extend OldTuple with Eq::{equal, not_equal}

///|
pub extend OldTuple with Debug::{to_repr}

///|
pub extend OldTuple with ToJson::{to_json}

///|
pub extend Change with Eq::{equal, not_equal}

///|
pub extend Change with Debug::{to_repr}

///|
pub extend Change with ToJson::{to_json}

///|
pub extend Transaction with Debug::{to_repr}

///|
pub extend Transaction with ToJson::{to_json}

///|
pub extend Diagnostics with Debug::{to_repr}

///|
pub extend Diagnostics with ToJson::{to_json}

///|
pub extend CdcError with Debug::{to_repr}

///|
pub extend Security with Debug::{to_repr}

///|
pub extend SnapshotEvent with Debug::{to_repr}

///|
pub extend LogicalMessage with Eq::{equal, not_equal}

///|
pub extend LogicalMessage with Debug::{to_repr}

///|
pub extend LogicalMessage with ToJson::{to_json}

///|
pub extend WriteStatus with Eq::{equal, not_equal}

///|
pub extend WriteStatus with Debug::{to_repr}

///|
pub extend TableCopyBatchId with Eq::{equal, not_equal}

///|
pub extend TableCopyBatchId with Debug::{to_repr}

///|
pub extend TableCopyBatchId with ToJson::{to_json}

///|
pub extend TableSyncPhase with Eq::{equal, not_equal}

///|
pub extend TableSyncPhase with Debug::{to_repr}

///|
pub extend TableSyncStatus with Debug::{to_repr}

///|
pub extend PipelineStatus with Debug::{to_repr}

///|
pub extend SchemaId with Eq::{equal, not_equal}

///|
pub extend SchemaId with Compare::{compare, op_lt, op_le, op_gt, op_ge}

///|
pub extend SchemaId with Debug::{to_repr}

///|
pub extend SchemaColumn with Eq::{equal, not_equal}

///|
pub extend SchemaColumn with Debug::{to_repr}

///|
pub extend SchemaColumn with ToJson::{to_json}

///|
pub extend SchemaSnapshot with Eq::{equal, not_equal}

///|
pub extend SchemaSnapshot with Debug::{to_repr}

///|
pub extend Cell with Eq::{equal, not_equal}

///|
pub extend Cell with Debug::{to_repr}

///|
pub extend PgDate with Eq::{equal, not_equal}

///|
pub extend PgDate with Debug::{to_repr}

///|
pub extend PgTime with Eq::{equal, not_equal}

///|
pub extend PgTime with Debug::{to_repr}

///|
pub extend PgTimestamp with Eq::{equal, not_equal}

///|
pub extend PgTimestamp with Debug::{to_repr}

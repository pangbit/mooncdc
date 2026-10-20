///|
/// Select initial rows by source table OID. Skipped tables still reset/create an
/// empty destination, retain schemas and join WAL replication at their snapshot.
/// Changes to this policy do not recopy tables that have already handed over.
pub(all) enum TableSyncCopy {
  IncludeAllTables
  SkipAllTables
  IncludeTables(ReadOnlyArray[UInt])
  SkipTables(ReadOnlyArray[UInt])
} derive(Eq, Debug)

///|
fn TableSyncCopy::should_copy_table(self : TableSyncCopy, oid : UInt) -> Bool {
  match self {
    IncludeAllTables => true
    SkipAllTables => false
    IncludeTables(ids) => ids.contains(oid)
    SkipTables(ids) => !ids.contains(oid)
  }
}

///|
fn TableSyncCopy::validate(self : TableSyncCopy) -> Unit raise {
  match self {
    IncludeAllTables | SkipAllTables => ()
    IncludeTables(ids) | SkipTables(ids) => {
      let seen : Map[UInt, Bool] = Map([])
      for oid in ids {
        if seen.contains(oid) {
          raise InvalidConfig("duplicate table_sync_copy table OID")
        }
        seen[oid] = true
      }
    }
  }
}

///|
pub extend TableSyncCopy with Eq::{equal, not_equal}

///|
pub extend TableSyncCopy with Debug::{to_repr}

///|
/// Read-only destination input. This view is not a transaction and cannot be
/// acknowledged. A WAL fragment need not contain its source Commit boundary.
pub struct DestinationEvents {
  commit_lsn : Lsn
  changes : ReadOnlyArray[Change]
  /// Opaque delivery lifetime identity, used only to fence replay debt.
  session : String
  priv ids : ReadOnlyArray[String?]
  priv schemas : ReadOnlyArray[ReadOnlyArray[ReplicatedSchema]]
}

///|
pub fn DestinationEvents::event_id(
  self : DestinationEvents,
  index : Int32,
) -> String? {
  self.ids.get(index).bind(x => x)
}

///|
pub fn DestinationEvents::replicated_schemas(
  self : DestinationEvents,
  index : Int32,
) -> ReadOnlyArray[ReplicatedSchema] {
  self.schemas.get(index).unwrap_or([])
}

///|
pub fn Transaction::destination_events(self : Transaction) -> DestinationEvents {
  {
    commit_lsn: self.commit_lsn,
    changes: self.changes,
    session: self.session,
    ids: self.changes.mapi((i, _) => self.event_id(i)),
    schemas: self.changes.mapi((i, _) => self.replicated_schemas(i)),
  }
}

///|
pub fn WalBatch::destination_events(self : WalBatch) -> DestinationEvents {
  {
    commit_lsn: self.commit_lsn,
    changes: self.changes,
    session: self.session,
    ids: self.changes.mapi((i, _) => self.event_id(i)),
    schemas: self.changes.mapi((i, _) => self.replicated_schemas(i)),
  }
}

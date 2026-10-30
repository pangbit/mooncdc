///|
priv struct WalHeader {
  xid : UInt32
  commit_lsn : Lsn
  commit_time_us : Int64
}

///|
priv enum DecodedWalEvent {
  Begin(WalHeader)
  Change(WalHeader, Change, Int32?)
  Commit(WalHeader, Lsn)
}

///|
/// Ordered pgoutput v1 fragment. Begin already identifies a committed source
/// transaction, but only end_lsn=Some marks receipt of its validated Commit.
/// A destination may own fragments before that boundary; no fragment alone
/// authorizes a checkpoint. Replay uses the original transaction/event IDs.
pub struct WalBatch {
  id : String
  xid : UInt32
  commit_lsn : Lsn
  commit_time_us : Int64
  changes : ReadOnlyArray[Change]
  end_lsn : Lsn?
  priv session : String
  priv sequence : Int32
  priv ids : ReadOnlyArray[String?]
  priv decoded_schemas : Map[Int32, ReadOnlyArray[ReplicatedSchema]]?
} derive(Debug)

///|
pub extend WalBatch with Debug::{to_repr}

///|
pub fn WalBatch::event_id(self : WalBatch, index : Int32) -> String? {
  self.ids.get(index).bind(value => value)
}

///|
pub fn WalBatch::replicated_schemas(
  self : WalBatch,
  index : Int32,
) -> ReadOnlyArray[ReplicatedSchema] {
  self.decoded_schemas.map_or([], schemas => schemas.get(index).unwrap_or([]))
}

///|
// Used exclusively by private projection/schema/encoding helpers. The placeholder
// end is never supplied to write_transaction or used as an acknowledgement.
fn WalBatch::internal_events(self : WalBatch) -> Transaction {
  {
    id: self.id,
    xid: self.xid,
    commit_lsn: self.commit_lsn,
    commit_time_us: self.commit_time_us,
    end_lsn: self.end_lsn.unwrap_or(self.commit_lsn),
    changes: self.changes,
    session: self.session,
    sequence: self.sequence,
    projected_ids: Some(self.ids),
    decoded_schemas: self.decoded_schemas,
  }
}

///|
fn WalBatch::with_events(self : WalBatch, tx : Transaction) -> WalBatch {
  {
    ..self,
    id: tx.id,
    changes: tx.changes,
    ids: tx.projected_ids.unwrap_or([]),
    decoded_schemas: tx.decoded_schemas,
  }
}

///|
fn DecodedWalEvent::batch(
  self : DecodedWalEvent,
  source : Source,
  session : String,
  sequence : Int32,
) -> WalBatch {
  let (header, changes, end_lsn, ordinal) = match self {
    Begin(header) => (header, [], None, None)
    Change(header, change, ordinal) => (header, [change], None, ordinal)
    Commit(header, end_lsn) => (header, [], Some(end_lsn), None)
  }
  let id = "\{source.system_id}/\{source.database}/\{source.slot}/\{header.commit_lsn.to_string()}"
  let ids = changes.map(change => {
    match change {
      Message(message) => Some(id + "/message/" + message.lsn.to_string())
      _ => ordinal.map(ordinal => id + "/" + ordinal.to_string())
    }
  })
  {
    id,
    xid: header.xid,
    commit_lsn: header.commit_lsn,
    commit_time_us: header.commit_time_us,
    changes: ReadOnlyArray::from_array(changes),
    end_lsn,
    session,
    sequence,
    ids: ReadOnlyArray::from_array(ids),
    decoded_schemas: None,
  }
}

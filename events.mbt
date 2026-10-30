///|
/// Raw transfer values preserve SQL NULL and unchanged out-of-line TOAST distinctly.
pub(all) enum Value {
  Null
  Text(String)
  Binary(Bytes)
  UnchangedToast
} derive(Eq, Debug, ToJson)

///|
pub struct Column {
  name : String
  type_oid : UInt32
  type_modifier : Int32
  key : Bool
} derive(Eq, Debug, ToJson)

///|
/// Immutable metadata snapshot attached to each row change.
pub struct Relation {
  oid : UInt32
  namespace_ : String
  name : String
  replica_identity : Byte
  columns : ReadOnlyArray[Column]
} derive(Eq, Debug, ToJson)

///|
pub struct TypeInfo {
  oid : UInt32
  namespace_ : String
  name : String
} derive(Eq, Debug, ToJson)

///|
/// Arbitrary pg_logical_emit_message payload. Content is not assumed to be UTF-8.
pub struct LogicalMessage {
  transactional : Bool
  lsn : Lsn
  prefix : String
  content : Bytes
} derive(Eq, Debug, ToJson)

///|
pub(all) enum OldTuple {
  Key(ReadOnlyArray[Value])
  Full(ReadOnlyArray[Value])
} derive(Eq, Debug, ToJson)

///|
pub(all) enum Change {
  RelationChanged(Relation)
  TypeChanged(TypeInfo)
  Insert(Relation, ReadOnlyArray[Value])
  Update(Relation, OldTuple?, ReadOnlyArray[Value])
  Delete(Relation, OldTuple)
  Truncate(ReadOnlyArray[Relation], cascade~ : Bool, restart_identity~ : Bool)
  Origin(Lsn, String)
  Message(LogicalMessage)
} derive(Eq, Debug, ToJson)

///|
/// Complete committed transaction. Use `event_id` for stable row-event identity.
pub struct Transaction {
  id : String
  xid : UInt32
  commit_lsn : Lsn
  end_lsn : Lsn
  commit_time_us : Int64
  changes : ReadOnlyArray[Change]
  priv session : String
  priv sequence : Int32
  priv projected_ids : ReadOnlyArray[String?]?
  priv decoded_schemas : Map[Int32, ReadOnlyArray[ReplicatedSchema]]?
} derive(Debug)

///|
/// Exact per-event schemas resolved by a pipeline with a SchemaStore. Truncate
/// returns one schema per relation; raw subscriptions and metadata return empty.
pub fn Transaction::replicated_schemas(
  self : Transaction,
  index : Int32,
) -> ReadOnlyArray[ReplicatedSchema] {
  match self.decoded_schemas {
    Some(schemas) => schemas.get(index).unwrap_or([])
    None => []
  }
}

///|
/// Serialize the complete business transaction without ephemeral acknowledgement tokens.
pub impl ToJson for Transaction with fn to_json(self) {
  {
    "id": Json::Json(self.id),
    "xid": Json::Json(self.xid),
    "commit_lsn": Json::Json(self.commit_lsn.to_string()),
    "end_lsn": Json::Json(self.end_lsn.to_string()),
    "commit_time_us": Json::Json(self.commit_time_us),
    "changes": Json::Json(self.changes),
  }
}

///|
/// Stable identity for a data change; metadata has no replay-stable ordinal.
/// Relation/type announcements can be repeated differently after reconnect.
pub fn Transaction::event_id(self : Transaction, index : Int32) -> String? {
  if index < 0 || index >= self.changes.length() {
    return None
  }
  if self.projected_ids is Some(ids) {
    return ids[index]
  }
  let mut ordinal = 0
  for i in 0..<=index {
    match self.changes[i] {
      Message(message) =>
        if i == index {
          return Some(self.id + "/message/" + message.lsn.to_string())
        }
      Insert(_, _) | Update(_, _, _) | Delete(_, _) | Truncate(_, ..) => {
        if i == index {
          return Some(self.id + "/" + ordinal.to_string())
        }
        ordinal += 1
      }
      _ => if i == index { return None }
    }
  }
  None
}

///|
/// Position and count diagnostics; WAL lag is an estimate, not unconsumed row count.
pub struct Diagnostics {
  reconnects : Int32
  received : Lsn
  durable : Lsn
  server_end : Lsn
  outstanding_transactions : Int32
  queued_transactions : Int32
  wal_lag_bytes : UInt64
} derive(Debug, ToJson)

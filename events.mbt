///|
/// Text transfer preserves SQL NULL and unchanged out-of-line TOAST distinctly.
pub(all) enum Value {
  Null
  Text(String)
  UnchangedToast
} derive(Eq, Debug, ToJson)

///|
pub struct Column {
  name : String
  type_oid : UInt
  type_modifier : Int
  key : Bool
} derive(Eq, Debug, ToJson)

///|
/// Immutable metadata snapshot attached to each row change.
pub struct Relation {
  oid : UInt
  namespace_ : String
  name : String
  replica_identity : Byte
  columns : ReadOnlyArray[Column]
} derive(Eq, Debug, ToJson)

///|
pub struct TypeInfo {
  oid : UInt
  namespace_ : String
  name : String
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
} derive(Eq, Debug, ToJson)

///|
/// Complete committed transaction. Use `event_id` for stable row-event identity.
pub struct Transaction {
  id : String
  xid : UInt
  commit_lsn : Lsn
  end_lsn : Lsn
  commit_time_us : Int64
  changes : ReadOnlyArray[Change]
  priv session : String
  priv sequence : Int
} derive(Debug)

///|
/// Serialize the complete business transaction without ephemeral acknowledgement tokens.
pub impl ToJson for Transaction with fn to_json(self) {
  {
    "id": self.id.to_json(),
    "xid": self.xid.to_json(),
    "commit_lsn": self.commit_lsn.to_string().to_json(),
    "end_lsn": self.end_lsn.to_string().to_json(),
    "commit_time_us": self.commit_time_us.to_json(),
    "changes": self.changes.to_json(),
  }
}

///|
/// Stable identity for a data change; metadata has no replay-stable ordinal.
/// Relation/type announcements can be repeated differently after reconnect.
pub fn Transaction::event_id(self : Transaction, index : Int) -> String? {
  if index < 0 || index >= self.changes.length() {
    return None
  }
  let mut ordinal = 0
  for i in 0..<=index {
    match self.changes[i] {
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
  reconnects : Int
  received : Lsn
  durable : Lsn
  server_end : Lsn
  outstanding_transactions : Int
  queued_transactions : Int
  wal_lag_bytes : UInt64
} derive(Debug, ToJson)

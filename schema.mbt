///|
/// Schema versions sort by transaction commit, then logical-message position.
pub struct SchemaId {
  commit_lsn : Lsn
  message_lsn : Lsn
} derive(Eq, Compare, Debug)

///|
/// Inclusive schema bound for an exclusive replication checkpoint.
pub fn SchemaId::before_lsn(position : Lsn) -> SchemaId raise {
  if position.value == 0 {
    raise InvalidConfig("zero schema retention position")
  }
  {
    commit_lsn: { value: position.value - 1, },
    message_lsn: { value: 0xffffffffffffffffUL, },
  }
}

///|
pub struct SchemaColumn {
  name : String
  type_oid : UInt
  type_modifier : Int
  ordinal : Int
  nullable : Bool
  primary_key_ordinal : Int?
  default_expression : String?
} derive(Eq, Debug, ToJson)

///|
/// Full table schema; publication membership is separate from identity membership.
pub struct SchemaSnapshot {
  id : SchemaId
  oid : UInt
  namespace_ : String
  name : String
  replica_identity : Byte
  columns : ReadOnlyArray[SchemaColumn]
  priv identity_attnums : ReadOnlyArray[Int]
  priv payload : Json
} derive(Debug)

///|
pub impl Eq for SchemaSnapshot with fn equal(self, other) {
  self.id == other.id &&
  self.oid == other.oid &&
  self.namespace_ == other.namespace_ &&
  self.name == other.name &&
  self.replica_identity == other.replica_identity &&
  self.columns == other.columns &&
  self.identity_attnums == other.identity_attnums
}

///|
priv struct DdlColumn {
  attname : String
  atttypid : UInt
  atttypmod : Int
  attnum : Int
  attnotnull : Bool
  default_expression : Json?
} derive(@json.FromJson)

///|
priv struct DdlIdentity {
  primary_key_attnums : Array[Int]
  relreplident : String
  replica_identity_index_attnums : Array[Int]
} derive(@json.FromJson)

///|
priv struct DdlPayload {
  command_tag : String
  publication_name : Json?
  nspname : String
  relname : String
  oid : UInt
  identity : DdlIdentity
  columns : Array[DdlColumn]
} derive(@json.FromJson)

///|
fn ddl_optional_string(value : Json?) -> String? raise {
  match value {
    None | Some(Null) => None
    Some(String(value)) => Some(value)
    _ => raise Protocol("DDL optional field must be string or null")
  }
}

///|
fn parse_schema(payload : Json, id : SchemaId) -> SchemaSnapshot raise {
  let ddl : DdlPayload = @json.from_json(payload)
  if ddl.oid == 0U ||
    ddl.columns.is_empty() ||
    ddl.columns.length() > 1600 ||
    id.commit_lsn.value == 0 ||
    id.message_lsn > id.commit_lsn {
    raise Protocol("invalid DDL schema bounds")
  }
  ignore(sql_identifier(ddl.nspname))
  ignore(sql_identifier(ddl.relname))
  let identity = match ddl.identity.relreplident {
    "d" => b'd'
    "f" => b'f'
    "i" => b'i'
    "n" => b'n'
    _ => raise Protocol("invalid DDL replica identity")
  }
  let primary : Map[Int, Int] = Map([])
  for index, attnum in ddl.identity.primary_key_attnums {
    if attnum <= 0 || primary.contains(attnum) {
      raise Protocol("invalid DDL primary key")
    }
    primary[attnum] = index + 1
  }
  let numbers : Map[Int, Bool] = Map([])
  let names : Map[String, Bool] = Map([])
  ddl.columns.sort_by((a, b) => a.attnum.compare(b.attnum))
  let columns = ddl.columns.map(column => {
    if column.attnum <= 0 ||
      column.attnum > 32767 ||
      column.atttypid == 0U ||
      numbers.contains(column.attnum) ||
      names.contains(column.attname) {
      raise Protocol("invalid or duplicate DDL column")
    }
    ignore(sql_identifier(column.attname))
    numbers[column.attnum] = true
    names[column.attname] = true
    SchemaColumn::{
      name: column.attname,
      type_oid: column.atttypid,
      type_modifier: column.atttypmod,
      ordinal: column.attnum,
      nullable: !column.attnotnull,
      primary_key_ordinal: primary.get(column.attnum),
      default_expression: ddl_optional_string(column.default_expression),
    }
  })
  let indexes : Map[Int, Bool] = Map([])
  for attnum in ddl.identity.replica_identity_index_attnums {
    if !numbers.contains(attnum) || indexes.contains(attnum) {
      raise Protocol("invalid DDL replica identity index")
    }
    indexes[attnum] = true
  }
  for attnum in primary.keys() {
    if !numbers.contains(attnum) {
      raise Protocol("DDL primary key column missing")
    }
  }
  let identity_attnums = match identity {
    b'd' => ddl.identity.primary_key_attnums
    b'i' => ddl.identity.replica_identity_index_attnums
    b'f' => columns.map(column => column.ordinal)
    _ => []
  }
  // Retain only consumed metadata, not upstream debug fields such as current_query.
  let payload : Json = {
    "command_tag": ddl.command_tag.to_json(),
    "publication_name": match ddl_optional_string(ddl.publication_name) {
      Some(name) => name.to_json()
      None => Json::null()
    },
    "nspname": ddl.nspname.to_json(),
    "relname": ddl.relname.to_json(),
    "oid": ddl.oid.to_json(),
    "identity": {
      "primary_key_attnums": ddl.identity.primary_key_attnums.to_json(),
      "relreplident": ddl.identity.relreplident.to_json(),
      "replica_identity_index_attnums": ddl.identity.replica_identity_index_attnums.to_json(),
    },
    "columns": ddl.columns
    .map(column => {
      "attname": column.attname.to_json(),
      "atttypid": column.atttypid.to_json(),
      "atttypmod": column.atttypmod.to_json(),
      "attnum": column.attnum.to_json(),
      "attnotnull": column.attnotnull.to_json(),
      "default_expression": match
        ddl_optional_string(column.default_expression) {
        Some(value) => value.to_json()
        None => Json::null()
      },
    })
    .to_json(),
  }
  {
    id,
    oid: ddl.oid,
    namespace_: ddl.nspname,
    name: ddl.relname,
    replica_identity: identity,
    columns: ReadOnlyArray::from_array(columns),
    identity_attnums: ReadOnlyArray::from_array(identity_attnums),
    payload,
  }
}

///|
/// Interpret the pinned Supabase ETL DDL payload. Unknown fields are ignored.
/// Other prefixes/publications return None; malformed or nontransactional DDL fails.
pub fn LogicalMessage::schema_snapshot(
  self : LogicalMessage,
  commit_lsn~ : Lsn,
  publication~ : String,
) -> SchemaSnapshot? raise {
  if self.prefix != "supabase_etl_ddl" {
    return None
  }
  if !self.transactional {
    raise Protocol("DDL message must be transactional")
  }
  try {
    let payload = @json.parse(@utf8.decode(self.content))
    let ddl : DdlPayload = @json.from_json(payload)
    if ddl.command_tag == "ALTER PUBLICATION" &&
      ddl_optional_string(ddl.publication_name) != Some(publication) {
      return None
    }
    Some(parse_schema(payload, { commit_lsn, message_lsn: self.lsn, }))
  } catch {
    e => raise Protocol("invalid DDL schema: \{e}")
  }
}

///|
/// Rebuild the published view using the last pgoutput column membership mask.
/// A fresh protocol Relation supersedes this fallback after publication changes.
pub fn SchemaSnapshot::relation(
  self : SchemaSnapshot,
  published : ReadOnlyArray[String],
) -> Relation raise {
  let selected : Map[String, Bool] = Map([])
  for name in published {
    if selected.contains(name) {
      raise Protocol("duplicate publication column")
    }
    selected[name] = true
  }
  let columns = self.columns.filter_map(column => {
    if selected.contains(column.name) {
      Some(Column::{
        name: column.name,
        type_oid: column.type_oid,
        type_modifier: column.type_modifier,
        key: self.identity_attnums.contains(column.ordinal),
      })
    } else {
      None
    }
  })
  if columns.length() != published.length() {
    raise Protocol("publication column missing from schema")
  }
  {
    oid: self.oid,
    namespace_: self.namespace_,
    name: self.name,
    replica_identity: self.replica_identity,
    columns,
  }
}

///|
// Called on the imported-snapshot reader after its ACCESS SHARE lock is held.
async fn Connection::snapshot_schema(
  self : Connection,
  oid : UInt,
  position : Lsn,
  timeout : Int,
  max_bytes : Int,
) -> SchemaSnapshot {
  let query =
    #|WITH key_columns AS (
    #| SELECT con.conrelid, jsonb_agg(k.attnum ORDER BY k.pos) AS attnums
    #| FROM pg_catalog.pg_constraint con
    #| CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY k(attnum,pos)
    #| WHERE con.contype='p' GROUP BY con.conrelid
    #|)
    #|SELECT jsonb_build_object(
    #| 'command_tag','SNAPSHOT','nspname',n.nspname,'relname',c.relname,'oid',c.oid::bigint,
    #| 'identity',jsonb_build_object(
    #|   'relreplident',c.relreplident::text,
    #|   'primary_key_attnums',coalesce(
    #|     (SELECT attnums FROM key_columns WHERE conrelid=c.oid),
    #|     (SELECT k.attnums FROM pg_catalog.pg_inherits i JOIN key_columns k ON k.conrelid=i.inhparent WHERE i.inhrelid=c.oid ORDER BY i.inhseqno LIMIT 1),
    #|     '[]'::jsonb),
    #|   'replica_identity_index_attnums',coalesce(
    #|     (SELECT jsonb_agg(k.attnum ORDER BY k.pos) FROM pg_catalog.pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,pos) WHERE i.indrelid=c.oid AND i.indisreplident AND k.pos<=i.indnkeyatts),'[]'::jsonb)),
    #| 'columns',(SELECT jsonb_agg(jsonb_build_object(
    #|   'attname',a.attname,'atttypid',a.atttypid::bigint,'atttypmod',a.atttypmod,'attnum',a.attnum,
    #|   'attnotnull',a.attnotnull,'default_expression',pg_catalog.pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum)
    #|   FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    #|   WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated=''))::text
    #|FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
  let rows = self.snapshot_query(
    query + " WHERE c.oid=\{oid}",
    timeout,
    max_bytes,
  )
  parse_schema(@json.parse(cell(rows, 0, 0)), {
    commit_lsn: position,
    message_lsn: { value: 0, },
  })
}

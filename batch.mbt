///|
/// Advisory decoded-batch budget. One indivisible item can exceed the target.
pub struct BatchConfig {
  memory_budget_ratio : Float
  max_bytes : UInt64
  max_fill_ms : Int
}

///|
pub fn BatchConfig::new(
  memory_budget_ratio? : Float = 0.2F,
  max_bytes? : UInt64 = 33554432UL,
  max_fill_ms? : Int = 10000,
) -> BatchConfig {
  { memory_budget_ratio, max_bytes, max_fill_ms, }
}

///|
fn BatchConfig::validate(self : BatchConfig) -> Unit raise {
  if !@memory.valid_budget(self.memory_budget_ratio, self.max_bytes) ||
    self.max_fill_ms < 0 {
    raise InvalidConfig("invalid batch memory budget or fill interval")
  }
}

///|
priv struct BatchRuntime {
  governor : @memory.Governor
  monitor : @memory.Monitor
  max_fill_ms : Int
}

///|
fn BatchRuntime::new(
  monitor : @memory.Monitor,
  config : BatchConfig,
) -> BatchRuntime raise {
  {
    monitor,
    governor: @memory.Governor::new(
      monitor,
      config.memory_budget_ratio,
      config.max_bytes,
    ),
    max_fill_ms: config.max_fill_ms,
  }
}

///|
fn batch_add(a : UInt64, b : UInt64) -> UInt64 {
  if 0xffffffffffffffffUL - a < b {
    0xffffffffffffffffUL
  } else {
    a + b
  }
}

///|
// Native MoonBit text is UTF-16. These are allocation hints, not retained heap
// measurements; shared metadata may be counted more than once.
fn row_size_hint(row : ReadOnlyArray[Value]) -> UInt64 {
  let mut size = 16UL + row.length().to_uint64() * 8UL
  for value in row {
    size = batch_add(
      size,
      match value {
        Text(text) => 16UL + text.length().to_uint64() * 2UL
        Binary(bytes) => 16UL + bytes.length().to_uint64()
        Null | UnchangedToast => 0UL
      },
    )
  }
  size
}

///|
fn relation_size_hint(relation : Relation) -> UInt64 {
  let mut size = 64UL +
    (
      relation.name.length().to_uint64() +
      relation.namespace_.length().to_uint64()
    ) *
    2UL
  for column in relation.columns {
    size = batch_add(size, 32UL + column.name.length().to_uint64() * 2UL)
  }
  size
}

///|
fn old_size_hint(old : OldTuple) -> UInt64 {
  match old {
    Key(row) | Full(row) => row_size_hint(row)
  }
}

///|
fn transaction_size_hint(tx : Transaction) -> UInt64 {
  let mut size = 96UL + tx.id.length().to_uint64() * 2UL
  for change in tx.changes {
    let (relation, value) = match change {
      Insert(relation, row) => (Some(relation), row_size_hint(row))
      Update(relation, old, row) =>
        (
          Some(relation),
          batch_add(row_size_hint(row), old.map_or(0UL, old_size_hint)),
        )
      Delete(relation, old) => (Some(relation), old_size_hint(old))
      RelationChanged(relation) => (Some(relation), 0UL)
      TypeChanged(info) =>
        (
          None,
          32UL +
          (
            info.namespace_.length().to_uint64() +
            info.name.length().to_uint64()
          ) *
          2UL,
        )
      Truncate(relations, ..) => {
        let mut bytes = 0UL
        for relation in relations {
          bytes = batch_add(bytes, relation_size_hint(relation))
        }
        (None, bytes)
      }
      Origin(_, name) => (None, 16UL + name.length().to_uint64() * 2UL)
      Message(message) =>
        (
          None,
          48UL +
          message.prefix.length().to_uint64() * 2UL +
          message.content.length().to_uint64(),
        )
    }
    size = batch_add(size, batch_add(16UL, value))
    if relation is Some(relation) {
      size = batch_add(size, relation_size_hint(relation))
    }
  }
  size
}

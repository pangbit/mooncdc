///|
// Only called after acknowledgement saved its checkpoint. A failed best-effort
// cleanup retains candidates and never changes the checkpoint or table lifecycle.
async fn PipelineDestination::cleanup_schemas(
  self : PipelineDestination,
) -> Unit {
  guard self.schema_store is Some(schemas) else { return }
  if self.cleanup_candidates.is_empty() {
    return
  }
  self.mutex.acquire()
  defer self.mutex.release()
  let metrics = self.control.metrics_data
  let labels = "{worker_type=\"apply\"}"
  metrics.increment("mooncdc_schema_cleanup_attempts_total" + labels)
  try {
    let checkpoint = stored_checkpoint(
      self.store.path + ".checkpoint",
      self.store.state.source,
      self.store.session,
    )
    let replay_bound = SchemaId::before_lsn(checkpoint)
    if self.decoding is Some(decoder) &&
      self.store.state.tables.any(table => {
        table.decoding is Some(_) &&
        table.phase is (Ready(lsn) | Catchup(lsn)) &&
        Lsn::parse(lsn) <= checkpoint &&
        decoder.tables.get(table.oid) is Some(Complete(_))
      }) {
      self.store.update(state => {
        ..state,
        tables: state.tables.map(table => {
          if table.decoding is Some(_) &&
            table.phase is (Ready(lsn) | Catchup(lsn)) &&
            Lsn::parse(lsn) <= checkpoint &&
            decoder.tables.get(table.oid) is Some(Complete(_)) {
            { ..table, decoding: None, }
          } else {
            table
          }
        }),
      })
    }
    let bounds : Array[(UInt, SchemaId)] = []
    let evaluated : Array[UInt] = []
    for table in self.store.state.tables {
      if !self.cleanup_candidates.contains(table.oid) {
        continue
      }
      let owned = match table.phase {
        Ready(lsn) | Catchup(lsn) => Lsn::parse(lsn) <= checkpoint
        _ => false
      }
      if !owned {
        continue
      }
      if self.destination.schema_retention(table.oid) is Some(retention) {
        let mut bound = if retention < replay_bound {
          retention
        } else {
          replay_bound
        }
        if table.decoding is Some(decoding) && decoding.snapshot_id() < bound {
          bound = decoding.snapshot_id()
        }
        bounds.push((table.oid, bound))
      }
      evaluated.push(table.oid)
    }
    if !bounds.is_empty() {
      let removed = schemas.prune(ReadOnlyArray::from_array(bounds))
      metrics.increment("mooncdc_schema_cleanups_total" + labels)
      metrics.increment(
        "mooncdc_schema_cleanup_tables_total" + labels,
        amount=bounds.length().to_uint64(),
      )
      if removed > 0 {
        metrics.increment(
          "mooncdc_schema_cleanup_pruned_versions_total" + labels,
          amount=removed.to_uint64(),
        )
      }
    }
    let skipped = evaluated.length() - bounds.length()
    if skipped > 0 {
      metrics.increment(
        "mooncdc_schema_cleanup_skipped_tables_total" + labels,
        amount=skipped.to_uint64(),
      )
    }
    for oid in evaluated {
      self.cleanup_candidates.remove(oid)
    }
  } catch {
    error => {
      if @async.is_being_cancelled() {
        raise error
      }
      metrics.increment("mooncdc_schema_cleanup_errors_total" + labels)
    }
  }
}

///|
priv struct MetricHistogram {
  mut count : UInt64
  mut sum : Double
  bounds : Array[Double]
  buckets : Array[UInt64]
}

///|
fn metric_buckets() -> Array[Double] {
  [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
}

///|
priv struct RuntimeMetrics {
  counters : Map[String, UInt64]
  histograms : Map[String, MetricHistogram]
  subscriptions : Map[UInt64, (String, () -> Diagnostics)]
  copy_lags : Map[String, UInt64]
  source_monitors : Map[UInt64, SourceObservation]
  mut pipeline : PipelineStore?
  mut memory : @memory.Monitor?
  mut batch : BatchRuntime?
}

///|
fn RuntimeMetrics::new() -> RuntimeMetrics {
  {
    counters: Map([]),
    histograms: Map([]),
    subscriptions: Map([]),
    copy_lags: Map([]),
    source_monitors: Map([]),
    pipeline: None,
    memory: None,
    batch: None,
  }
}

///|
fn RuntimeMetrics::increment(
  self : RuntimeMetrics,
  key : String,
  amount? : UInt64 = 1UL,
) -> Unit {
  self.counters[key] = batch_add(self.counters.get(key).unwrap_or(0UL), amount)
}

///|
fn RuntimeMetrics::duration(
  self : RuntimeMetrics,
  key : String,
  started : @bench.Timestamp,
) -> Unit {
  let sample = (@bench.monotonic_clock_end(started) / 1000000.0).max(0.0)
  self.sample(key, sample, metric_buckets())
}

///|
fn RuntimeMetrics::sample(
  self : RuntimeMetrics,
  key : String,
  sample : Double,
  bounds : Array[Double],
) -> Unit {
  let histogram = self.histograms.get_or_init(key, () => {
    count: 0UL,
    sum: 0.0,
    bounds: bounds.copy(),
    buckets: Array::make(bounds.length(), 0UL),
  })
  histogram.count = batch_add(histogram.count, 1UL)
  histogram.sum += sample
  for i, bound in histogram.bounds {
    if sample <= bound {
      histogram.buckets[i] = batch_add(histogram.buckets[i], 1UL)
    }
  }
}

///|
fn metric_escape(value : String) -> String {
  value
  .replace_all(old="\\", new="\\\\")
  .replace_all(old="\n", new="\\n")
  .replace_all(old="\"", new="\\\"")
}

///|
fn metric_sample(name : String, labels : String, value : String) -> String {
  if labels.is_empty() {
    name + " " + value + "\n"
  } else {
    name + "{" + labels + "} " + value + "\n"
  }
}

///|
fn metric_labels(a : String, b : String) -> String {
  if a.is_empty() {
    b
  } else if b.is_empty() {
    a
  } else {
    a + "," + b
  }
}

///|
fn metric_key_parts(key : String) -> (String, String) {
  match key.find("{") {
    Some(offset) =>
      (key[:offset].to_owned(), key[offset + 1:key.length() - 1].to_owned())
    None => (key, "")
  }
}

///|
fn ActivityHandle::metric_worker(self : ActivityHandle) -> String {
  "worker_type=\"\{self.worker_type}\""
}

///|
/// Render this control's bounded Prometheus counters, histograms and live gauges.
/// Counters include retries/replays and are not business exactly-once counts.
/// Labels never include connection strings, relation names or error messages.
pub fn ApplyControl::prometheus(
  self : ApplyControl,
  pipeline_id? : String,
  destination? : String,
  project? : String,
  stall_timeout_ms? : Int = 300000,
) -> String raise {
  let labels : Array[String] = []
  for
    (key, value) in [
      ("pipeline_id", pipeline_id),
      ("destination", destination),
      ("project", project),
    ] {
    if value is Some(value) {
      if @utf8.encode(value).length() > 1024 {
        raise InvalidConfig("metrics label exceeds 1024 UTF-8 bytes")
      }
      labels.push("\{key}=\"\{metric_escape(value)}\"")
    }
  }
  let global = labels.join(",")
  let metrics = self.metrics_data
  let out : Array[String] = []
  let described : Map[String, Bool] = Map([])
  let emit = fn(name : String, kind : String, labels : String, value : String) {
    if !described.contains(name) {
      out.push("# TYPE \{name} \{kind}\n")
      described[name] = true
    }
    out.push(metric_sample(name, metric_labels(global, labels), value))
  }
  let health = self.health(stall_timeout_ms~)
  emit("mooncdc_live", "gauge", "", if health.live { "1" } else { "0" })
  emit("mooncdc_ready", "gauge", "", if health.ready { "1" } else { "0" })
  for lifecycle in ["initializing", "running", "stopping"] {
    let active = match self.lifecycle {
      Initializing => lifecycle == "initializing"
      Running => lifecycle == "running"
      Stopping => lifecycle == "stopping"
    }
    emit(
      "mooncdc_lifecycle",
      "gauge",
      "state=\"\{lifecycle}\"",
      if active {
        "1"
      } else {
        "0"
      },
    )
  }
  for worker in ["apply", "table_sync"] {
    let mut active = 0UL
    let mut pending = 0UL
    let mut queued = 0UL
    let mut lag = 0UL
    for _, (kind, read) in metrics.subscriptions {
      if kind == worker {
        let diagnostic = read()
        active += 1UL
        pending += diagnostic.outstanding_transactions.to_uint64()
        queued += diagnostic.queued_transactions.to_uint64()
        if diagnostic.wal_lag_bytes > lag {
          lag = diagnostic.wal_lag_bytes
        }
      }
    }
    let labels = "worker_type=\"\{worker}\""
    emit("mooncdc_subscriptions_active", "gauge", labels, active.to_string())
    emit(
      "mooncdc_outstanding_transactions",
      "gauge",
      labels,
      pending.to_string(),
    )
    emit("mooncdc_queued_transactions", "gauge", labels, queued.to_string())
    if active > 0UL {
      emit("mooncdc_observed_wal_lag_bytes", "gauge", labels, lag.to_string())
    }
  }
  for worker in ["apply", "table_sync"] {
    let mut monitors = 0
    let mut received_lag = 0UL
    let mut durable_lag = 0UL
    let mut gap = 0UL
    let mut age = 0.0
    let mut samples = 0
    for id, observation in metrics.source_monitors {
      if self.activity_registry.contains(id) {
        if observation.worker_type == worker {
          monitors += 1
          if observation.current is Some(current) &&
            observation.sampled is Some(sampled) {
            samples += 1
            let (received, durable) = (observation.progress)()
            if current > received &&
              current.value - received.value > received_lag {
              received_lag = current.value - received.value
            }
            if current > durable && current.value - durable.value > durable_lag {
              durable_lag = current.value - durable.value
            }
            if received > durable && received.value - durable.value > gap {
              gap = received.value - durable.value
            }
            age = age.max(@bench.monotonic_clock_end(sampled) / 1000000.0)
          }
        }
      }
    }
    let labels = "worker_type=\"\{worker}\""
    emit(
      "mooncdc_source_lsn_monitors_active",
      "gauge",
      labels,
      monitors.to_string(),
    )
    if samples > 0 {
      emit(
        "mooncdc_current_wal_received_lag_bytes",
        "gauge",
        labels,
        received_lag.to_string(),
      )
      emit(
        "mooncdc_current_wal_durable_lag_bytes",
        "gauge",
        labels,
        durable_lag.to_string(),
      )
      emit(
        "mooncdc_received_durable_gap_bytes",
        "gauge",
        labels,
        gap.to_string(),
      )
      emit(
        "mooncdc_source_lsn_sample_age_seconds",
        "gauge",
        labels,
        age.to_string(),
      )
    }
  }
  if metrics.pipeline is Some(store) {
    for phase in ["pending", "copying", "catchup", "ready", "errored"] {
      let count = store.state.tables
        .filter(table => {
          match table.phase {
            Pending => phase == "pending"
            Copying => phase == "copying"
            Catchup(_) => phase == "catchup"
            Ready(_) => phase == "ready"
            Errored(_, _, _) => phase == "errored"
          }
        })
        .length()
      emit("mooncdc_tables", "gauge", "state=\"\{phase}\"", count.to_string())
    }
  }
  if !metrics.copy_lags.is_empty() {
    let mut lag = 0UL
    for _, value in metrics.copy_lags {
      if value > lag {
        lag = value
      }
    }
    emit(
      "mooncdc_table_copy_current_wal_lag_bytes",
      "gauge",
      "",
      lag.to_string(),
    )
    emit(
      "mooncdc_table_copy_monitors_active",
      "gauge",
      "",
      metrics.copy_lags.length().to_string(),
    )
  }
  if metrics.memory is Some(monitor) {
    let sample = monitor.current()
    let source = match sample.source {
      System => "system"
      ProcessCgroup => "process_cgroup"
      RootCgroup => "root_cgroup"
    }
    let labels = "source=\"\{source}\""
    emit("mooncdc_memory_used_bytes", "gauge", labels, sample.used.to_string())
    emit(
      "mooncdc_memory_total_bytes",
      "gauge",
      labels,
      sample.total.to_string(),
    )
    emit(
      "mooncdc_memory_backpressure_active",
      "gauge",
      "",
      if monitor.is_paused() {
        "1"
      } else {
        "0"
      },
    )
    let (activate, resume_count) = monitor.transitions()
    emit(
      "mooncdc_memory_backpressure_transitions_total",
      "counter",
      "direction=\"activate\"",
      activate.to_string(),
    )
    emit(
      "mooncdc_memory_backpressure_transitions_total",
      "counter",
      "direction=\"resume\"",
      resume_count.to_string(),
    )
  }
  if metrics.batch is Some(batch) {
    emit(
      "mooncdc_batch_target_bytes",
      "gauge",
      "",
      batch.governor.target_bytes().to_string(),
    )
    emit(
      "mooncdc_batch_registered_slots",
      "gauge",
      "",
      batch.governor.registered_slots().to_string(),
    )
  }
  let keys = metrics.counters.keys().to_array()
  keys.sort()
  for key in keys {
    let (name, labels) = metric_key_parts(key)
    emit(name, "counter", labels, metrics.counters[key].to_string())
  }
  let keys = metrics.histograms.keys().to_array()
  keys.sort()
  for key in keys {
    let (name, labels) = metric_key_parts(key)
    if !described.contains(name) {
      out.push("# TYPE \{name} histogram\n")
      described[name] = true
    }
    let histogram = metrics.histograms[key]
    let labels = metric_labels(global, labels)
    for i, bound in histogram.bounds {
      out.push(
        metric_sample(
          name + "_bucket",
          metric_labels(labels, "le=\"\{bound}\""),
          histogram.buckets[i].to_string(),
        ),
      )
    }
    out.push(
      metric_sample(
        name + "_bucket",
        metric_labels(labels, "le=\"+Inf\""),
        histogram.count.to_string(),
      ),
    )
    out.push(metric_sample(name + "_sum", labels, histogram.sum.to_string()))
    out.push(
      metric_sample(name + "_count", labels, histogram.count.to_string()),
    )
  }
  out.join("")
}

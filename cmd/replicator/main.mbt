///|
priv struct ValidationSchemas {}

///|
// Called only after the scoped async operation has unwound/joined every child.
// libc exit flushes stdio; bypassing async's final signal re-raise gives service
// supervisors the outcome of draining instead of the initiating signal.
extern "C" fn finish_process(code : Int32) -> Unit = "exit"

///|
impl @cdc.SchemaStore for ValidationSchemas with fn get(_, _, _) {
  fail("configuration validation performed schema IO")
}

///|
impl @cdc.SchemaStore for ValidationSchemas with fn all(_) {
  fail("configuration validation performed schema IO")
}

///|
impl @cdc.SchemaStore for ValidationSchemas with fn put(_, _) {
  fail("configuration validation performed schema IO")
}

///|
impl @cdc.SchemaStore for ValidationSchemas with fn prune(_, _) {
  fail("configuration validation performed schema IO")
}

///|
fn ReplicatorConfig::store(
  self : ReplicatorConfig,
  suffix : String,
  control? : @cdc.ApplyControl,
) -> @cdc.PostgresStateStore raise {
  @cdc.PostgresStateStore::new(
    self.state,
    store_id=self.id + suffix,
    schema=self.state_schema,
    table=self.state_table,
    control?,
  )
}

///|
fn ReplicatorConfig::destination(
  self : ReplicatorConfig,
  schemas : &@cdc.SchemaStore,
) -> @cdc.ClickHouseDestination raise {
  @cdc.ClickHouseDestination::new(
    endpoint=self.endpoint,
    user=self.user,
    password=self.password,
    database=self.database,
    publication=self.publication,
    schema_store=schemas,
    engine=self.engine,
  )
}

///|
fn ReplicatorConfig::identity(self : ReplicatorConfig) -> String {
  // JSON framing prevents delimiter collisions; credentials are never persisted.
  Json::Json([
    self.source.host,
    self.source.port.to_string(),
    self.source.database,
    self.publication,
    self.endpoint,
    self.database,
    if self.engine == @cdc.MergeTree {
      "merge_tree"
    } else {
      "replacing_merge_tree"
    },
  ]).stringify()
}

///|
fn ReplicatorConfig::active_reports(self : ReplicatorConfig) -> ErrorReports {
  {
    ..self.reports,
    secrets: self.reports.secrets +
    [self.source.password, self.state.password, self.password],
  }
}

///|
fn ReplicatorConfig::pipeline_store(
  self : ReplicatorConfig,
  control : @cdc.ApplyControl,
) -> &@cdc.StateStore raise {
  let inner : &@cdc.StateStore = self.store("/state", control~)
  if self.reports.sentry is None && self.reports.notification is None {
    inner
  } else {
    ReportingStore::{
      inner,
      reports: self.active_reports(),
      known: Map([]),
      observation_ready: false,
    }
  }
}

///|
async fn ReplicatorConfig::pipeline(
  self : ReplicatorConfig,
  target : &@cdc.Destination,
  control : @cdc.ApplyControl,
  schemas : &@cdc.SchemaStore,
) -> Unit {
  @cdc.run_pipeline(
    self.source,
    target,
    publication=self.publication,
    destination_id=self.identity(),
    control~,
    state_store=self.pipeline_store(control),
    schema_store=schemas,
    copy_concurrency=self.copy_concurrency,
    copy_connections_per_table=self.copy_connections_per_table,
    table_sync_monitor_refresh_interval_ms=self.copy_monitor_interval_ms,
    invalidated_slot_behavior=self.invalidated_slot_behavior,
    batch_rows=self.batch_rows,
    table_retry_delay_ms=self.retry_delay_ms,
    max_table_retries=self.retries,
    table_sync_copy=self.copy_policy,
    slot_failover=self.slot_failover,
    memory_backpressure?=self.memory_backpressure,
    batch?=self.batch,
    memory_refresh_interval_ms=self.memory_refresh_interval_ms,
  )
}

///|
async fn ReplicatorConfig::validate(self : ReplicatorConfig) -> Unit {
  let control = @cdc.ApplyControl()
  control.stop()
  let schemas = ValidationSchemas::{ }
  ignore(self.store("/schemas"))
  self.pipeline(self.destination(schemas), control, schemas)
  if self.metrics_port is Some(_) {
    ignore(
      control.prometheus(
        pipeline_id=self.id,
        destination="clickhouse",
        project?=self.reports.project,
      ),
    )
  }
}

///|
fn ReplicatorConfig::metrics_settings(
  self : ReplicatorConfig,
) -> (Int32, String?)? {
  self.metrics_port.map(port => (port, self.metrics_listen_address))
}

///|
async fn ReplicatorConfig::run(
  self : ReplicatorConfig,
  control : @cdc.ApplyControl,
) -> Unit {
  let run : async () -> Unit = () => {
    // Source helpers must exist before acquiring a state store in schema etl.
    if self.source_migrations {
      @cdc.run_source_migrations(self.source, control~)
    }
    if control.health().status == "stopping" {
      return
    }
    @cdc.with_state_schema_store(
      self.store("/schemas", control~),
      identity=self.identity(),
      schemas => self.pipeline(self.destination(schemas), control, schemas),
    )
  }
  let with_health = () => {
    match self.health {
      None => run()
      Some(health) =>
        @cdc.with_health_server(
          control,
          run,
          port=health.port,
          stall_timeout_ms=health.stall_ms,
        )
    }
  }
  match self.metrics_settings() {
    None => with_configcat(self.configcat, with_health)
    Some((port, listen_address)) =>
      @cdc.with_metrics_server(
        control,
        () => with_configcat(self.configcat, with_health),
        port~,
        listen_address?,
        pipeline_id=self.id,
        destination="clickhouse",
        project?=self.reports.project,
        stall_timeout_ms=self.health.map_or(300000, health => health.stall_ms),
      )
  }
}

///|
async fn drain_on_cancel(
  control : @cdc.ApplyControl,
  run : async () -> Unit,
) -> Unit {
  @async.with_task_group(group => {
    let worker = group.spawn(() => @async.protect_from_cancel(run))
    match @async.handle_cancellation(() => worker.wait()) {
      Some(_) => ()
      None => control.stop()
      // The group joins its protected child, including errors from draining.
      // Waiting on the cancelled task again would lose that result to
      // WaitedTaskAlreadyCancelled after its protected body completes.
    }
  })
}

///|
async fn execute() -> Unit {
  @signal.set_global_cancellation_signals([@signal.SIGINT, @signal.SIGTERM])
  let args = @env.args()
  let mut path = None
  let mut check = false
  let mut i = 1
  while i < args.length() {
    match args[i] {
      "--help" => {
        println(
          "Usage: replicator [--config path.json|path.yaml|path.yml] [--check]\nWithout --config: configuration/base + {APP_ENVIRONMENT:-prod}, each with yaml > yml > json priority.\nAPP_ overrides nested keys with __. SIGINT/SIGTERM request a graceful drain.",
        )
        return
      }
      "--config" if path is None && i + 1 < args.length() => {
        path = Some(args[i + 1])
        i += 1
      }
      "--check" if !check => check = true
      _ => raise ConfigError("invalid arguments; use --help")
    }
    i += 1
  }
  let config = load_config(path, @env.get_env_vars())
  config.validate() catch {
    error => {
      eprintln(config.active_reports().redact(failure_message(error)))
      finish_process(1)
      return
    }
  }
  if check {
    println("Configuration valid (connectivity not checked)")
    return
  }
  let control = @cdc.ApplyControl()
  let result = @async.handle_cancellation(() => {
    drain_on_cancel(control, () => config.run(control))
  }) catch {
    error => {
      let reports = config.active_reports()
      let message = reports.redact(failure_message(error))
      eprintln(message)
      @async.protect_from_cancel(() => reports.report(message, None))
      finish_process(1)
      return
    }
  }
  if result is None {
    finish_process(0)
  }
}

///|
fn failure_message(error : Error) -> String {
  match error {
    ConfigError(message) | @cdc.InvalidConfig(message) =>
      "Configuration: " + message
    @cdc.Server(code, message) => "PostgreSQL [\{code}]: " + message
    @cdc.Authentication(message) => "Authentication: " + message
    @cdc.Protocol(message) => "Protocol: " + message
    @cdc.Conversion(message) => "Conversion: " + message
    @cdc.LimitExceeded(message) => "Limit exceeded: " + message
    @cdc.UnsafeResume(message) => "Unsafe resume: " + message
    @cdc.CheckpointError(message) => "State: " + message
    @cdc.Retryable(message) => "Retryable destination error: " + message
    @cdc.Manual(message) => "Destination repair required: " + message
    @cdc.Permanent(message) => "Permanent destination error: " + message
    @cdc.Closed => "CDC resource is closed"
    _ => error.to_string()
  }
}

///|
async fn main {
  execute() catch {
    error => {
      eprintln(failure_message(error))
      finish_process(1)
    }
  }
}

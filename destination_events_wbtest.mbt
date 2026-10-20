///|
test "destination views preserve fragment identities without inventing a commit" {
  let decoder = Decoder::new(4096, 100, 10)
  ignore(decoder.decode_event(begin_fixture()))
  let source = pipeline_fixture().source
  ignore(decoder.decode_event(relation_fixture("items")))
  let batch = decoder
    .decode_event(row_fixture_for_wal())
    .batch(source, "first", 1)
  assert_true(batch.end_lsn is None)
  let view = batch.destination_events()
  assert_eq(view.commit_lsn, batch.commit_lsn)
  assert_eq(view.changes, batch.changes)
  assert_eq(view.session, "first")
  assert_eq(view.event_id(0), batch.event_id(0))
  assert_eq(view.event_id(1), None)
  assert_eq(view.replicated_schemas(0), batch.replicated_schemas(0))
  let replay = { ..batch, session: "second", }
  assert_eq(replay.destination_events().event_id(0), view.event_id(0))
  assert_eq(replay.destination_events().session, "second")
  let tx = batch.internal_events()
  let committed = tx.destination_events()
  assert_eq(committed.changes, view.changes)
  assert_eq(committed.event_id(0), view.event_id(0))
  assert_true(batch.end_lsn is None)
}

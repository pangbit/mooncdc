///|
async test "event apply preserves adjacent commit boundaries during consumption and resume" {
  for restarting in [false, true] {
    for status in [WriteStatus::Accepted, Durable] {
      @async.with_timeout(5000, () => {
        let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
        defer server.close()
        let dir = @fs.tmpdir(prefix="mooncdc-wal-boundary-")
        defer @fs.rmdir(dir, recursive=true)
        let path = dir + "/checkpoint"
        let source = wal_test_source()
        let initial = Lsn::parse(if restarting { "0/65" } else { "0/10" })
        if restarting {
          save_checkpoint(path, source, initial)
        }
        let config = ConnectionConfig::new(
          host="127.0.0.1",
          port=server.addr.port(),
          user="user",
          database="db",
          security=LocalPlaintext,
          timeout_ms=1000,
        )
        @memory.with_sampler(
          2,
          None,
          _ => {
            snapshot: {
              used: 0UL,
              total: 1000000000UL,
              source: @memory.System,
            },
            fresh: true,
          },
          monitor => {
            let batch = BatchRuntime::new(monitor, BatchConfig::new())
            let target : WalBoundaryDestination = {
              status,
              batches: [],
              flushes: 0,
            }
            let control = ApplyControl()
            let checkpoints : Array[UInt64] = []
            @async.with_task_group(group => {
              let peer = group.spawn(() => {
                let (tcp, _) = server.accept()
                defer tcp.close()
                mock_replication(tcp, "success", stream=tcp => {
                  // Abbreviated LSNs: commit 100 ends at 101, where the next
                  // commit starts. A later commit makes a skipped middle
                  // transaction observable even if the checkpoint advances.
                  // Repeated 100/101 commits must still be filtered; Resume
                  // also starts with both 100 commits behind its checkpoint.
                  for lsn in [100UL, 100UL, 101UL, 101UL, 200UL] {
                    for
                      data in [
                        begin_fixture(lsn~),
                        relation_fixture("items"),
                        row_fixture_for_wal(),
                        commit_fixture(lsn~),
                      ] {
                      wal_send(tcp, data)
                    }
                  }
                  while tcp.read_some() is Some(_) {
                    ()
                  }
                })
              })
              subscribe_impl(
                config,
                slot="slot",
                publication="pub",
                start=if restarting { Resume } else { At(initial) },
                checkpoint_path=path,
                worker_retry=true,
                wal_events=true,
                heartbeat_ms=10000,
                memory=monitor,
                batch_governor=batch.governor,
                sub => {
                  assert_eq(sub.diagnostics().durable, initial)
                  sub.apply_events(target, control~, batch~, on_checkpoint=() => {
                    let saved = load_checkpoint(path, source)
                    checkpoints.push(saved.value)
                    assert_eq(sub.diagnostics().durable, saved)
                    assert_eq(
                      target.flushes,
                      if status == Accepted {
                        checkpoints.length()
                      } else {
                        0
                      },
                    )
                    if saved.value == 201UL {
                      control.stop()
                    }
                  })
                  assert_eq(sub.diagnostics().outstanding_transactions, 0)
                },
              )
              peer.wait()
            })
            assert_eq(
              target.batches.map(batch => batch.commit_lsn.value),
              if restarting {
                [101UL, 200UL]
              } else {
                [100UL, 101UL, 200UL]
              },
            )
            assert_eq(
              checkpoints,
              if restarting {
                [102UL, 201UL]
              } else {
                [101UL, 102UL, 201UL]
              },
            )
            for fragment in target.batches {
              assert_true(
                fragment.changes is [RelationChanged(_), Insert(_, _)],
              )
              assert_eq(
                fragment.end_lsn.map(lsn => lsn.value),
                Some(fragment.commit_lsn.value + 1),
              )
              assert_eq(fragment.event_id(1), Some(fragment.id + "/0"))
            }
            assert_eq(load_checkpoint(path, source).value, 201UL)
          },
        )
      })
    }
  }
}

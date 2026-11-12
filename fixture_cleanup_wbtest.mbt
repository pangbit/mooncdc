///|
// A failed body keeps its original error; cleanup failures still fail a body
// that succeeded. Each owned fixture gets a separate finite cleanup deadline.
async fn finish_test_cleanup(
  failed : Ref[Bool],
  cleanup : async () -> Unit,
) -> Unit nocancel {
  @async.protect_from_cancel(() => {
    @async.with_timeout(10000, cleanup) catch {
      error =>
        if failed.val {
          println("Fixture cleanup failed after the test body: \{error}")
        } else {
          raise error
        }
    }
  })
}

///|
async test "fixture cleanup deadline leaves remaining cleanup runnable" {
  let first_finished = Ref(false)
  let second_finished = Ref(false)
  let start = @env.now()
  try
    @async.with_timeout(20, () => {
      let failed = Ref(false)
      defer finish_test_cleanup(failed, () => {
        @async.sleep(10)
        second_finished.val = true
      })
      defer finish_test_cleanup(failed, () => {
        defer {
          first_finished.val = true
        }
        @async.sleep(30000)
      })
      errdefer {
        failed.val = true
      }
      @async.sleep(30000)
    })
  catch {
    @async.TimeoutError => ()
    error => raise error
  } noraise {
    _ => fail("expected body cancellation")
  }
  assert_true(first_finished.val)
  assert_true(second_finished.val)
  assert_true(@env.now() - start < 15000UL)
}

///|
async test "fixture cleanup survives cancellation and preserves the original error" {
  let cleaned = Ref(false)
  try
    @async.with_timeout(20, () => {
      let failed = Ref(false)
      defer finish_test_cleanup(failed, () => {
        @async.sleep(10)
        cleaned.val = true
        raise Protocol("cleanup failure")
      })
      errdefer {
        failed.val = true
      }
      @async.sleep(1000)
    })
  catch {
    @async.TimeoutError => ()
    error => raise error
  } noraise {
    _ => fail("expected timeout")
  }
  assert_true(cleaned.val)
  let body : () -> Unit raise = () => raise Protocol("body failure")
  try {
    let failed = Ref(false)
    defer finish_test_cleanup(failed, () => raise Protocol("cleanup failure"))
    errdefer {
      failed.val = true
    }
    body()
  } catch {
    Protocol(message) => assert_eq(message, "body failure")
    error => raise error
  } noraise {
    _ => fail("expected body failure")
  }
  try
    finish_test_cleanup(Ref(false), () => raise Protocol("cleanup failure"))
  catch {
    Protocol(message) => assert_eq(message, "cleanup failure")
    error => raise error
  } noraise {
    _ => fail("cleanup failure must fail a successful body")
  }
}

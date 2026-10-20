"""Small filesystem fixtures and a local cleanup child; no package builds."""
import importlib.util
import errno
import os
from pathlib import Path
import select
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("footprint", Path(__file__).with_name("package-footprint-sample.py"))
footprint = importlib.util.module_from_spec(spec)
spec.loader.exec_module(footprint)


class FootprintTests(unittest.TestCase):
    def setUp(self):
        self.owned = tempfile.TemporaryDirectory(dir="/tmp", prefix="mooncdc-package-owned-")
        self.evidence = tempfile.TemporaryDirectory(dir="/tmp", prefix="mooncdc-package-evidence-")
        self.addCleanup(self.owned.cleanup)
        self.addCleanup(self.evidence.cleanup)
        self.roots = [self.owned.name, self.evidence.name]

    def test_allocations_include_evidence_and_directory_buckets(self):
        p = Path(self.owned.name) / "module/_build/native/debug/test/root.o"
        p.parent.mkdir(parents=True)
        p.write_bytes(b"x" * 5000)
        e = Path(self.evidence.name) / "archive.tar"
        e.write_bytes(b"y" * 7000)
        result = footprint.sample(self.roots, "building")
        expected = sum(p.stat().st_blocks * 512 for root in self.roots
                       for p in [Path(root), *Path(root).rglob("*")])
        self.assertTrue(result["complete"])
        self.assertEqual(result["total_allocated_bytes"], expected)
        self.assertTrue(any(b["directory"] == "module/_build/native/debug/test"
                            and b["kind"] == ".o" for b in result["buckets"]))

    def test_hardlinks_are_counted_once_across_roots(self):
        p = Path(self.owned.name) / "object.o"
        p.write_bytes(b"a" * 4096)
        os.link(p, Path(self.evidence.name) / "copy.o")
        result = footprint.sample(self.roots, "building")
        self.assertTrue(result["complete"])
        self.assertEqual(result["hardlink_aliases"], 1)
        self.assertEqual(result["total_allocated_bytes"],
                         p.stat().st_blocks * 512 + sum(Path(r).stat().st_blocks * 512 for r in self.roots))

    def test_sparse_logical_size_is_not_allocated_size(self):
        p = Path(self.owned.name) / "sparse"
        with p.open("wb") as f:
            f.truncate(footprint.LIMIT + 1)
        result = footprint.sample(self.roots, "building")
        self.assertTrue(result["complete"])
        self.assertLess(result["total_allocated_bytes"], p.stat().st_size)

    def test_unknown_duplicate_and_missing_evidence_fail_closed(self):
        for roots in [self.roots + [self.owned.name], [self.owned.name],
                      self.roots + ["/tmp"], self.roots + [self.owned.name + "/missing"]]:
            result = footprint.sample(roots, "building")
            self.assertFalse(result["complete"])
            self.assertFalse(result["within_budget"])

    def test_foreign_symlink_is_not_followed_or_reported_as_zero(self):
        (Path(self.owned.name) / "outside").symlink_to("/etc")
        result = footprint.sample(self.roots, "building")
        self.assertFalse(result["complete"])
        self.assertTrue(result["errors"])

    def test_sample_limits_fail_closed(self):
        for kwargs in [dict(max_entries=0), dict(max_seconds=0)]:
            result = footprint.sample(self.roots, "building", **kwargs)
            self.assertFalse(result["complete"])

    def test_overbudget_keeps_measured_evidence(self):
        original = footprint.LIMIT
        try:
            footprint.LIMIT = 1
            (Path(self.owned.name) / "object").write_bytes(b"content")
            result = footprint.sample(self.roots, "failed")
            self.assertTrue(result["complete"])
            self.assertFalse(result["within_budget"])
            self.assertGreater(result["total_allocated_bytes"], 1)
            self.assertTrue(result["buckets"])
        finally:
            footprint.LIMIT = original


class LiveAllocationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir="/tmp", prefix="mooncdc-allocation-test-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.victim = self.root / "_build"
        self.victim.mkdir()
        (self.victim / "object").write_bytes(b"x" * 4096)
        (self.root / "retained").write_bytes(b"y" * 4096)

    def concurrent_remover(self):
        ready, removed = threading.Event(), threading.Event()
        errors = []
        def remove():
            try:
                if not ready.wait(2):
                    raise RuntimeError("scanner did not reach deletion barrier")
                shutil.rmtree(self.victim)
            except BaseException as error:
                errors.append(error)
            finally:
                removed.set()
        thread = threading.Thread(target=remove)
        thread.start()
        def finish():
            ready.set()
            thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
        self.addCleanup(finish)
        return ready, removed

    def test_original_observer_reproduces_real_concurrent_directory_deletion(self):
        # Frozen traversal from the failed observer saved in a79fbc9. Keep this
        # fixture self-contained when historical evidence is excluded from a ZIP.
        def legacy_scan():
            return sum(p.lstat().st_blocks * 512
                       for p in [self.root, *self.root.rglob("*")] if p.exists())
        accessor = getattr(self.root, "_accessor", None)
        if accessor is None:
            self.skipTest("legacy Path accessor reproduction targets the recorded Python 3.9 runtime")
        ready, removed = self.concurrent_remover()
        scanner = threading.get_ident()
        real_scandir = accessor.scandir
        def scandir(path):
            if threading.get_ident() == scanner and isinstance(path, (str, Path)) and Path(path) == self.victim:
                ready.set()
                self.assertTrue(removed.wait(2))
            return real_scandir(path)
        with mock.patch.object(accessor, "scandir", side_effect=scandir):
            with self.assertRaises(FileNotFoundError):
                legacy_scan()

    def test_confirmed_descendant_deletion_finishes_remaining_paths(self):
        ready, removed = self.concurrent_remover()
        scanner = threading.get_ident()
        real_iterdir = Path.iterdir
        attempts, missing = [], []
        def roots():
            attempts.append(1)
            return [self.root]
        def iterdir(path):
            if threading.get_ident() == scanner and path == self.victim:
                ready.set()
                self.assertTrue(removed.wait(2))
            return real_iterdir(path)
        with mock.patch.object(Path, "iterdir", iterdir):
            measured = footprint.allocated_bytes(roots, on_missing=lambda *event: missing.append(event))
        self.assertEqual(len(attempts), 1)
        self.assertEqual(len(missing), 1)
        expected = sum(p.lstat().st_blocks * 512 for p in [self.root, *self.root.iterdir()])
        self.assertEqual(measured, expected)
        self.assertGreater(measured, 0)

    def test_continuous_cleanup_of_multiple_directories_does_not_exhaust_retries(self):
        victims = [self.victim]
        for index in range(4):
            directory = self.root / str(index)
            directory.mkdir()
            (directory / 'object').write_bytes(b'x' * 4096)
            victims.append(directory)
        real_iterdir = Path.iterdir
        missing, attempts = [], []
        def roots():
            attempts.append(1)
            return [self.root]
        def iterdir(path):
            if path in victims:
                shutil.rmtree(path)
            return real_iterdir(path)
        with mock.patch.object(Path, 'iterdir', iterdir):
            measured = footprint.allocated_bytes(roots, on_missing=lambda *e: missing.append(e))
        self.assertEqual(len(attempts), 1)
        self.assertEqual(len(missing), len(victims))
        expected = sum(p.lstat().st_blocks * 512 for p in [self.root, *self.root.iterdir()])
        self.assertGreaterEqual(measured, expected)
        self.assertGreater(measured, 0)

    def test_concurrent_file_deletion_is_confirmed_without_losing_other_bytes(self):
        ready, removed = self.concurrent_remover()
        real_lstat = Path.lstat
        scanner = threading.get_ident()
        missing = []
        def lstat(path):
            if threading.get_ident() == scanner and path == self.victim / 'object':
                ready.set()
                self.assertTrue(removed.wait(2))
            return real_lstat(path)
        with mock.patch.object(Path, 'lstat', lstat):
            measured = footprint.allocated_bytes(lambda: [self.root], on_missing=lambda *e: missing.append(e))
        self.assertEqual(len(missing), 1)
        self.assertGreaterEqual(measured, (self.root / 'retained').lstat().st_blocks * 512)

    def test_read_error_confirming_absence_is_not_hidden(self):
        real_lstat = Path.lstat
        count = 0
        def lstat(path):
            nonlocal count
            if path == self.victim:
                count += 1
                if count == 1:
                    raise FileNotFoundError(errno.ENOENT, 'fixture disappeared', str(path))
                raise PermissionError(errno.EACCES, 'fixture cannot confirm absence', str(path))
            return real_lstat(path)
        with mock.patch.object(Path, 'lstat', lstat):
            with self.assertRaises(PermissionError):
                footprint.allocated_bytes(lambda: [self.root])

    def test_owned_root_disappearance_requires_fresh_scope(self):
        transient = self.root / 'transient'
        transient.mkdir()
        real_iterdir = Path.iterdir
        attempts = []
        def roots():
            attempts.append(1)
            return [self.root, transient] if len(attempts) == 1 else [self.root]
        def iterdir(path):
            if path == transient:
                transient.rmdir()
            return real_iterdir(path)
        with mock.patch.object(Path, 'iterdir', iterdir):
            measured = footprint.allocated_bytes(roots)
        self.assertEqual(len(attempts), 2)
        self.assertGreater(measured, 0)

    def test_driver_cleanup_lifecycle_keeps_retained_scope_until_child_exit(self):
        control, evidence, cache, owned = [self.root / name for name in
                                           ['control', 'evidence', 'cache', 'owned']]
        persistent = [control, evidence, cache]
        for directory in persistent:
            directory.mkdir()
            (directory / 'retained').write_bytes(b'x' * 4096)
        build = owned / 'extracted' / '_build'
        dependencies = owned / 'committed' / '.mooncakes' / 'async'
        for directory in [build, dependencies]:
            directory.mkdir(parents=True)
            for index in range(5):
                child = directory / str(index)
                child.mkdir()
                (child / 'object').write_bytes(b'x' * 4096)
        # Match driver ordering: remove extracted build; write final hashes,
        # digest and summary/marker; scope-exit removes the entire owned root;
        # only then does the child exit. Pipes make both races deterministic.
        program = '''
import pathlib, shutil, sys
owned, evidence = map(pathlib.Path, sys.argv[1:])
assert sys.stdin.readline().strip() == 'build'
shutil.rmtree(owned / 'extracted' / '_build')
(evidence / 'final-source-sha256').write_text('same source hash')
(evidence / 'package-sha256').write_text('same package hash')
(evidence / 'summary.json').write_text('{"source_unchanged":true}')
print('driver-complete', flush=True)
assert sys.stdin.readline().strip() == 'root'
shutil.rmtree(owned)
print('root-removed', flush=True)
assert sys.stdin.readline().strip() == 'exit'
'''
        child = subprocess.Popen([sys.executable, '-c', program, str(owned), str(evidence)],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        def finish():
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=2)
        self.addCleanup(finish)
        def signal_child(signal, reply):
            child.stdin.write(signal + '\n')
            child.stdin.flush()
            self.assertTrue(select.select([child.stdout], [], [], 2)[0], 'cleanup child did not reply')
            self.assertEqual(child.stdout.readline().strip(), reply)
        attempts, missing = [], []
        def roots():
            attempts.append(1)
            # Same lifecycle rule as the supervisor: persistent accounted
            # roots are mandatory; a fresh directory inventory adds live roots.
            return persistent + [p for p in self.root.iterdir() if p == owned]
        real_iterdir = Path.iterdir
        def remove_build(path):
            if path == build:
                signal_child('build', 'driver-complete')
            return real_iterdir(path)
        with mock.patch.object(Path, 'iterdir', remove_build):
            active_bytes = footprint.allocated_bytes(roots, on_missing=lambda *e: missing.append(e))
        self.assertGreater(active_bytes, 0)
        self.assertTrue((evidence / 'summary.json').is_file())
        attempts.clear()
        def remove_root(path):
            if path == owned:
                signal_child('root', 'root-removed')
            return real_iterdir(path)
        with mock.patch.object(Path, 'iterdir', remove_root):
            cleanup_bytes = footprint.allocated_bytes(roots, on_missing=lambda *e: missing.append(e))
        self.assertEqual(len(attempts), 2)
        self.assertIsNone(child.poll())
        self.assertFalse(owned.exists())
        self.assertEqual(set(roots()), set(persistent))
        self.assertGreater(cleanup_bytes, 0)
        child.stdin.write('exit\n')
        child.stdin.flush()
        self.assertEqual(child.wait(timeout=2), 0)
        final_bytes = footprint.allocated_bytes(roots)
        self.assertGreater(final_bytes, 0)
        self.assertTrue(missing)

    def test_repeated_missing_scope_is_incomplete_not_zero(self):
        events = []
        with self.assertRaises(footprint.IncompleteAllocationObservation):
            footprint.allocated_bytes(lambda: [self.root / "absent"], on_missing=lambda *e: events.append(e))
        self.assertEqual(len(events), 3)

    def test_actual_permission_error_is_not_retried_or_accepted(self):
        self.assertNotEqual(os.geteuid(), 0, "run this read-error fixture as the ordinary user")
        self.victim.chmod(0)
        missing = []
        try:
            with self.assertRaises(PermissionError):
                footprint.allocated_bytes(lambda: [self.root], on_missing=lambda *e: missing.append(e))
        finally:
            self.victim.chmod(0o700)
        self.assertEqual(missing, [])

    def test_io_error_is_not_retried_or_accepted(self):
        attempts = []
        def roots():
            attempts.append(1)
            return [self.root]
        with mock.patch.object(Path, "iterdir", side_effect=OSError(errno.EIO, "fixture I/O error")):
            with self.assertRaises(OSError) as error:
                footprint.allocated_bytes(roots)
        self.assertEqual(error.exception.errno, errno.EIO)
        self.assertEqual(len(attempts), 1)

    def test_known_overbudget_stops_before_a_later_missing_entry(self):
        missing = []
        real_iterdir = Path.iterdir
        def iterdir(path):
            if path == self.root:
                return iter([self.root / "absent", self.root / "retained"])
            return real_iterdir(path)
        with mock.patch.object(Path, "iterdir", iterdir):
            value = footprint.allocated_bytes(lambda: [self.root], limit_bytes=1,
                                              on_missing=lambda *e: missing.append(e))
        self.assertGreater(value, 1)
        self.assertEqual(missing, [])

    def test_early_exit_receipt_keeps_a_lower_bound_separate_from_complete_scans(self):
        complete = footprint.observe_allocated_bytes(lambda: [self.root])
        lower_bound = footprint.observe_allocated_bytes(lambda: [self.root], limit_bytes=1)
        self.assertTrue(complete['scan_complete'])
        self.assertEqual(complete['value_kind'], 'complete_scan')
        self.assertFalse(lower_bound['scan_complete'])
        self.assertEqual(lower_bound['value_kind'], 'over_limit_scan_lower_bound')
        self.assertFalse(complete['atomic_snapshot'])
        self.assertLess(lower_bound['allocated_bytes'], complete['allocated_bytes'])
        receipt = {}
        for observation in [complete, lower_bound, complete]:
            footprint.record_allocation_observation(receipt, observation)
        record = receipt['owned_allocation']
        self.assertEqual(record['max_complete_scan_bytes'], complete['allocated_bytes'])
        self.assertEqual(record['max_over_limit_scan_lower_bound_bytes'], lower_bound['allocated_bytes'])
        self.assertIsNone(record['actual_peak_bytes'])
        self.assertNotIn('peak_owned_allocated_bytes', receipt)

    def test_live_scan_counts_hardlinks_once_across_retained_roots(self):
        retained = self.root / 'evidence'
        retained.mkdir()
        os.link(self.root / 'retained', retained / 'same-bytes')
        observation = footprint.observe_allocated_bytes(lambda: [self.root, retained])
        expected = sum(path.lstat().st_blocks * 512 for path in
                       [self.root, self.victim, self.victim / 'object',
                        self.root / 'retained', retained])
        self.assertTrue(observation['scan_complete'])
        self.assertEqual(observation['allocated_bytes'], expected)
        self.assertEqual(observation['duplicate_inode_entries'], 2)

    def test_empty_scope_and_zero_attempts_do_not_qualify(self):
        with self.assertRaises(ValueError):
            footprint.allocated_bytes(lambda: [])
        with self.assertRaises(ValueError):
            footprint.allocated_bytes(lambda: [self.root], max_attempts=0)


if __name__ == "__main__":
    unittest.main()

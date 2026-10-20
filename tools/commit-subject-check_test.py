"""Subject grammar and real Git range fixtures; never change the project repo."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("commit-subject-check.py")
spec = importlib.util.spec_from_file_location("commit_subject_check", SCRIPT)
check = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = check
spec.loader.exec_module(check)


class SubjectTests(unittest.TestCase):
    def test_valid_types_scope_breaking_and_no_unwritten_limits(self):
        for title in ["fix: preserve checkpoints", "feat(core): add a source",
                      "feat!: change the API", "feat(core)!: change the API",
                      "tools: inspect a fixture", "wip: draft a fix",
                      "Fix: accept case-insensitive types", "ci(native/linux): run checks",
                      "docs: " + "a long title " * 12]:
            with self.subTest(title=title):
                self.assertIsNone(check.subject_error(title))

    def test_missing_prefix_empty_description_and_malformed_markers(self):
        for title in ["", "Reject extra inputs before reusing native debug artifacts",
                      "fix checkpoint errors", "fix: ", "fix:  ", "fix:no separator",
                      "fix(): empty scope", "fix( ): blank scope",
                      "fix!(core): wrong marker position", "fix(core): first\nsecond"]:
            with self.subTest(title=title):
                self.assertIsNotNone(check.subject_error(title))

    def test_language_hint_does_not_misclassify_unicode_identifiers(self):
        for title in ["fix: preserve café identifiers", "docs: 修订说明"]:
            self.assertIsNone(check.subject_error(title))
            self.assertTrue(check.needs_language_review(title))
        self.assertFalse(check.needs_language_review("fix: preserve UTF-8 values"))


class RangeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="mooncdc-commit-subject-test-")
        self.addCleanup(self.tmp.cleanup)
        self.repo = Path(self.tmp.name)
        self.env = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                        GIT_AUTHOR_NAME="Fixture", GIT_COMMITTER_NAME="Fixture",
                        GIT_AUTHOR_EMAIL="fixture@example.invalid",
                        GIT_COMMITTER_EMAIL="fixture@example.invalid",
                        GIT_TERMINAL_PROMPT="0")
        self.git("init", "-q")
        self.git("symbolic-ref", "HEAD", "refs/heads/main")
        self.serial = 0
        self.base = self.commit("Legacy title without a prefix")
        self.git("update-ref", "refs/remotes/origin/main", self.base)

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo, env=self.env,
                                       stderr=subprocess.PIPE, text=True).strip()

    def commit(self, subject):
        self.serial += 1
        name = "fixture-" + str(self.serial)
        (self.repo / name).write_text("synthetic\n")
        self.git("add", "--", name)
        self.git("commit", "-q", "-m", subject)
        return self.git("rev-parse", "HEAD")

    def subjects(self, selection):
        return [subject for _, subject in check.selected_subjects(self.repo, selection)]

    def cli_event(self, name, event):
        path = self.repo / "event.json"
        path.write_text(json.dumps(event))
        return subprocess.run([sys.executable, "-B", str(SCRIPT), "--github-event", str(path),
                               "--event-name", name], cwd=self.repo, env=self.env,
                              text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def test_push_checks_only_before_after_not_bad_legacy_baseline(self):
        head = self.commit("fix: preserve the durable prefix")
        event = {"before": self.base, "after": head}
        selection = check.github_selection(self.repo, "push", event)
        self.assertEqual(self.subjects(selection), ["fix: preserve the durable prefix"])
        result = self.cli_event("push", event)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Checked 1 non-merge subject(s); 0 invalid", result.stdout)

    def test_invalid_new_commit_is_rejected_by_the_actual_cli(self):
        head = self.commit("Reject extra inputs before reusing native debug artifacts")
        result = self.cli_event("push", {"before": self.base, "after": head})
        self.assertEqual(result.returncode, 1)
        self.assertIn(head[:12], result.stdout)

    def test_pr_uses_real_head_not_checkout_merge_or_new_base_commits(self):
        self.git("checkout", "-q", "-b", "feature")
        head = self.commit("feat(core)!: update the source contract")
        self.git("checkout", "-q", "main")
        new_base = self.commit("Unrelated base title without a prefix")
        self.git("merge", "-q", "--no-ff", "feature", "-m", "Merge pull request #1")
        event = {"pull_request": {"base": {"sha": new_base}, "head": {"sha": head}}}
        selection = check.github_selection(self.repo, "pull_request", event)
        self.assertEqual(selection.head, head)
        self.assertNotEqual(selection.head, self.git("rev-parse", "HEAD"))
        self.assertEqual(self.subjects(selection), ["feat(core)!: update the source contract"])
        self.assertEqual(self.cli_event("pull_request", event).returncode, 0)

    def test_new_branch_uses_default_branch_boundary(self):
        self.git("checkout", "-q", "-b", "release/0.1.0")
        head = self.commit("build(release): configure checks")
        event = {"before": "0" * 40, "after": head, "ref": "refs/heads/release/0.1.0",
                 "repository": {"default_branch": "main"}}
        selection = check.github_selection(self.repo, "push", event)
        self.assertEqual(selection.base, self.base)
        self.assertEqual(self.subjects(selection), ["build(release): configure checks"])
        self.assertEqual(self.cli_event("push", event).returncode, 0)

    def test_new_branch_at_default_head_has_no_new_subjects(self):
        event = {"before": "0" * 40, "after": self.base, "ref": "refs/heads/new-branch",
                 "repository": {"default_branch": "main"}}
        self.assertEqual(self.subjects(check.github_selection(self.repo, "push", event)), [])

    def test_initial_default_push_checks_head_only(self):
        head = self.commit("docs: describe initial release")
        event = {"before": "0" * 40, "after": head, "ref": "refs/heads/main",
                 "repository": {"default_branch": "main"}}
        self.assertEqual(self.subjects(check.github_selection(self.repo, "push", event)),
                         ["docs: describe initial release"])

    def test_deleted_ref_skips_without_requiring_old_object(self):
        result = self.cli_event("push", {"before": "f" * 40, "after": "0" * 40, "deleted": True})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Checked 0 non-merge subject(s)", result.stdout)

    def test_missing_base_fails_without_expanding_to_all_history(self):
        head = self.commit("fix: preserve the durable prefix")
        result = self.cli_event("push", {"before": "f" * 40, "after": head})
        self.assertEqual(result.returncode, 2)
        self.assertNotIn("Legacy title", result.stdout)

    def test_merge_subject_is_excluded_but_its_new_commits_are_checked(self):
        self.git("checkout", "-q", "-b", "feature")
        self.commit("fix: preserve state ownership")
        self.git("checkout", "-q", "main")
        self.git("merge", "-q", "--no-ff", "feature", "-m", "Merge branch feature")
        head = self.git("rev-parse", "HEAD")
        selection = check.github_selection(self.repo, "push", {"before": self.base, "after": head})
        self.assertEqual(self.subjects(selection), ["fix: preserve state ownership"])

    def test_explicit_range_and_draft_modes(self):
        head = self.commit("test: check range selection")
        result = subprocess.run([sys.executable, "-B", str(SCRIPT), "--base", self.base, "--head", head],
                                cwd=self.repo, env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        result = subprocess.run([sys.executable, "-B", str(SCRIPT), "--subject", "missing prefix"],
                                cwd=self.repo, env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 1)

    def test_malformed_events_and_unsupported_names_fail(self):
        for name, event in [("pull_request", {}), ("push", {"before": "--all", "after": self.base})]:
            with self.subTest(name=name, event=event):
                self.assertEqual(self.cli_event(name, event).returncode, 2)
        with self.assertRaises(ValueError):
            check.github_selection(self.repo, "workflow_dispatch", {})


if __name__ == "__main__":
    unittest.main()

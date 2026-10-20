"""Check CONTRIBUTING's commit-subject structure using only Python and Git.

No type whitelist, mandatory scope, length limit or body template is imposed.
English wording and the accuracy of the chosen type remain review decisions.
"""
import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Optional


HEADER = re.compile(r"[A-Za-z][A-Za-z0-9_-]*(?:\(([^()\r\n]+)\))?!?: \S[^\r\n]*")
OID = re.compile(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})")


def subject_error(subject):
    match = HEADER.fullmatch(subject)
    if match is None or (match[1] is not None and not match[1].strip()):
        return "expected type: description, type(scope): description, or either with ! before :"
    return None


def needs_language_review(subject):
    # Unicode identifiers can legitimately appear in an English sentence.
    # This is a review hint, never an automatic language rejection.
    return any(ord(char) > 127 and char.isalpha() for char in subject)


def git(repo, *args):
    result = subprocess.run(
        ["git", "--no-optional-locks", "--no-pager", *args], cwd=repo,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30,
    )
    if result.returncode:
        raise ValueError("Git input unavailable; fetch the exact base/head objects before checking: "
                         + result.stderr.strip())
    return result.stdout.rstrip("\n")


def resolve(repo, ref):
    return git(repo, "rev-parse", "--verify", "--end-of-options", ref + "^{commit}")


def event_oid(value):
    if not isinstance(value, str) or OID.fullmatch(value) is None:
        raise ValueError("event base/head must be a full Git object ID")
    return value.lower()


@dataclass(frozen=True)
class Selection:
    base: Optional[str]
    head: Optional[str]
    reason: str


def github_selection(repo, event_name, event):
    if event_name == "pull_request":
        pr = event["pull_request"]
        return Selection(resolve(repo, event_oid(pr["base"]["sha"])),
                         resolve(repo, event_oid(pr["head"]["sha"])),
                         "PR base..head; excludes the synthetic checkout merge")
    if event_name != "push":
        raise ValueError("only push and pull_request supply an automatic commit range")
    after = event_oid(event["after"])
    if event.get("deleted") is True or set(after) == {"0"}:
        return Selection(None, None, "deleted ref; no introduced commits")
    before = event_oid(event["before"])
    head = resolve(repo, after)
    if set(before) != {"0"}:
        return Selection(resolve(repo, before), head, "push before..after")
    default = event["repository"]["default_branch"]
    if not isinstance(default, str) or not default:
        raise ValueError("new ref push requires repository.default_branch")
    if event["ref"] == "refs/heads/" + default:
        return Selection(None, head, "initial default-branch push; head only")
    return Selection(resolve(repo, "refs/remotes/origin/" + default), head,
                     "new ref push; default branch..head, not all existing history")


def selected_subjects(repo, selection):
    if selection.head is None:
        return []
    if selection.base is None:
        # An initial default-branch push has no previous remote boundary.
        parents = git(repo, "rev-list", "--parents", "-n", "1", selection.head).split()
        hashes = [selection.head] if len(parents) <= 2 else []
    else:
        hashes = git(repo, "rev-list", "--reverse", "--no-merges",
                     selection.base + ".." + selection.head).splitlines()
    return [(sha, git(repo, "show", "--no-show-signature", "-s", "--format=%s", sha))
            for sha in hashes]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--subject", help="check a draft subject without creating a commit")
    mode.add_argument("--base", help="check only commits in BASE..HEAD")
    mode.add_argument("--github-event", type=Path, help="GitHub event JSON path")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--event-name", choices=["push", "pull_request"])
    args = parser.parse_args(argv)
    if args.head != "HEAD" and args.base is None:
        parser.error("--head is only used with --base")
    if args.event_name and args.github_event is None:
        parser.error("--event-name requires --github-event")
    if args.github_event and not args.event_name:
        parser.error("--github-event requires --event-name")
    try:
        if args.subject is not None:
            subjects = [("draft", args.subject)]
        else:
            repo = Path.cwd()
            if args.github_event:
                event = json.loads(args.github_event.read_text())
                selection = github_selection(repo, args.event_name, event)
            else:
                selection = Selection(resolve(repo, args.base), resolve(repo, args.head),
                                      "explicit base..head")
            print("Scope: " + selection.reason)
            subjects = selected_subjects(repo, selection)
        failures = 0
        for sha, subject in subjects:
            error = subject_error(subject)
            if error:
                print("ERROR " + sha[:12] + " " + repr(subject) + ": " + error)
                failures += 1
            elif needs_language_review(subject):
                print("REVIEW " + sha[:12] + ": confirm English wording for " + repr(subject))
        print("Checked " + str(len(subjects)) + " non-merge subject(s); " + str(failures) + " invalid.")
        print("English wording and type/summary accuracy require human review.")
        return 1 if failures else 0
    except (ValueError, KeyError, TypeError, OSError, subprocess.TimeoutExpired) as error:
        print("Cannot determine/check the requested range: " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

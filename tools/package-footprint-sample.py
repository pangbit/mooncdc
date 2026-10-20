"""Read-only allocated-byte snapshots for owned package workspaces.

This does not launch, stop, delete, or accept a package run. A future supervisor
must capture stdout even on failure and stop its own process group on nonzero.
"""
import argparse
import json
import os
from pathlib import Path
import stat
import time

LIMIT = 512 * 1024 * 1024


class IncompleteAllocationObservation(RuntimeError):
    pass


def observe_allocated_bytes(roots_for_attempt, *, limit_bytes=LIMIT, max_attempts=3,
                            on_missing=None):
    """Rescan a live owned scope after ENOENT, never return an unknown zero.

    Vanished descendants have no remaining directory allocation. Confirm their
    absence and keep the bytes already counted, then finish the other paths.
    Root disappearance requires a fresh scope and traversal. Other read errors
    propagate immediately. An over-limit scan lower bound stops before any retry.
    A complete traversal is a non-atomic observation, never a true disk peak.
    Count each (device, inode) once, including across retained evidence roots.
    """
    if max_attempts < 1:
        raise ValueError("allocation attempts must be positive")
    last_missing = None
    for attempt in range(1, max_attempts + 1):
        total = 0
        seen = set()
        duplicate_inode_entries = 0
        def observation(complete):
            return {"allocated_bytes": total, "scan_complete": complete,
                    "value_kind": "complete_scan" if complete else "over_limit_scan_lower_bound",
                    "limit_bytes": limit_bytes, "duplicate_inode_entries": duplicate_inode_entries,
                    "atomic_snapshot": False}
        try:
            roots = [Path(p) for p in roots_for_attempt()]
            if not roots:
                raise ValueError("allocation scope is empty")
            # Required roots must be observable, including an empty directory.
            for root in roots:
                root.lstat()
            pending = list(roots)
            while pending:
                p = pending.pop()
                try:
                    metadata = p.lstat()
                except FileNotFoundError as error:
                    if p in roots:
                        raise
                    # Recheck the missing name. If it was recreated, scan it;
                    # permission and I/O failures must never become absence.
                    try:
                        p.lstat()
                    except FileNotFoundError:
                        if on_missing is not None:
                            on_missing(attempt, error, total)
                        continue
                    raise error
                inode = (metadata.st_dev, metadata.st_ino)
                if inode in seen:
                    duplicate_inode_entries += 1
                    continue
                seen.add(inode)
                total += metadata.st_blocks * 512
                if total > limit_bytes:
                    return observation(False)
                if stat.S_ISDIR(metadata.st_mode):
                    # Unlike glob, iterdir does not hide PermissionError.
                    try:
                        pending.extend(p.iterdir())
                    except FileNotFoundError as error:
                        if p in roots:
                            raise
                        try:
                            p.lstat()
                        except FileNotFoundError:
                            if on_missing is not None:
                                on_missing(attempt, error, total)
                            continue
                        raise error
            # A removed root cannot silently qualify as an empty observation.
            for root in roots:
                root.lstat()
            return observation(True)
        except FileNotFoundError as error:
            last_missing = error
            if on_missing is not None:
                on_missing(attempt, error, total)
    raise IncompleteAllocationObservation(
        "allocation scope kept disappearing; no complete observation"
    ) from last_missing


def allocated_bytes(roots_for_attempt, *, limit_bytes=LIMIT, max_attempts=3,
                    on_missing=None):
    """Compatibility value for limit checks; an over-limit value is a lower bound.

    Receipts must use observe_allocated_bytes to retain the observation kind.
    """
    return observe_allocated_bytes(roots_for_attempt, limit_bytes=limit_bytes,
                                   max_attempts=max_attempts,
                                   on_missing=on_missing)["allocated_bytes"]


def record_allocation_observation(receipt, observation):
    """Keep completed scans and early-exit bounds separate; no peak is inferred."""
    record = receipt.setdefault("owned_allocation", {
        "unit": "bytes", "metric": "st_blocks * 512; unique (st_dev, st_ino)",
        "enforcement": "sampled-assertion; not a hard quota",
        "max_complete_scan_bytes": None,
        "max_over_limit_scan_lower_bound_bytes": None,
        "actual_peak_bytes": None})
    record["latest"] = dict(observation)
    key = ("max_complete_scan_bytes" if observation["scan_complete"] else
           "max_over_limit_scan_lower_bound_bytes")
    prior = record[key]
    record[key] = max(prior if prior is not None else 0, observation["allocated_bytes"])


def checked_roots(paths):
    roots = []
    for value in paths:
        p = Path(value).absolute()
        if p.is_symlink() or ".." in p.parts:
            raise ValueError("noncanonical or linked root")
        p = p.resolve(strict=True)
        if p.parent != Path("/tmp").resolve() or not p.name.startswith(
            ("mooncdc-package-owned-", "mooncdc-package-evidence-")
        ):
            raise ValueError("root outside dedicated package namespace")
        if not p.is_dir() or p.stat().st_uid != os.getuid():
            raise ValueError("unknown root ownership")
        if any(p == q or p in q.parents or q in p.parents for q in roots):
            raise ValueError("duplicate or overlapping roots")
        roots.append(p)
    if not any(p.name.startswith("mooncdc-package-owned-") for p in roots):
        raise ValueError("owned workspace required")
    if not any(p.name.startswith("mooncdc-package-evidence-") for p in roots):
        raise ValueError("evidence root required, including retained evidence")
    return roots


def category(relative):
    parts = relative.parts
    if "_build" in parts:
        i = parts.index("_build")
        return "/".join(parts[: min(i + 4, len(parts))])
    if ".mooncakes" in parts:
        return "/".join(parts[: parts.index(".mooncakes") + 1])
    return parts[0] if len(parts) > 1 else "."


def sample(paths, phase, *, max_entries=100000, max_seconds=10):
    start = time.monotonic()
    result = dict(schema="mooncdc-package-footprint-v1", phase=phase,
                  limit_bytes=LIMIT, total_allocated_bytes=0, entries=0,
                  hardlink_aliases=0, roots=[], buckets=[], errors=[],
                  complete=False, within_budget=False,
                  unit="bytes", metric="st_blocks * 512; unique (st_dev, st_ino)",
                  value_kind="incomplete_scan_lower_bound", atomic_snapshot=False,
                  enforcement="sampled-assertion; not a hard quota")
    seen = set()
    buckets = {}
    try:
        roots = checked_roots(paths)
        result["roots"] = [str(p) for p in roots]
        for root in roots:
            device = root.stat().st_dev
            pending = [root]
            while pending:
                if time.monotonic() - start > max_seconds:
                    raise ValueError("metadata sampling deadline")
                p = pending.pop()
                st = p.lstat()
                result["entries"] += 1
                if result["entries"] > max_entries:
                    raise ValueError("metadata entry limit")
                if st.st_dev != device or st.st_uid != os.getuid():
                    raise ValueError("unexpected mount or owner")
                if stat.S_ISLNK(st.st_mode):
                    raise ValueError("unresolved symlink allocation; refusing undercount")
                if not (stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode)):
                    raise ValueError("unexpected filesystem object")
                inode = (st.st_dev, st.st_ino)
                if inode in seen:
                    result["hardlink_aliases"] += 1
                    continue
                seen.add(inode)
                allocated = st.st_blocks * 512
                result["total_allocated_bytes"] += allocated
                relative = p.relative_to(root)
                kind = "directory" if stat.S_ISDIR(st.st_mode) else p.suffix or "other"
                key = (root.name, category(relative), kind)
                bucket = buckets.setdefault(key, dict(root=root.name,
                    directory=key[1], kind=kind, allocated_bytes=0,
                    logical_bytes=0, entries=0))
                bucket["allocated_bytes"] += allocated
                bucket["logical_bytes"] += st.st_size
                bucket["entries"] += 1
                if stat.S_ISDIR(st.st_mode):
                    pending.extend(sorted(p.iterdir(), reverse=True))
        result["complete"] = True
        result["value_kind"] = "complete_scan"
        result["within_budget"] = result["total_allocated_bytes"] <= LIMIT
    except (OSError, ValueError) as e:
        # A disappeared file during compilation is not a trustworthy zero.
        result["errors"].append(type(e).__name__ + ": " + str(e))
    result["buckets"] = [buckets[k] for k in sorted(buckets)]
    result["elapsed_seconds"] = round(time.monotonic() - start, 6)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True,
                        choices=["before-ordinary", "building", "after-ordinary", "failed", "final"])
    parser.add_argument("roots", nargs="+", help="owned, evidence, and every retained evidence root")
    args = parser.parse_args()
    result = sample(args.roots, args.phase)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["complete"] and result["within_budget"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

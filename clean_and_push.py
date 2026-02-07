#!/usr/bin/env python3
"""
Clean .gitmodules and all .git directories, then optionally push to a Git repo.
"""

import argparse
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path


def _handle_remove_readonly(func, path: str, exc_info):  # noqa: ARG001
    """On Windows, clear read-only and retry so .git (e.g. hooks) can be removed."""
    if not os.access(path, os.W_OK):
        os.chmod(path, stat.S_IWUSR | stat.S_IRUSR)
        func(path)
    else:
        raise  # noqa: B904


def find_git_dirs(root: Path) -> list[Path]:
    """Find all .git directories under root (including root itself)."""
    found = []
    for path in root.rglob(".git"):
        if path.is_dir():
            found.append(path)
    return sorted(found, key=lambda p: len(p.parts), reverse=True)  # deepest first


def delete_gitmodules(root: Path) -> bool:
    """Delete .gitmodules in root if it exists. Returns True if deleted."""
    f = root / ".gitmodules"
    if f.is_file():
        try:
            f.unlink()
            print(f"Deleted: {f}")
            return True
        except OSError as e:
            print(f"Error deleting {f}: {e}", file=sys.stderr)
            return False
    return False


def delete_git_dirs(root: Path, dry_run: bool = False) -> int:
    """Delete all .git directories under root. Returns count deleted."""
    dirs = find_git_dirs(root)
    count = 0
    for d in dirs:
        if dry_run:
            print(f"[dry-run] Would delete: {d}")
            count += 1
            continue
        try:
            shutil.rmtree(d, onerror=_handle_remove_readonly)
            print(f"Deleted: {d}")
            count += 1
        except OSError as e:
            # On Windows, try force-remove via cmd if rmtree failed (e.g. locked by Git/OneDrive)
            if sys.platform == "win32":
                try:
                    subprocess.run(
                        ["cmd", "/c", "rd", "/s", "/q", str(d)],
                        check=True,
                        capture_output=True,
                        timeout=30,
                    )
                    print(f"Deleted: {d} (via rd)")
                    count += 1
                except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
                    print(f"Error deleting {d}: {e}", file=sys.stderr)
            else:
                print(f"Error deleting {d}: {e}", file=sys.stderr)
    return count


def git_add_commit_push(
    repo_url: str,
    message: str,
    cwd: Path,
    dry_run: bool = False,
) -> bool:
    """Initialize repo (if needed), add, commit, and push to given remote."""
    if dry_run:
        print(f"[dry-run] Would: git init (if no .git), add ., commit -m \"{message}\", remote add origin {repo_url}, push -u origin main (or master)")
        return True

    # If we just deleted .git, we need to re-init
    git_dir = cwd / ".git"
    if not git_dir.is_dir():
        try:
            subprocess.run(
                ["git", "init"],
                cwd=cwd,
                check=True,
                capture_output=True,
                text=True,
            )
            print("Initialized new Git repository.")
        except subprocess.CalledProcessError as e:
            print(f"git init failed: {e.stderr or e}", file=sys.stderr)
            return False
        except FileNotFoundError:
            print("Git is not installed or not in PATH.", file=sys.stderr)
            return False

    steps = [
        (["git", "add", "."], "add"),
        (["git", "commit", "-m", message], "commit"),
    ]
    for cmd, name in steps:
        try:
            r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
            if r.returncode != 0:
                if "commit" in name and "nothing to commit" in (r.stdout or "") + (r.stderr or ""):
                    print("Nothing to commit (working tree clean).")
                else:
                    print(f"git {name} failed: {r.stderr or r.stdout or r}", file=sys.stderr)
                    if "commit" in name:
                        return False
        except FileNotFoundError:
            print("Git is not installed or not in PATH.", file=sys.stderr)
            return False

    # Set remote and push
    try:
        # Remove existing origin if any (in case URL changed)
        subprocess.run(
            ["git", "remote", "remove", "origin"],
            cwd=cwd,
            capture_output=True,
        )
    except Exception:
        pass

    try:
        subprocess.run(
            ["git", "remote", "add", "origin", repo_url],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"git remote add failed: {e.stderr or e}", file=sys.stderr)
        return False

    # Determine default branch (main or master)
    r = subprocess.run(
        ["git", "branch", "-M", "main"],
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    branch = "main"

    try:
        subprocess.run(
            ["git", "push", "-u", "origin", branch],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        )
        print(f"Pushed to {repo_url} (branch: {branch}).")
        return True
    except subprocess.CalledProcessError as e:
        print(f"git push failed: {e.stderr or e.stdout or e}", file=sys.stderr)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete .gitmodules and all .git directories; optionally push to a Git repo."
    )
    parser.add_argument(
        "--repo",
        "-r",
        metavar="URL",
        help="Git repo URL (HTTPS or SSH) to push to after cleaning and committing.",
    )
    parser.add_argument(
        "--message",
        "-m",
        metavar="MSG",
        default="Add file for Lab 5 submission",
        help="Commit message (default: 'Add file for Lab 5 submission').",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be done, do not delete or push.",
    )
    parser.add_argument(
        "--dir",
        "-d",
        metavar="PATH",
        default=".",
        help="Root directory to clean (default: current directory).",
    )
    args = parser.parse_args()
    if not (args.message or "").strip():
        args.message = "Add file for Lab 5 submission"

    root = Path(args.dir).resolve()
    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        sys.exit(1)

    print(f"Root: {root}")
    delete_gitmodules(root)
    n = delete_git_dirs(root, dry_run=args.dry_run)
    print(f"Removed {n} .git directory/directories.")

    if args.repo:
        if not args.dry_run and n == 0 and (root / ".git").is_dir():
            # We didn't remove the top-level .git; user might still want to push.
            # So we'll add/commit/push in place. But we did remove .gitmodules and sub .git dirs.
            pass
        ok = git_add_commit_push(
            args.repo,
            args.message,
            root,
            dry_run=args.dry_run,
        )
        sys.exit(0 if ok else 1)
    else:
        if args.dry_run:
            print("[dry-run] No --repo given, skipping push.")


if __name__ == "__main__":
    main()

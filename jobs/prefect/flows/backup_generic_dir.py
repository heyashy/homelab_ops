from __future__ import annotations

from prefect import flow, task
from pathlib import Path
from datetime import datetime, timezone
import subprocess
import os
import re
import time


def _slugify(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9._-]+", "-", name)
    name = re.sub(r"-{2,}", "-", name).strip("-")
    return name or "backup"


def _parse_exclude_dirs(exclude_dirs: str | None) -> list[str]:
    if not exclude_dirs:
        return []
    parts = [p.strip().strip("/") for p in exclude_dirs.split(",")]
    return [p for p in parts if p]


@task(log_prints=True)
def validate_paths(target_dir: str, dest_dir: str) -> tuple[Path, Path]:
    t = Path(target_dir).resolve()
    d = Path(dest_dir).resolve()

    if not t.exists() or not t.is_dir():
        raise ValueError(f"target_dir does not exist or is not a directory: {t}")

    d.mkdir(parents=True, exist_ok=True)

    # quick write test
    test_file = d / ".prefect_write_test"
    test_file.write_text("ok")
    test_file.unlink(missing_ok=True)

    print(f"Target: {t}")
    print(f"Dest:   {d}")
    return t, d


@task(log_prints=True)
def create_tar(target: Path, dest: Path, exclude_dirs: str | None = None) -> Path:
    # file name based on date + target folder name
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    suffix = _slugify(target.name)
    archive = dest / f"{date}-{suffix}.tar"

    excludes = _parse_exclude_dirs(exclude_dirs)

    cmd = ["tar"]

    # Exclude directories (relative to target root)
    # This excludes both the dir itself and its children.
    for ex in excludes:
        cmd += [f"--exclude=./{ex}", f"--exclude=./{ex}/*"]

    # Make archive from target directory contents
    cmd += ["-cf", str(archive), "-C", str(target), "."]

    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)

    size_mb = archive.stat().st_size / (1024 * 1024)
    print(f"Created: {archive} ({size_mb:,.1f} MB)")
    return archive


@task(log_prints=True)
def prune_old_archives(dest: Path, keep_days: int, name_hint: str) -> int:
    """
    Deletes .tar files older than keep_days in dest, but only those matching *-{name_hint}.tar
    so multiple backup flows can share the same dest_dir safely.
    """
    if keep_days < 1:
        print("keep_days < 1; skipping prune.")
        return 0

    now = time.time()
    cutoff = now - (keep_days * 86400)

    suffix = _slugify(name_hint)
    pattern = f"*-{suffix}.tar"

    deleted = 0
    for p in sorted(dest.glob(pattern)):
        try:
            mtime = p.stat().st_mtime
        except FileNotFoundError:
            continue
        if mtime < cutoff:
            print(f"Pruning old archive: {p.name}")
            p.unlink(missing_ok=True)
            deleted += 1

    print(f"Pruned {deleted} archive(s) older than {keep_days} day(s).")
    return deleted


@flow(name="backup-generic-dir")
def backup_generic_dir(
    target_dir: str,
    dest_dir: str,
    exclude_dirs: str | None = None,
    keep_days: int = 10,
) -> str:
    """
    Generic directory backup:
    - Tar everything under target_dir into dest_dir/YYYY-MM-DD-<target_dir_name>.tar
    - Optionally exclude subdirs (comma-separated, relative to target_dir)
    - Prune older archives for that target_dir_name in dest_dir
    """
    target, dest = validate_paths(target_dir, dest_dir)
    archive = create_tar(target, dest, exclude_dirs)
    prune_old_archives(dest, keep_days, target.name)
    return str(archive)


if __name__ == "__main__":
    # handy local run
    backup_generic_dir(
        target_dir=os.environ.get("TARGET_DIR", "/srv/homelab/state/static_server"),
        dest_dir=os.environ.get("DEST_DIR", "/mnt/mediaserve/static_server"),
        exclude_dirs=os.environ.get("EXCLUDE_DIRS", ""),
        keep_days=int(os.environ.get("KEEP_DAYS", "10")),
    )


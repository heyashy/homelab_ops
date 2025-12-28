from __future__ import annotations

from prefect import flow, task
from pathlib import Path
from datetime import datetime, timezone, timezone, timedelta
import subprocess


@task(log_prints=True)
def ensure_dest_dir(dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    print(f"Ensured destination directory exists: {dest_dir}")


@task(log_prints=True)
def backup_homeassistant_config(
    ha_config_dir: Path,
    dest_dir: Path = Path("/mnt/mediaserve/homeassistant"),
    include_db: bool = True,
    extra_excludes: list[str] | None = None,
) -> Path:
    """
    Creates:
      /mnt/backups/mediaserve/homeassistant/YYYY-MM-DD-homeassistant.tar

    Excludes bulky directories (media, caches, etc.) but keeps history by default.
    """
    if not ha_config_dir.exists():
        raise FileNotFoundError(f"Home Assistant config dir not found: {ha_config_dir}")

    # UTC date keeps filenames consistent, even if the server timezone changes
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    archive = dest_dir / f"{date_str}-homeassistant.tar"

    excludes = [
        # Big stuff
        "media",            # recordings/snapshots
        "tts",
        "backups",
        "nest",
        # "www",              # can be chunky; remove if you really want it

        # Caches / build artefacts
        "__pycache__",
        ".cache",
        "deps",
        

        "node_modules",

        # Noise
        "*.log",
        "*.log.*",
        "*.tmp",

        # SQLite transient files (keep the main DB, skip the WAL/SHM)
        #"*.db-wal",
        #"*.db-shm",
    ]

    if not include_db:
        excludes.append("home-assistant_v2.db")

    if extra_excludes:
        excludes.extend(extra_excludes)

    # Build tar command
    cmd = ["tar", "-cf", str(archive)]
    for ex in excludes:
        cmd += ["--exclude", ex]

    # Create tar from inside the HA config dir so paths in the tar are clean
    cmd += ["-C", str(ha_config_dir), "."]

    print("Running tar command:")
    print(" ".join(cmd))

    subprocess.run(cmd, check=True)

    # Quick visibility: size
    size_bytes = archive.stat().st_size
    size_mb = size_bytes / (1024 * 1024)
    print(f"Created backup: {archive} ({size_mb:.1f} MB)")

    return archive

@task(log_prints=True)
def prune_old_backups(dest_dir: str, keep_days: int = 10) -> int:
    """
    Deletes HA tar backups older than keep_days in dest_dir.
    Matches files like YYYY-MM-DD-homeassistant.tar
    """
    dest = Path(dest_dir)
    if not dest.exists():
        print(f"{dest} does not exist - nothing to prune")
        return 0

    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)
    deleted = 0

    for p in dest.glob("*-homeassistant.tar"):
        try:
            mtime = datetime.utcfromtimestamp(p.stat().st_mtime)
            if mtime < cutoff:
                p.unlink()
                deleted += 1
                print(f"Deleted old backup: {p.name} (mtime {mtime.isoformat()}Z)")
        except Exception as e:
            print(f"Failed to prune {p}: {e}")

    print(f"Pruned {deleted} backup(s) older than {keep_days} days")
    return deleted

@flow(name="backup-homeassistant")
def backup_homeassistant(
    ha_config_dir: str = "/srv/config/homeassistant",
    dest_dir: str = "/mnt/backups/mediaserve/homeassistant",
    include_db: bool = True,
    keep_days: int = 10
):
    dest = Path(dest_dir)
    ensure_dest_dir(dest)

    archive = backup_homeassistant_config(
        ha_config_dir=Path(ha_config_dir),
        dest_dir=dest,
        include_db=include_db,
    )
    prune_old_backups(dest_dir, keep_days)
    return str(archive)


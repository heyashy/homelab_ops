from __future__ import annotations

from prefect import flow, task, get_run_logger
from pathlib import Path
import os


DEFAULT_UID = 1000
DEFAULT_GID = 1000


@task(log_prints=True)
def fix_permissions(
    root: str,
    uid: int = DEFAULT_UID,
    gid: int = DEFAULT_GID,
    dry_run: bool = False,
    include_dirs: bool = True,
) -> dict:
    """
    Recursively chown everything under `root` to uid:gid.

    Returns a summary dict:
      - scanned: total paths seen
      - changed: successful chown operations
      - skipped: paths skipped (e.g. non-existent)
      - errors: errors encountered
    """
    logger = get_run_logger()
    root_path = Path(root)

    if not root_path.exists():
        raise FileNotFoundError(f"Root path does not exist: {root_path}")

    scanned = 0
    changed = 0
    skipped = 0
    errors = 0

    # Optionally include the root dir itself
    targets = [root_path] if include_dirs else []
    targets.extend(root_path.rglob("*"))

    for path in targets:
        scanned += 1

        # Skip directories if include_dirs=False
        if path.is_dir() and not include_dirs:
            skipped += 1
            continue

        try:
            if dry_run:
                print(f"[DRY-RUN] chown {uid}:{gid} {path}")
                changed += 1
            else:
                os.chown(path, uid, gid)
                changed += 1

        except FileNotFoundError:
            skipped += 1
        except PermissionError as e:
            errors += 1
            print(f"[WARN] PermissionError on {path}: {e}")
        except OSError as e:
            errors += 1
            print(f"[WARN] OSError on {path}: {e}")

    logger.info(
        f"Permission fix complete for {root_path} "
        f"(uid:gid={uid}:{gid}) scanned={scanned} changed={changed} skipped={skipped} errors={errors}"
    )

    return {
        "root": str(root_path),
        "uid": uid,
        "gid": gid,
        "dry_run": dry_run,
        "include_dirs": include_dirs,
        "scanned": scanned,
        "changed": changed,
        "skipped": skipped,
        "errors": errors,
    }


@flow(name="permission-fixup")
def permission_fixup(
    root: str = "/srv/homelab/state",
    uid: int = DEFAULT_UID,
    gid: int = DEFAULT_GID,
    dry_run: bool = False,
    include_dirs: bool = True,
) -> dict:
    """
    Prefect flow wrapper.
    """
    return fix_permissions(root=root, uid=uid, gid=gid, dry_run=dry_run, include_dirs=include_dirs)


if __name__ == "__main__":
    # Local/manual run (optional)
    permission_fixup()


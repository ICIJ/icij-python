import logging
import subprocess
from pathlib import Path

from .connection_info import PostgresConnectionInfo

MIGRATIONS_DIR = Path(__file__).parent.joinpath("db", "migrations")

logger = logging.getLogger(__name__)

_DEFAULT_MIGRATE_ARG = ["--no-dump-schema", "--wait", "-d", str(MIGRATIONS_DIR)]


def migrate(
    connection_info: PostgresConnectionInfo,
    db_name: str,
    timeout_s: float | None = None,
):
    url = connection_info.url(db=db_name)

    global_args = list(_DEFAULT_MIGRATE_ARG)
    global_args.extend(("--url", url))
    if timeout_s is not None:
        global_args.extend(("--wait-timeout", f"{timeout_s:.0f}s"))
    args = ["dbmate"] + global_args + ["up", "--strict", "-v"]
    logger.debug("starting migration command: %s", " ".join(args))
    try:
        process = subprocess.run(
            args, capture_output=True, check=True, encoding="utf-8"
        )
    except subprocess.CalledProcessError as e:
        logger.error(
            'command "%s" failed with:\n- error: %s- output: %s',
            " ".join(e.cmd),
            e.stderr,
            e.output,
        )
        raise
    logger.debug("migration successful !\nLogs: \n%s", process.stdout)

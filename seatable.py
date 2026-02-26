"""
Database Backend Router
Switch between PostgreSQL and SeaTable via DB_BACKEND env var.
Default: postgres
"""

import os

DB_BACKEND = os.getenv("DB_BACKEND", "postgres")

if DB_BACKEND == "seatable":
    from db_seatable import *  # noqa: F401,F403
else:
    from db_postgres import *  # noqa: F401,F403

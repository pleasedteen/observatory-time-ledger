"""Compatibility alias for retention/garbage-collection entry points."""
from .retention import *  # noqa: F401,F403
from .retention import (  # noqa: F401
    CleanupService,
    GarbageCollector,
    RetentionManager,
    RetentionReport,
    cleanup_lake,
    plan_retention,
    run_retention,
)

cleanup = run_retention

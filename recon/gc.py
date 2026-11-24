"""Compatibility alias for :mod:`recon.retention`."""
from .retention import *  # noqa: F401,F403
from .retention import (  # noqa: F401
    DEFAULT_KEEP_VERSIONS,
    GarbageCollection,
    GarbageCollector,
    GCService,
    RetentionInterrupted,
    RetentionManager,
    RetentionPolicy,
    RetentionReport,
    RetentionService,
    apply_retention,
    as_lake,
    collect_garbage,
    gc,
    plan_retention,
    reclaim,
    resume_gc,
    resume_retention,
    run_retention,
)

# Some callers expect collect() directly from the gc module.
collect = run_retention
run_gc = run_retention

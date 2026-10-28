"""Delta helper behaviour that broke on the real cluster."""

import json
import os
import shutil


def test_query_app_id_changes_when_checkpoint_is_recreated(app_home):
    from src.common.config import checkpoint
    from src.common.lake import query_app_id

    q = "q_test_app_id"
    cp = checkpoint(q)
    shutil.rmtree(cp, ignore_errors=True)
    assert query_app_id(q) == q                      # no checkpoint yet → plain name

    os.makedirs(cp)
    with open(os.path.join(cp, "metadata"), "w") as f:
        json.dump({"id": "run-1"}, f)
    first = query_app_id(q)

    shutil.rmtree(cp)                                 # checkpoint reset → Spark writes a new query id
    os.makedirs(cp)
    with open(os.path.join(cp, "metadata"), "w") as f:
        json.dump({"id": "run-2"}, f)
    second = query_app_id(q)

    # A new txnAppId means Delta no longer skips the restarted query's batch 0, 1, 2 ...
    assert first == f"{q}-run-1" and second == f"{q}-run-2" and first != second

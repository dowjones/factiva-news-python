from factiva.analytics import common
from factiva.analytics.common.tools import load_environment_value
from factiva.analytics.snapshot.jobs import UpdateJob
# from factiva.news.snapshot import Snapshot

VALID_SNAPSHOT_ID = load_environment_value('FACTIVA_SNAPSHOTID')
# Fill out this data
VALID_UPDATE_ID = load_environment_value('FACTIVA_SNAPSHOT_UPDATEID')

# Uncomment to test. Snapshot Updates can only be processed once every
# 24 hours
# def test_update_creation():
#     test_snapshot = Snapshot(snapshot_id=VALID_SNAPSHOT_ID)
#     test_snapshot.process_update(update_type='replacements')
#     assert len(test_snapshot.last_update_job.files) > 0


def test_update_creation():
    test_update = UpdateJob(update_id=VALID_UPDATE_ID)
    test_update.get_job_results()
    assert len(test_update.files) > 0
    assert len(test_update.job_id) > 0
    assert test_update.job_state == common.API_JOB_DONE_STATE

from datetime import datetime
from factiva.news import Snapshot
from factiva.news.snapshot import AnalyticsJob

querystr = "publication_datetime >= '2018-01-01 00:00:00' AND publication_datetime <= '2018-01-10 00:00:00' AND LOWER(language_code) = 'en'"
st1 = Snapshot(query=querystr)

# st1.submit_analytics_job()

# historic_analytics_job = AnalyticsJob()
# historic_analytics_job.job_id = '60a0bb91-3b94-4f54-b8b9-432b72b9315e'
# historic_analytics_job.job_state = 'JOB_CREATED'
# historic_analytics_job.submitted_datetime = datetime.now()
# historic_analytics_job.link = 'https://api.dowjones.com/alpha/analytics/60a0bb91-3b94-4f54-b8b9-432b72b9315e'
# historic_analytics_job.data = []

# st1.last_analytics_job = historic_analytics_job
# st1.get_analytics_job_results()

st1.process_analytics()
st1.last_analytics_job.data.head()

print('Done!')

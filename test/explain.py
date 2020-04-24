from datetime import datetime
from factiva.news import Snapshot

querystr = "publication_datetime >= '2018-01-01 00:00:00' AND publication_datetime <= '2018-01-10 00:00:00' AND LOWER(language_code) = 'en'"
st1 = Snapshot(query=querystr)

# st1.submit_explain_job()

# historic_explain_job = {
#     'job_id': '8ce4389c-dec9-4cd9-b9fc-8193881b4430',
#     'job_state': 'JOB_CREATED',
#     'submitted_datetime': datetime.now(),
#     'link': 'https://api.dowjones.com/alpha/extractions/documents/8ce4389c-dec9-4cd9-b9fc-8193881b4430/_explain',
#     'document_volume': 0
# }
# st1.last_explain_job = historic_explain_job
# st1.get_explain_job_results()

st1.process_explain()

print('Done!')

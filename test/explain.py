from datetime import datetime
from factiva.news.snapshot import Snapshot, ExplainJob

querystr = "publication_datetime >= '2018-01-01 00:00:00' AND publication_datetime <= '2018-01-10 00:00:00' AND LOWER(language_code) = 'en'"
st1 = Snapshot(query=querystr)

# st1.submit_explain_job()

historic_explain_job = ExplainJob()
historic_explain_job.job_id = 'd8ac32e4-82c6-4b84-a3d4-e21b0171d2d3'
historic_explain_job.job_state = 'JOB_CREATED'
historic_explain_job.submitted_datetime = datetime.now()
historic_explain_job.link = f'https://api.dowjones.com/alpha/extractions/documents/{historic_explain_job.job_id}/_explain'
historic_explain_job.document_volume = 0
st1.last_explain_job = historic_explain_job
st1.get_explain_job_results()

# st1.process_explain()

print(f'Doc Volume: {st1.last_explain_job.document_volume} for Explain ID {st1.last_explain_job.job_id}')

print('Done!')

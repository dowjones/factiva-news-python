# import json
from datetime import datetime
from factiva.news.snapshot import Snapshot, ExtractionJob

querystr = "publication_datetime >= '2018-01-01 00:00:00' AND publication_datetime <= '2018-01-02 00:00:00' AND LOWER(language_code) = 'en'"
st1 = Snapshot(query=querystr)  # Takes the user key from the FACTIVA_APIKEY environment variable

# st1.submit_extraction_job()

# historic_extraction_job = ExtractionJob()
# historic_extraction_job.job_id = 'ovdwdjktkk'
# historic_extraction_job.job_state = 'JOB_QUEUED'
# historic_extraction_job.submitted_datetime = datetime.now()
# historic_extraction_job.link = f'https://api.dowjones.com/alpha/extractions/documents/dj-synhub-extraction-{st1.api_user.api_key}-{historic_extraction_job.job_id}'
# st1.last_extraction_job = historic_extraction_job

# st1.get_extraction_job_results()

# st1.download_extraction_files()

st1.process_extraction()

# sn_dw = Snapshot(snapshot_id='3kxgjdg7ix')
# print(sn_dw)

print('Done!')

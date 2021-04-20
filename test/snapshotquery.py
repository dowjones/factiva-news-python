from factiva.news.snapshot import Snapshot, SnapshotQuery

querystr = "publication_datetime >= '2018-01-01 00:00:00' AND publication_datetime <= '2018-01-10 00:00:00' AND LOWER(language_code) = 'en'"
st1 = Snapshot(query=querystr)

print(st1.query.get_explain_query())
print(st1.query.get_analytics_query())
print(st1.query.get_extract_query())

query1 = SnapshotQuery(where=querystr, limit=1000, file_format='avro', frequency='YEAR', date_field='publication_datetime', group_by_source_code=True, top=25)
st2 = Snapshot(query=query1)

print(st2.query.get_explain_query())
print(st2.query.get_analytics_query())
print(st2.query.get_extract_query())

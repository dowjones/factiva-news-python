Quickstart
==========

The easiest way to start using `factiva-news` is by running a news extraction volume estimation using the Explain operation.

.. code-block:: python

    from factiva.news.snapshot import Snapshot
    my_query = "publication_datetime >= '2020-01-01 00:00:00' AND LOWER(language_code) = 'en'"
    my_snapshot = Snapshot(user_key='abcd1234abcd1234abcd1234abcd1234', query=my_query)
    my_snapshot.process_explain()  # This operation can take several minutes to complete
    print(my_snapshot.last_explain_job.document_volume)

After its execution, the object `last_explain_job` contains details about the job itself and the estimated volume.

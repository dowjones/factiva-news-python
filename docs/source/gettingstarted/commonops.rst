Quick Operations
================


Check Account Statistics
------------------------

Assumes the ENV variable ``FACTIVA_USERKEY`` is set.

.. code-block:: python

    from factiva.analytics import UserKey
    u = UserKey(stats=True)
    print(u)

.. code-block::

    <class 'factiva.analytics.UserKey'>
    ├─key: ****************************1234
    ├─cloud_token: <NotLoaded>
    ├─account_name: <NotLoaded>
    ├─account_type: <NotLoaded>
    ├─active_product: <NotLoaded>
    ├─max_allowed_concurrent_extractions: 0
    ├─max_allowed_extracted_documents: 0
    ├─max_allowed_extractions: 0
    ├─currently_running_extractions: 0
    ├─total_downloaded_bytes: 0
    ├─total_extracted_documents: 0
    ├─total_extractions: 0
    ├─total_stream_instances: 0
    ├─total_stream_subscriptions: 0
    ├─enabled_company_identifiers:
    │  └─<NotLoaded>
    ├─remaining_documents: 0
    └─remaining_extractions: 0


Get Account's Historical Full Extractions
-----------------------------------------

Uses the passed ``key`` parameter and ignores the ENV variable ``FACTIVA_USERKEY``.

.. code-block:: python

    from factiva.analytics import UserKey
    u = UserKey(key='abcd1234abcd1234abcd1234abcd1234')
    extractions = u.get_extractions()

The variable ``extractions`` will contain a ``pandas.DataFrame`` instance with the requested data.


Get Volume Estimates With Snapshot Explain
------------------------------------------

Assumes the ENV variable ``FACTIVA_USERKEY`` is set.

.. code-block:: python

    from factiva.analytics import SnapshotExplain
    my_query = "publication_datetime >= '2020-01-01 00:00:00' AND LOWER(language_code) = 'en'"
    my_explain = SnapshotExplain(query=my_query)
    my_explain.process_explain()  # This operation can take several minutes to complete
    print(my_explain)

After its execution, the object `last_explain_job` contains details about the job itself and the estimated volume.

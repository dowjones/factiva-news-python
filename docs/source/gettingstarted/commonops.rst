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

    <class 'factiva.core.userkey.UserKey'>
    |-key = ****************************1234
    |-cloud_token = **Not Fetched**
    |-account_name = AccName1234
    |-account_type = account_with_contract_limits
    |-active_products = DNA
    |-max_allowed_concurrent_extractions = 5
    |-max_allowed_extracted_documents = 200,000
    |-max_allowed_extractions = 3
    |-currently_running_extractions = 0
    |-total_downloaded_bytes = 7,253,890
    |-total_extracted_documents = 2,515
    |-total_extractions = 1
    |-total_stream_instances = 4
    |-total_stream_subscriptions = 1
    |-enabled_company_identifiers = [{'id': 4, 'name': 'isin'}, {'id': 3, 'name': 'cusip'}, {'id': 1, 'name': 'sedol'}, {'id': 5, 'name': 'ticker_exchange'}]
    |-remaining_documents = 197,485
    |-remaining_extractions = 2


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

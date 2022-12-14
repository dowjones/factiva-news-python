Dow Jones Factiva Analytics Python Library
##########################################
.. image:: https://github.com/dowjones/factiva-analytics-python/actions/workflows/main_test_publish.yml/badge.svg

This library simplifies the integration to Factiva Analytics API services that delivers premium news content.

The following services are currently implemented.

* **auth**: Contains tools to handle UserKey authentication and account statistics.
* **Snapshots**: Allows to run each snapshot creation, monitoring, download and local exploration, in an individual manner. Also allows to run the whole process within a single method.
* **Streams**: In addition to creating and getting stream details, contains the methods to easily implement a stream listener and push the content to other locations appropriate for high-available setups.
* **Taxonomy**: Operations that return taxonomies applied to classify news content.

Installation
============
To install this library, run the following commands.

.. code-block::

    $ pip install --upgrade factiva-analytics

Using Library services
======================
Most Factiva Analytics services are implemented in this library. There may be a delay (commonly weeks) when new features are released and their operations are implemented in this package.

Creating a User Instance and Getting its statistics
---------------------------------------------------
Create `UserKey` instance and retrieve a summary of the account statistics.

.. code-block:: python

    from factiva.analytics import UserKey
    u = UserKey(
        key='abcd1234abcd1234abcd1234abcd1234',  # Not needed if the ENV variable FACTIVA_USERKEY is set
        stats=True)  # Connects to the API and pulls the latest account status
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

Snapshots
---------
Create a new snapshot and download to a local repository just require a few lines of code.

.. code-block:: python

    from factiva.analytics import Snapshot
    my_query = "publication_datetime >= '2020-01-01 00:00:00' AND LOWER(language_code) = 'en'"
    my_snapshot = Snapshot(
        user_key='abcd1234abcd1234abcd1234abcd1234',  # Can be ommited if exist as env variable
        query=my_query)
    my_snapshot.process_extract()  # This operation can take several minutes to complete

After the process completes, the output files are stored in a subfolder named as the Extraction Job ID.

In the previous code a new snapshot is created using my_query as selection criteria and user_key for user authentication. After the job is being validated internally, a Snapshot Id is obtained along with the list of files to download. Files are automatically downloaded to a folder named equal to the snapshot ID, and contents are loaded as a Pandas DataFrame to the variable news_articles. This process may take several minutes, but automates the extraction process significantly.

Streams
-------
Create a stream instance and get the details to configure the stream client and listen the content as it is delivered.

.. code-block:: python

    from factiva.analytics import Stream

    stream_query = Stream(
        user_key='abcd1234abcd1234abcd1234abcd1234',   # Can be ommited if exist as env variable
        user_key_stats=True,
        query="publication_datetime >= '2021-04-01 00:00:00' AND LOWER(language_code)='en' AND UPPER(source_code) = 'DJDN'",
        )
    
    print(stream_query.create())

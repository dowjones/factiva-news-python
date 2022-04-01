Dow Jones Factiva News Python Library
#####################################
.. image:: https://github.com/dowjones/factiva-news-python/actions/workflows/master_test_publish.yml/badge.svg

This library simplifies the integration to Factiva API services for news-related services.

The following services are currently implemented.

* **Snapshots**: Allows to run each snapshot creation, monitoring, download and local exploration, in an individual manner. Also allows to run the whole process within a single method.
* **Streams**: In addition to creating and getting stream details, contains the methods to easily implement a stream listener and push the content to other locations appropriate for high-available setups.

The previous components rely on the API-Key authentication method, which is a prerequisite when using either of those services.

Installation
============
To install this library, run the following commands.

.. code-block::

    $ pip install --upgrade factiva-news

Using Library services
======================
Both services, Snapshots and Streams are implemented in this library.

Enviroment vars
===============
To be able to use Stream Listener options, add the following environment vars depending on your selected listener tool

To use BigQuery Stream Listener
.. code-block::

    $ export GOOGLE_APPLICATION_CREDENTIALS="/Users/Files/credentials.json"
    $ export STREAMLOG_BQ_TABLENAME=project.dataset.table

To use MongoDB Stream Listener
.. code-block::

    $ export MONGODB_CONNECTION_STRING=mongodb://localhost:27017
    $ export MONGODB_DATABASE_NAME=factiva-news
    $ export MONGODB_COLLECTION_NAME=stream-listener  

To define custom directories. If they are not set, the project root path will be used
.. code-block::

    $ export DOWNLOAD_FILES_DIR=/users/dowloads
    $ export STREAM_FILES_DIR=/users/listeners
    $ export LOG_FILES_DIR=/users/logs


Snapshots
---------
Create a new snapshot and download to a local repository just require a few lines of code.

.. code-block:: python

    from factiva.news.snapshot import Snapshot
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

    from factiva.news.stream import Stream

    stream_query = Stream(
        user_key='abcd1234abcd1234abcd1234abcd1234',   # Can be ommited if exist as env variable
        user_key_stats=True,
        query="publication_datetime >= '2021-04-01 00:00:00' AND LOWER(language_code)='en' AND UPPER(source_code) = 'DJDN'",
        )
    
    print(stream_query.create())

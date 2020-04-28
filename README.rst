Dow Jones Factiva News Python Library
#####################################

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
Create a new snapshot and download to a local repository just require a few lines of code.

.. code-block:: python

    from factiva.news.snapshot import Snapshot
    my_query = "publication_datetime >= '2020-01-01 00:00:00' AND LOWER(language_code) = 'en'"
    my_snapshot = Snapshot(api_user='abcd1234abcd1234abcd1234abcd1234', query=my_query)
    news_articles = my_snapshot.process_extract()  # This operation can take several minutes to complete

In the previous code a new snapshot is created using my_query as selection criteria and api_key for user authentication. After the job is being validated internally, a Snapshot Id is obtained along with the list of files to download. Files are automatically downloaded to a folder named equal to the snapshot ID, and contents are loaded as a Pandas DataFrame to the variable news_articles. This process may take several minutes, but automates the extraction process significantly.

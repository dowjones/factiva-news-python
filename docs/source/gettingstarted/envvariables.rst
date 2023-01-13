Environment Variables
=====================

When a class is instantiated, depending on the functionality some Environment Variables might be
required unless a value is specified.


Authentication
--------------

UserKey
^^^^^^^

* ``FACTIVA_USERKEY`` : Assigned API user key. E.g. ``abcd1234abcd1234abcd1234abcd1234``. Used in all services except ArticleRetrieval.

OAuthUser
^^^^^^^^^

* ``FACTIVA_CLIENTID`` : Assigned OAuth Client ID. E.g. ``0abcd1wxyz2abcd3wxyz4abcd5wxyz6o``. Required for ArticleRetrieval.
* ``FACTIVA_USERNAME`` : Assigned OAuth Username. E.g. ``9ZZZ000000-svcaccount@dowjones.com``. Required for ArticleRetrieval.
* ``FACTIVA_PASSWORD`` : Assigned OAuth Password. E.g. ``pa55WOrdpa55WOrd``. Required for ArticleRetrieval.

Logging
^^^^^^^

* ``FACTIVA_LOGLEVEL``: Level of detail for the logs. Accepted values are ``DEBUG``, ``INFO`` (default), ``WARNING``, ``ERROR``, ``CRITICAL``.


Streams
-------

* ``FACTIVA_SUBSCRIPTIONID`` : Subscription ID from an existing Streaming Instance. E.g. ``dj-synhub-stream-abcd1234abcd1234abcd1234abcd1234-1234abcxyz-filtered-abc123``.


Handlers and Data Processing
----------------------------

Elasticsearch
^^^^^^^^^^^^^

ENV variables used in Elasticsearch.


BigQuery
^^^^^^^^
ENV variables used in BigQuery.



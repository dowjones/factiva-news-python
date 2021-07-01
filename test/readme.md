# Test notes for the factiva-news Python package
General notes that describe the context the package is expected to be tested, and other considerations.

## Environment
The environment is conditioned by the main dependencies: requests and pandas. As per the Pandas documentation, the preference is to use [Python version 3.7.1](https://pandas.pydata.org/pandas-docs/stable/getting_started/install.html#python-version-support) and above, and requests work with almost any version of Python.

The code has been writen using features like f-string which are available since Python 3.6, thus, that's the minimum supported version. As per Pandas guidelines, Python 3.7.1 or above is preferred.

## Pandas dependency
Although the big majority of actions can be performed using the requests library, dealing with some responses can be easier using Pandas. Additionally, in the near future the response of new endpoints, and even new version are expected to return a more structured response. The package factiva-pipelines also uses Pandas as the main data manipulation tool.

## Test execution
The initial goal is to test a full sequence of steps for each operation separately. This means that operation-level test will check that all operations for a Snapshot, Stream Management, Taxonomy, etc.; execute managing all cases that seem reasonable in most situations. The operations to test are:

* Snapshots Class - constructor and other basic validations (snapshots.py)
* Snapshots - Explains v1 (explain.py)
* Snapshots - Analytics v1 (analytics.py)
* Snapshots - Extraction v1 (extraction.py)
* Snapshots - Updates v1 (updates.py)
* Stream - Instance Management (stream.py)
* Taxonomies - Retrieve and manage freshness (taxonomy.py)

In each test file it is possible to find specific considerations as comments.

Before starting the test, set the following enviroment variables with values like this:
```
FACTIVA_APIKEY='abcd1234abcd1234abcd1234abcd1234'
FACTIVA_SNAPSHOTID='pdsdakabfd'
FACTIVA_SNAPSHOT_UPDATEID='pdsdakabfd-additions-20210513060214'
```

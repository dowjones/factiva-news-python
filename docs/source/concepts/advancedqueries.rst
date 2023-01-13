Advanced Queries
================


Where Attribute
---------------
The where attribute is the most used attribute to select content by applying different conditions 
to the available fields. Below there are some code snippets that help building a where statement, 
and notes that help modify the syntax according to the need.

Range of dates
**************
It works with the standard SQL syntax. If updates will be collected in the future, avoid using an end-date.
Alternative fields are: `modification_datetime` and `ingestion_datetime`.

.. code-block:: sql

    publication_datetime >= '2010-01-01 00:00:00' AND publication_datetime <= '2020-06-30 23:59:59'


Filter by `source_codes`
************************
The following clause is useful to select sources by their individual code.

.. code-block:: sql

    UPPER(source_code) IN ('AASFNE', 'HTACCF', 'NLADLW', 'ADVTSR', 'AFNROL', 'AGEEOL', 'AGEE', 'HNASNI', 'APRS', 'ASXTEX', 'AUSTOL')

In case sources will be selected by their category or source family, a better option is using `restrictor_codes`. This field is not in the documentation, but the CSE or Integration Team can provide more information like source family codes

.. code-block:: sql

    REGEXP_CONTAINS(restrictor_codes, r'(?i)(^|,)(jpost|nytf|wp|latm|j)($|,)')


Filter by `subject_codes`
*************************

.. code-block:: sql

    REGEXP_CONTAINS(subject_codes, r'(?i)(^|,)(mcat|ccat|ecat|gglobe|ghea|ghnwi|gcns|gpir|gdatap|greest|grisk|gsci|gspace|gtrans)($|,)')


Filtering by the region where the source is headquarted
*******************************************************

.. code-block:: sql

    REGEXP_CONTAINS(region_of_origin, r'(?i)(aust|spain|italy|usa|uk)')


Filtering by language
*********************

.. code-block:: sql

    LOWER(language_code) IN ('en', 'es', 'it')


Filtering by company codes
**************************

This is applicable to any company-related fields (about, occur or company_codes and other combinations with identifiers - ISIN, CUSIP...).

.. code-block:: sql

    REGEXP_CONTAINS(company_codes, r'(?i)(^|,)(agbpet|agip|agphng|agpnme|agzgi|altgaz|bbor|brnene|distrg|eenivm|egapg|enichm|enie|enimnt)($|,)')

In case the interest is to ensure at least one company is tagged (the field is not empty), the expressions looks like this

.. code-block:: sql

    LENGTH(company_codes) > 2


Filtering for content with at least 1 relevant company

.. code-block:: sql

    LENGTH(company_codes_about) > 0


Filtering by Industry code
**************************

.. code-block:: sql

    REGEXP_CONTAINS(industry_codes, r'(?i)(^|,)(i1|i25121|i2567)($|,)')


Filtering by Executive codes
****************************

.. code-block:: sql

    REGEXP_CONTAINS(LOWER(person_codes), r'(?i)(^|,)(76064380|2349856)($|,)')


Filtering by the region the article is about
********************************************

.. code-block:: sql

    REGEXP_CONTAINS(region_codes, r'(?i)(^|,)(aust|spain|italy|usa|uk)($|,)')


Filtering by terms in full-text (Keyword search)
************************************************

.. code-block:: sql

    REGEXP_CONTAINS(CONCAT(title, ' ', IFNULL(snippet, ''), ' ', IFNULL(body, '')), r'(?i)(^|\b)(economic|economy|regulation|deficit|budget\W+tax|central\W+bank)($|.|\b)')

More examples are available in the Data Selection Samples in the Dow Jones Developer Portal (https://developer.dowjones.com/site/docs/data_selection_samples/index.gsp#)

Building the where statement. Python concatenates the strings when inside the parenthesis. Mind the extra space at the end of each string.

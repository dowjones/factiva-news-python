from setuptools import setup

with open("README.rst", "r") as fh:
    long_desc = fh.read()

with open('src/factiva/analytics/__version__.py') as f:
    for line in f:
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            version = version.strip('"')
            version = version.strip("'")
            continue

setup(
    name='factiva-analytics',
    version=version,
    description='Python package to interact with Factiva Analytics APIs. Services are described in the Dow Jones Developer Platform.',
    long_description=long_desc,
    long_description_content_type='text/x-rst',
    license='MIT',
    author='Dow Jones Customer Engineers',
    author_email='customer.solutions@dowjones.com',

    # Warning: the folder 'factiva' should NOT have an __init__.py file to avoid conflicts with the same namespace across other packages
    package_dir={'': 'src'},
    packages=['factiva.analytics', 'factiva.analytics.snapshot', 'factiva.analytics.stream',
              'factiva.analytics.dicts', 'factiva.analytics.const', 'factiva.analytics.taxonomy',
              'factiva.analytics.auth', 'factiva.analytics.req', 'factiva.analytics.tools'],
    url='https://developer.dowjones.com/',
    project_urls={
            "GitHub": "https://github.com/dowjones/factiva-analytics-python",
            "Documentation": "https://factiva-analytics-python.readthedocs.io/",
            "Bug Tracker": "https://github.com/dowjones/factiva-analytics-python/issues",
        },

    classifiers=[  # Optional
        # How mature is this project? Common values are
        #   1 - Planning
        #   2 - Pre-Alpha
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',
        'Intended Audience :: Financial and Insurance Industry',
        'Operating System :: OS Independent',
        'Topic :: Office/Business :: News/Diary',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    keywords='news, news aggregator, risk, compliance, nlp, alternative data, factiva, trading news, market movers',
    python_requires='>=3.8',
    install_requires=[
        'requests', 'pandas', 'fastavro', 'google-cloud-core', 'google-cloud-pubsub'
    ],
    extras_require={
        "MongoDB": ["pymongo"],
        "Elasticsearch": ["elasticsearch"],
        "BigQuery": ["google-cloud-bigquery"]
    })

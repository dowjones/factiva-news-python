from setuptools import setup  # , find_namespace_packages

with open("README.rst", "r") as fh:
    long_desc = fh.read()

setup(
    name='factiva-news',
    version='0.1.1',  # TODO: Migrate the version to something that suports __version__ and handles it in a more automatic way.
    description='Python package to interact with Factiva news-related APIs. Services are described in the Dow Jones Developer Platform.',
    long_description=long_desc,
    long_description_content_type='text/x-rst',
    author='Miguel Ballesteros',
    author_email='miguel.ballesteros@dowjones.com',
    # Warning: the folder 'factiva' should NOT have an __init__.py file to avoid conflicts with the same namespace across other packages
    package_dir={'': 'src'},
    packages=['factiva.news', 'factiva.news.snapshot', 'factiva.news.stream'],
    # packages=find_namespace_packages(include=['factiva.news.*']),
    # package_data={'': ['*.csv']},
    url='https://github.com/dowjones/factiva-news-python',

    classifiers=[  # Optional
        # How mature is this project? Common values are
        #   1 - Planning
        #   2 - Pre-Alpha
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',
        'Intended Audience :: Financial and Insurance Industry',
        'Operating System :: OS Independent',
        'Topic :: Office/Business :: News/Diary',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='news, news aggregator, risk, compliance, nlp, alternative data',
    python_requires='>=3.5',
    install_requires=['factiva-core', 'fastavro']
)

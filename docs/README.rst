Generating package documentation
################################
The documentation for this package is generated automatically from `docstrings`. 

* To create the documentation, run :code:`make html` inside the :code:`docs` directory. This will generate the HTML files with the documentation inside :code:`build` directory. 

* To modify style or format, modify the files within the :code:`source` directory.

* To add more modules to the documentation run :code:`sphinx-apidoc -o source {MODULE_DIRECTORY}`

Requirements
-------------
* Sphinx 

Integrating with :code:`readthedocs.org`
----------------------------------------
To publish the documentation to sites such as :code:`readthedocs.org`:

1. Link the repository to your account

2. Add :code:`setup.py`: to the project settings on the site. This will build the dependencies needed.
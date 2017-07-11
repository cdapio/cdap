.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=========
Split URL
=========

The SPLIT-URL directive splits a URL into protocol, authority, host,
port, path, filename, and query.

Syntax
------

::

    split-url <column>

The ``<column>`` is a column containing the URL.

Usage Notes
-----------

The SPLIT-URL directive will parse the URL into its constituents. Upon
splitting the URL, the directive creates seven new columns by appending
to the original column name:

-  ``column_protocol``
-  ``column_authority``
-  ``column_host``
-  ``column_port``
-  ``column_path``
-  ``column_filename``
-  ``column_query``

If the URL cannot be parsed correctly, an exception is throw. If the URL
column does not exist, columns with a ``null`` value are added to the
record.

Examples
--------

Using this record as an example:

::

    {
      "url": "http://example.com:80/docs/books/tutorial/index.html?name=networking#DOWNLOADING"
    }

Applying this directive:

::

    split-url url

would result in this record:

::

    {
      "url": "http://example.com:80/docs/books/tutorial/index.html?name=networking#DOWNLOADING",
      "url_protocol": "http",
      "url_authority": "example.com:80",
      "url_host": "example.com",
      "url_port": 80,
      "url_path": "/docs/books/tutorial/index.html",
      "url_filename": "/docs/books/tutorial/index.html?name=networking",
      "url_query": "name=networking"
    }

When the URL field in the record is ``null``:

::

    {
      "url": null
    }

the directive will generate:

::

    {
      "url": null,
      "url_protocol": null,
      "url_authority": null,
      "url_host": null,
      "url_port": null,
      "url_path": null,
      "url_filename": null,
      "url_query": null
    }

.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===========
Invoke HTTP
===========

The INVOKE-HTTP directive is an experimental directive to trigger an
HTTP POST request with a body composed from specified fields.

Syntax
------

::

    invoke-http <url> <column>[,<column>*] <header>[,<header>*]

The ``<column>``\ s specify the value to be sent to the service
``<url>`` in the POST request as the ``body``.

Usage Notes
-----------

The INVOKE-HTTP directive is used to apply a transformation on data
using an existing REST service. This directive passes the specified
columns as the POST body in the form of a JSON Object. The keys in the
JSON object are the column names, with the values and their types
derived from the objects stored in the column.

Upon processing of request by the service, the INVOKE-HTTP directive
expects the data to be written back as a JSON object. The JSON object is
then added to the record with its keys being the column names and the
values being the values returned for the keys. The types are all valid
JSON types.

Currently, the JSON response has to be simple types. Nested JSONs are
not currently supported.

*Note:* There is a significant cost associated with making an HTTP call
and this directive should not be used in an environment which would use
it to process large quantities of data.

When an HTTP service requires more than one header to be passed, they
can be specified as key-value pairs. For example:

::

      X-Proxy-Server=0.0.0.0,X-Auth-Type=Basic

*Note:* The key and value are separated by an equals sign (``=``) and
headers are separated by commas (``,``).

Examples
--------

Using this record as an example:

::

    {
        "latitude": 24.56,
        "longitude": -65.23,
        "IMEI": 212332321313,
        "location": "Mars City"
    }

Assume that a service to locate a postal code, given a latitude and
longitude, is available at an address such as
``http://hostname/v3/api/geo-find``.

Applying this directive:

::

    invoke-http http://hostname/v3/api/geo-find latitude,longitude

This would be translated into a ``POST`` call:

::

    POST v3/api/geo-find

with a body of:

::

    {
        "latitude": 24.56,
        "longitude": -65.23
    }

Note that only the two fields specified as parameters to the directive
are sent to the service.

In case of a failure, the input record is passed to the error collector
so that it can be re-processed later.

.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _http-restful-api-v2-introduction:

===========================================================
Introduction
===========================================================

.. include:: /_includes/include-v280-deprecate-http-restful-api-v2.rst

.. highlight:: console

.. _http-restful-api-v2-conventions:

Conventions
============

In this API, *client* refers to an external application that is calling CDAP using the HTTP interface.
*Application* refers to a user Application that has been deployed into CDAP.

.. rubric:: Base URL

All URLs referenced in this API have this base URL::

  http://<host>:<port>/v2

where ``<host>`` is the host name of the CDAP server and ``<port>`` is the port that is set as the ``router.bind.port``
in ``cdap-site.xml`` (default: ``10000``).

**Note:** If SSL is enabled for CDAP, then the base URL uses ``https`` and ``<port>`` becomes the port that is set
as the ``router.ssl.bind.port`` in ``cdap-site.xml`` (default: 10443).

In this API, the base URL is represented as::

  <base-url>

For example::

  PUT <base-url>/streams/<new-stream-id>

means::

  PUT http://<host>:<port>/v2/streams/<new-stream-id>


.. rubric:: Variable Replacement

Text that are variables that you are to replace is indicated by a series of angle brackets (``< >``). For example::

  PUT <base-url>/streams/<new-stream-id>

indicates that—in addition to the ``<base-url>``—the text ``<new-stream-id>`` is a variable
and that you are to replace it with your value, perhaps in this case *mystream*::

  PUT <base-url>/streams/mystream


.. _http-restful-api-v2-conventions-reserved-unsafe-characters:

Reserved and Unsafe Characters
==============================

In path parameters, reserved and unsafe characters must be replaced with their equivalent
percent-encoded format, using the "``%hh``" syntax, as described in 
`RFC3986: Uniform Resource Identifier (URI): Generic Syntax <http://tools.ietf.org/html/rfc3986#section-2.1>`__.

In general, any character that is not a letter, a digit, or one of ``$-_.+!*'()`` should be encoded.

See the section on :ref:`Path Parameters<services-path-parameters>` for suggested approaches to
encoding parameters.


Status Codes
============

`Common status codes <http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html>`__ returned for all HTTP calls:


.. list-table::
   :widths: 10 30 60
   :header-rows: 1

   * - Code
     - Description
     - Explanation
   * - ``200``
     - ``OK``
     - The request returned successfully
   * - ``400``
     - ``Bad Request``
     - The request had a combination of parameters that is not recognized
   * - ``401``
     - ``Unauthorized``
     - The request did not contain an authentication token
   * - ``403``
     - ``Forbidden``
     - The request was authenticated but the client does not have permission
   * - ``404``
     - ``Not Found``
     - The request did not address any of the known URIs
   * - ``405``
     - ``Method Not Allowed``
     - A request was received with a method not supported for the URI
   * - ``409``
     - ``Conflict``
     - A request could not be completed due to a conflict with the current resource state
   * - ``500``
     - ``Internal Server Error``
     - An internal error occurred while processing the request
   * - ``501``
     - ``Not Implemented``
     - A request contained a query that is not supported by this API

**Note:** These returned status codes are not necessarily included in the descriptions of the APIs,
but a request may return any of these.


Working with CDAP Security
==========================
When working with a CDAP cluster with security enabled (``security.enabled=true`` in
``cdap-site.xml``), all calls to the HTTP RESTful APIs must be authenticated. Clients must
first obtain an access token from the authentication server (see the :ref:`Client
Authentication <client-authentication>` section of the :ref:`developers:developer-index`).
In order to authenticate, all client requests must supply this access token in the
``Authorization`` header of the request::

   Authorization: Bearer <token>

For CDAP-issued access tokens, the authentication scheme must always be ``Bearer``.


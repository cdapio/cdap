.. :author: Cask Data, Inc.
   :description: HTTP RESTful Interface to the Cask Data Application Platform
   :copyright: Copyright © 2014 Cask Data, Inc.

.. _restful-api:

===========================================================
CDAP HTTP RESTful API
===========================================================


.. toctree::
   :maxdepth: 1
   
    Stream <stream>
    Dataset <dataset>
    Query <query>
    Procedure <procedure>
    Client <client>
    Service <service>
    Logging <logging>
    Metrics <metrics>
    Monitor <monitor>

.. highlight:: console

Introduction
============

The Cask Data Application Platform (CDAP) has an HTTP interface for a multitude of purposes:

- `Stream: <stream.html>`__ sending data events to a Stream or to inspect the contents of a Stream
- `Dataset: <dataset.html>`__ interacting with Datasets, Dataset Modules, and Dataset Types
- `Query: <query.html>`__ sending ad-hoc queries to CDAP Datasets
- `Procedure: <procedure.html>`__ sending calls to a stored Procedure
- `Client: <client.html>`__ deploying and managing Applications and managing the life cycle of Flows,
  Procedures, MapReduce Jobs, Workflows, and Custom Services
- `Service: <service.html>`__ supports making requests to the methods of an Application’s Services
- `Logging: <logging.html>`__ retrieving Application logs
- `Metrics: <metrics.html>`__ retrieving metrics for system and user Applications (user-defined metrics)
- `Monitor: <monitor.html>`__ checking the status of various System and Custom CDAP services

Conventions
-----------

In this API, *client* refers to an external application that is calling CDAP using the HTTP interface.
*Application* refers to a user Application that has been deployed into CDAP.

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

means
::

  PUT http://<host>:<port>/v2/streams/<new-stream-id>
  

Text that are variables that you are to replace is indicated by a series of angle brackets (``< >``). For example::

  PUT <base-url>/streams/<new-stream-id>

indicates that—in addition to the ``<base-url>``—the text ``<new-stream-id>`` is a variable
and that you are to replace it with your value, perhaps in this case *mystream*::

  PUT <base-url>/streams/mystream

.. rst2pdf: PageBreak

Status Codes
------------

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

**Note:** These returned status codes are not necessarily included in the descriptions of the API,
but a request may return any of these.


Working with CDAP Security
--------------------------
When working with a CDAP cluster with security enabled (``security.enabled=true`` in
``cdap-site.xml``), all calls to the HTTP RESTful APIs must be authenticated. Clients must
first obtain an access token from the authentication server (see the :ref:`Client
Authentication <client-authentication>` section of the *CDAP Developers’ Guide*). In order to
authenticate, all client requests must supply this access token in the ``Authorization``
header of the request::

   Authorization: Bearer <token>

For CDAP-issued access tokens, the authentication scheme must always be ``Bearer``.


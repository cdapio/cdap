.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _http-restful-api:
.. _restful-api:

===========================================================
CDAP HTTP RESTful API
===========================================================

.. toctree::
    v3 <http-restful-api-v3/index>
    v2 (Deprecated) <http-restful-api-v2/index>


.. |httpv3| replace:: **HTTP RESTful API v3:**
.. _httpv3: http-restful-api-v3/index.html

- |httpv3|_ CDAP has an HTTP interface for a multitude of purposes: everything from 
  sending data events to a Stream or to inspect the contents of a Stream through checking
  the status of various System and Custom CDAP services. V3 of the API includes the
  namespacing of applications, data and metadata to achieve application and data
  isolation. This is an inital step towards introducing `multi-tenancy
  <http://en.wikipedia.org/wiki/Multitenancy>`__ into CDAP.
  
  
.. |httpv2| replace:: **HTTP RESTful API v2:**
.. _httpv2: http-restful-api-v2/index.html

- |httpv2|_ This earlier, non-namespaced version of the HTTP RESTful API is deprecated as
  of CDAP version 2.8.0. Please update to your code to use the new 
  :ref:`API v3 <http-restful-api-v3>` with namespaces.


.. |httpv3| replace:: **HTTP RESTful API v3**
.. _httpv3: ../http-restful-api-v3/index.html

.. topic::  **Note: HTTP RESTful API v2 Deprecated** 

    As of *CDAP v2.8.0*, the *HTTP RESTful API v2* has been deprecated, pending removal in
    a later version. Replace all use of the *HTTP RESTful API v2* with |httpv3|_. This
    involves using a namespace and a namespace id in the call, as
    :ref:`described in the documentation <http-restful-api-v3-namespace>`. The ``default``
    namespace can be used as a drop-in replacement in existing API v2 code.

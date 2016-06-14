.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _client-authentication:

=====================
Client Authentication
=====================

Client authentication in CDAP consists of two components:

- **Authentication Server:** Clients must first authenticate with the authentication server using valid credentials.
  The authentication server integrates with different authentication
  backends (LDAP, JASPI plugins) using a plugin API. Once authenticated, clients are issued an access token
  representing their identity.
- **CDAP Router:** the CDAP router serves as the secured host in the perimeter security
  model.  All client calls to the cluster go through the router, and must present a valid access
  token when security is enabled.

CDAP Authentication Process
---------------------------

CDAP provides support for authenticating clients using OAuth 2 Bearer tokens, which are issued
by the CDAP authentication server.  The authentication server provides the integration point
for all external authentication systems.  Clients authenticate with the authentication server as
follows:

.. image:: ../_images/auth_flow_simple.png
   :width: 7in
   :align: center

#. Client initiates authentication, supplying credentials.

#. Authentication server validates supplied credentials against an external identity service,
   according to configuration (LDAP, Active Directory, custom).

   a. If validation succeeds, the authentication server returns an Access Token to the client.
   #. If validation fails, the authentication server returns a failure message, at which point
      the client can retry.

#. The client stores the resulting Access Token and supplies it in subsequent requests.
#. CDAP processes validate the supplied Access Token on each request.

   a. If validation succeeds, processing continues to authorization.
   #. If the submitted token is invalid, an "invalid token" error is returned.
   #. If the submitted token is expired, an "expired token" error is returned.  In this case, the
      client should restart authorization from step #1.

Supported Authentication Mechanisms
-----------------------------------
CDAP provides several ways to authenticate a client's identity:

- :ref:`installation-basic-authentication`
- :ref:`installation-ldap-authentication`
- :ref:`installation-jaspi-authentication`
- :ref:`Custom Authentication <developers-custom-authentication>`

To configure security, see the Administration Manual's :ref:`configuration-security`.

Obtaining an Access Token
-------------------------
Obtain a new access token by calling::

   GET <base-auth-url>/token
   
The ``<base-auth-url>`` can be found either by making a request and retrieving the
authentication URI (``auth_uri``) in the response body or by knowing the configuration of
the CDAP server for the ``security.auth.server.bind.address`` and port, as described in the
:ref:`Administration Manual: Security <admin:admin-security>`.

The required header and request parameters may vary according to the external
authentication mechanism that has been configured.  For username and password based
mechanisms, the ``Authorization`` header may be used::

   Authorization: Basic czZCaGRSa3F0MzpnWDFmQmF0M2JW

HTTP Responses
++++++++++++++
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Authentication was successful and an access token will be returned
   * - ``401 Unauthorized``
     - Authentication failed


Success Response Fields
+++++++++++++++++++++++
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Response Fields
     - Description
   * - ``access_token``
     - The Access Token issued for the client.  The serialized token contents are base-64 encoded
       for safe transport over HTTP.
   * - ``token_type``
     - In order to conform with the OAuth 2.0 Bearer Token Usage specification (`RFC 6750`_), this
       value must be "Bearer".
   * - ``expires_in``
     - Token validity lifetime in seconds.

.. _RFC 6750: http://tools.ietf.org/html/rfc6750


Example
+++++++

Sample request::

   GET <base-auth-url>/token HTTP/1.1
   Host: server.example.com
   Authorization: Basic czZCaGRSa3F0MzpnWDFmQmF0M2JW


Sample response::

   HTTP/1.1 200 OK
   Content-Type: application/json;charset=UTF-8
   Cache-Control: no-store
   Pragma: no-cache

   {
     "access_token":"2YotnFZFEjr1zCsicMWpAA",
     "token_type":"Bearer",
     "expires_in":3600,
   }


Comments
++++++++
- Only ``Bearer`` tokens (`RFC 6750`_) are currently supported


Authentication with RESTful Endpoints
-------------------------------------
When security is enabled on a CDAP cluster, only requests with a valid access token will
be allowed by CDAP.  Clients accessing CDAP HTTP RESTful endpoints will first need to
obtain an access token from the authentication server, as described above, which will be
passed to the router daemon on subsequent HTTP requests.

The following request and response descriptions apply to all CDAP HTTP RESTful endpoints::

   GET <base-url>/<resource> HTTP/1.1

The ``<base-url>`` is typically ``http://<host>:10000`` or
``https://<host>:10443``, as described in the :ref:`RESTful API Conventions
<reference:http-restful-api-conventions-base-url>`.

In order to authenticate, all client requests must supply the ``Authorization`` header::

   Authorization: Bearer wohng8Xae7thahfohshahphaeNeeM5ie

For CDAP-issued access tokens, the authentication scheme must always be ``Bearer``.


HTTP Responses
++++++++++++++
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Authentication was successful and an access token will be returned
   * - ``401 Unauthorized``
     - Authentication failed
   * - ``403 Forbidden``
     - Authentication succeeded, but access to the requested resource was denied

Error Response Fields
+++++++++++++++++++++
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Response Fields
     - Description
   * - ``error``
     - An error code describing the type of failure (see `Error Code Values`_)
   * - ``error_description``
     - A human readable description of the error that occurred
   * - ``auth_uri``
     - List of URIs for running authentication servers.  If a client receives a ``401
       Unauthorized`` response, it can use one of the values from this list to request a new
       access token.

Error Code Values
+++++++++++++++++
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Response Fields
     - Description
   * - ``invalid_request``
     - The request is missing a required parameter or is otherwise malformed
   * - ``invalid_token``
     - The supplied access token is expired, malformed, or otherwise invalid.  The client may
       request a new access token from the authorization server and try the call again.
   * - ``insufficient_scope``
     - The supplied access token was valid, but the authenticated identity failed authorization
       for the requested resource

Example
+++++++
A sample request and responses for different error conditions are shown below.  Header values are
wrapped for display purposes.

Request::

   GET <base-url>/resource HTTP/1.1
   Host: server.example.com
   Authorization: Bearer wohng8Xae7thahfohshahphaeNeeM5ie

Missing token::

   HTTP/1.1 401 Unauthorized
   WWW-Authenticate: Bearer realm="example"

   {
     "auth_uri": ["https://server.example.com:10010/token"]
   }

Invalid or expired token::

   HTTP/1.1 401 Unauthorized
   WWW-Authenticate: Bearer realm="example",
                       error="invalid_token",
                       error_description="The access token expired"

   {
     "error": "invalid_token",
     "error_description": "The access token expired",
     "auth_uri": ["https://server.example.com:10010/token"]
   }

Comments
++++++++
- The ``auth_uri`` value in the error responses indicates where the authentication server(s) are
  running, allowing clients to discover instances from which they can obtain access tokens.

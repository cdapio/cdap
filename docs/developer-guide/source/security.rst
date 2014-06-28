.. :Author: Continuuity, Inc.
   :Description: Reactor Security

=====================================
Reactor Security
=====================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. rst2pdf: .. contents::
.. rst2pdf: config _templates/pdf-config
.. rst2pdf: stylesheets _templates/pdf-stylesheet
.. rst2pdf: build ../build-pdf/

Continuuity Reactor supports securing clusters using a perimeter security model.  With perimeter
security, access to cluster nodes is restricted through a firewall.  Cluster nodes can communicate
with each other, but outside clients can only communicate with the cluster through a secured
gateway.  Using Reactor security, the Reactor authentication server issues credentials (access
tokens) to authenticated clients.  Clients then send these credentials on requests to Reactor.
Calls that lack valid access tokens will be rejected, limiting access to only authenticated
clients.

Authentication in Reactor consists of two components:

- **Authentication Server** - the authentication server integrates with different authentication
  backends (LDAP, JASPI plugins) using a plugin API.  Clients must first authenticate with the
  authentication server through this configured backend.  Once authenticated, clients are issued
  an access token representing their identity.
- **Reactor Router** - the Reactor router serves as the secured gateway in the perimeter security
  model.  All client calls to the cluster go through the router, and must present a valid access
  token when security is enabled.

For more details on the authentication process, see `Client Authentication`_.

By enabling perimeter security for Reactor, you can prevent access by any clients without valid
credentials.  In addition, access logging can be enabled in Reactor to provide an audit log of all
operations.


Enabling Security
==================
To enable security in the Continuuity Reactor, add these properties to ``continuuity-site.xml``:

==========================================  ===========
   Property                                   Value
==========================================  ===========
security.enabled                              true
security.auth.server.address                  <hostname>
==========================================  ===========


Configuring SSL
================
To configure the granting of ``AccessToken``\s via SSL, add these properties to ``continuuity-site.xml``:

==========================================  ===========
   Property                                   Value
==========================================  ===========
security.server.ssl.enabled                   true
security.server.ssl.keystore.path            <path>
security.server.ssl.keystore.password        <password>
==========================================  ===========

Enabling Access Logging
========================
To enable access logging, add the following to ``logback.xml`` (typically under ``/etc/continuuity/conf/``) ::

    <appender name="AUDIT" class="ch.qos.logback.core.rolling.RollingFileAppender">
      <file>access.log</file>
      <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
        <fileNamePattern>access.log.%d{yyyy-MM-dd}</fileNamePattern>
        <maxHistory>30</maxHistory>
      </rollingPolicy>
      <encoder>
        <pattern>%msg%n</pattern>
      </encoder>
    </appender>
    <logger name="http-access" level="TRACE" additivity="false">
      <appender-ref ref="AUDIT" />
    </logger>

You may also configure the file being logged to by changing the path under ``<file>...</file>``.

Configuring Authentication Mechanisms
======================================
Reactor provides several ways to authenticate a user's identity.

Basic Authentication
---------------------
The simplest way to identity a user is to authenticate against a realm file.
To configure basic authentication add the following properties to ``continuuity-site.xml``:

==========================================  ===========
   Property                                   Value
==========================================  ===========
security.authentication.handlerClassName     com.continuuity.security.server.BasicAuthenticationHandler
security.authentication.basic.realmfile      <path>
==========================================  ===========

The realm file is of the following format::

  username: password[,rolename ...]

Note that it is not advisable to use this method of authentication. In production, we recommend using any of the
other methods described below.

LDAP Authentication
--------------------
You can configure Reactor to authenticate against an LDAP instance by adding these
properties to ``continuuity-site.xml``:

================================================  ===========
   Property                                         Value
================================================  ===========
security.authentication.handlerClassName            com.continuuity.security.server.LDAPAuthenticationHandler
security.authentication.loginmodule.className       org.eclipse.jetty.plus.jaas.spi.LdapLoginModule
security.authentication.handler.debug               true/false
security.authentication.handler.hostname            <hostname>
security.authentication.handler.port                <port>
security.authentication.handler.userBaseDn          <userBaseDn>
security.authentication.handler.userRdnAttribute    <userRdnAttribute>
security.authentication.handler.userObjectClass     <userObjectClass>
================================================  ===========

In addition, you may also configure these optional properties:

=====================================================  ===========
   Property                                               Value
=====================================================  ===========
security.authentication.handler.bindDn                  <bindDn>
security.authentication.handler.bindPassword            <bindPassword>
security.authentication.handler.userIdAttribute         <userIdAttribute>
security.authentication.handler.userPasswordAttribute   <userPasswordAttribute>
security.authentication.handler.roleBaseDn              <roleBaseDn>
security.authentication.handler.roleNameAttribute       <roleNameAttribute>
security.authentication.handler.roleMemberAttribute     <roleMemberAttribute>
security.authentication.handler.roleObjectClass         <roleObjectClass>
=====================================================  ===========

Java Authentication Service Provider Interface (JASPI) Authentication
----------------------------------------------------------------------
To authenticate a user using JASPI add the following properties to ``continuuity-site.xml``:

================================================  ===========
   Property                                         Value
================================================  ===========
security.authentication.handlerClassName            com.continuuity.security.server.JASPIAuthenticationHandler
security.authentication.loginmodule.className       <custom-login-module>
================================================  ===========

In addition, any properties with the prefix ``security.authentication.handler.``,
such as ``security.authentication.handler.hostname``, will also be used by the handler.
These properties, without the prefix, will be used to instantiate the ``javax.security.auth.login.Configuration`` used
by the ``LoginModule``.

Custom Authentication
----------------------
To provide a custom authentication mechanism you may create your own ``AuthenticationHandler`` by overriding
``AbstractAuthenticationHandler`` and implementing the abstract methods. ::

  public class CustomAuthenticationHandler extends AbstractAuthenticationHandler {

    @Inject
    public CustomAuthenticationHandler(CConfiguration configuration) {
      super(configuration);
    }

    @Override
    protected LoginService getHandlerLoginService() {
      // ...
    }

    @Override
    protected IdentityService getHandlerIdentityService() {
      // ...
    }

    @Override
    protected Configuration getLoginModuleConfiguration() {
      // ...
    }
  }

To make your custom handler class available to the authentication service, copy your packaged jar file (and any
additional dependency jars) to the ``security/lib/`` directory within your Reactor installation
(typically under ``/opt/continuuity``).

Example Configuration
=======================
This is what your ``continuuity-site.xml`` could include when configured to enable security, SSL, and
authentication using LDAP::

  <property>
    <name>security.enabled</name>
    <value>true</value>
  </property>

  <!-- SSL configuration -->
  <property>
    <name>security.server.ssl.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>security.server.ssl.keystore.path</name>
    <value>/home/john/keystore.jks</value>
    <description>Path to the SSL keystore.</description>
  </property>

  <property>
    <name>security.server.ssl.keystore.password</name>
    <value>password</value>
    <description>Password for the SSL keystore.</description>
  </property>

  <!-- LDAP configuration -->
  <property>
    <name>security.authentication.handlerClassName</name>
    <value>com.continuuity.security.server.LDAPAuthenticationHandler</value>
  </property>

  <property>
    <name>security.authentication.loginmodule.className</name>
    <value>org.eclipse.jetty.plus.jaas.spi.LdapLoginModule</value>
  </property>

  <property>
    <name>security.authentication.handler.debug</name>
    <value>true</value>
  </property>

  <!--
    Override the following properties to use your LDAP server.
    Any optional parameters, as described above, may also be included.
  -->
  <property>
    <name>security.authentication.handler.hostname</name>
    <value>example.com</value>
    <description>Hostname of the LDAP server.</description>
  </property>

  <property>
    <name>security.authentication.handler.port</name>
    <value>389</value>
    <description>Port number of the LDAP server.</description>
  </property>

  <property>
    <name>security.authentication.handler.userBaseDn</name>
    <value>ou=people,dc=example</value>
  </property>

  <property>
    <name>security.authentication.handler.userRdnAttribute</name>
    <value>cn</value>
  </property>

  <property>
    <name>security.authentication.handler.userObjectClass</name>
    <value>inetorgperson</value>
  </property>

Testing Security
=================
To ensure that you've configured security correctly, run these simple tests to verify that the
security components are working as expected:

- After configuring Reactor as described above, restart the Reactor and attempt to use a service::

	curl -v <base-url>/apps

- This should return a 401 Unauthorized response. Submit a username and password to obtain an ``AccessToken``::

	curl -v -u username:password http://<gateway>:10009

- This should return a 200 OK response with the ``AccessToken`` string in the response body.
  Reattempt the first command, but this time include the ``AccessToken`` as a header in the command::

	curl -v -H "Authorization: Bearer <AccessToken>" <base-url>/apps

- This should return a 200 OK response.

- Visiting the Reactor Dashboard should redirect you to a login page that prompts for credentials.
  Entering the credentials should let you work with the Reactor Dashboard as normally.


Client Authentication
=====================
Reactor provides support for authenticating clients using OAuth 2 Bearer tokens, which are issued
by the Reactor authentication server.  The authentication server provides the integration point
for all external authentication systems.  Clients authenticate with the authentication server as
follows:

.. image:: _images/auth_flow_simple.png

  
#. Client initiates authentication, supplying credentials.

#. Authentication server validates supplied credentials against an external identity service,
   according to configuration (LDAP, Active Directory, custom).

   a. If validation succeeds, the authentication server returns an Access Token to the client.
   #. If validation fails, the authentication server returns a failure message, at which point
      the client can retry.

#. The client stores the resulting Access Token and supplies it in subsequent requests.
#. Reactor processes validate the supplied Access Token on each request.

   a. If validation succeeds, processing continues to authorization.
   #. If the submitted token is invalid, an "invalid token" error is returned.
   #. If the submitted token is expired, an "expired token" error is returned.  In this case, the
      client should restart authorization from step #1. 



Obtaining an Access Token
-------------------------
Obtain a new access token by calling::

   GET /token

The required header and request parameters may vary according to the external authentication
mechanism that has been configured.  For username and password based mechanisms, the
``Authorization`` header may be used::

   Authorization: Basic czZCaGRSa3F0MzpnWDFmQmF0M2JW


HTTP Responses
..............
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
~~~~~~~~~~~~~~~~~~~~~~~
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Response Fields
     - Description
   * - ``access_token``
     - The Access Token issued for the client.  The serialized token contents are base-64 encoded
       for safe transport over HTTP.
   * - ``token_type``
     - In order to conform with the OAuth 2.0 Bearer Token Usage specification (`RFC 6750`__), this
       value must be "Bearer".
   * - ``expires_in``
     - Token validity lifetime in seconds.

.. _rfc6750: http://tools.ietf.org/html/rfc6750

__ rfc6750_

Example
.......

Sample request::

   GET /token HTTP/1.1
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
........
- Only ``Bearer`` tokens (`RFC 6750`__) are currently supported

__ rfc6750_

.. rst2pdf: PageBreak


Authentication with REST Endpoints
----------------------------------
When security is enabled on a Reactor cluster, only requests with a valid access token will be
allowed by Reactor.  Clients accessing REST endpoints will first need to obtain an access token
from the authentication server, as described above, which will be passed to the Router daemon on
subsequent HTTP requests.

The following request and response descriptions apply to all Reactor REST endpoints::

   GET /<resource> HTTP/1.1

In order to authenticate, all client requests must supply the ``Authorization`` header::

   Authorization: Bearer wohng8Xae7thahfohshahphaeNeeM5ie

For Reactor issued access tokens, the authentication scheme must always be ``Bearer``.


HTTP Responses
..............
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
~~~~~~~~~~~~~~~~~~~~~
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
,,,,,,,,,,,,,,,,,
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
.......
A sample request and responses for different error conditions are shown below.  Header values are
wrapped for display purposes.

Request::

   GET /resource HTTP/1.1
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
........
- The ``auth_uri`` value in the error responses indicates where the authentication server(s) are
  running, allowing clients to discover instances from which they can obtain access tokens.


Where to Go Next
================
Now that you've secured your Continuuity Reactor, take a look at:

- `Operating a Continuuity Reactor <operations.html>`__,
  which covers putting Continuuity Reactor into production.

.. rst2pdf: PageBreak

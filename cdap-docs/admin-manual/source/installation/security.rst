.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _configuration-security:

=============
CDAP Security
=============

Cask Data Application Platform (CDAP) supports securing clusters using perimeter
security. Here, we’ll discuss how to setup a secure CDAP instance.

Additional security information, including client APIs and the authentication process, is covered
in the Developers’ Manual :ref:`security-index` section.

We recommend that in order for CDAP to be secure, CDAP security should always be used in conjunction with
`secure Hadoop clusters <http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SecureMode.html>`__.
In cases where secure Hadoop is not or cannot be used, it is inherently insecure and any applications
running on the cluster are effectively "trusted”. Though there is still value in having the perimeter access
be authenticated in that situation, whenever possible a secure Hadoop cluster should be employed with CDAP security.

CDAP Security is configured in ``cdap-site.xml`` and ``cdap-security.xml``:

* ``cdap-site.xml`` has non-sensitive information, such as the type of authentication mechanism and their configuration.
* ``cdap-security.xml`` is used to store sensitive information such as keystore passwords and
  SSL certificate keys. It should be owned and readable only by the CDAP user.
  
These files are shown in :ref:`appendix-cdap-site.xml` and :ref:`appendix-cdap-security.xml`.

File paths can either be absolute paths or, in the case of 
:ref:`standalone CDAP <standalone-index>`, relative to the CDAP SDK installation directory.

.. _enabling-security:

Enabling Security
-----------------
To enable security in CDAP, add these properties to ``cdap-site.xml``:

================================================= ===================== =====================================================
Property                                          Value                 Notes
================================================= ===================== =====================================================
``security.enabled``                              ``true``
``security.auth.server.address``                  *Deprecated*          Use ``security.auth.server.bind.address`` instead
``security.auth.server.bind.address``             ``<hostname>``
================================================= ===================== =====================================================

Configuring Kerberos (required)
...............................
To configure Kerberos authentication for various CDAP services, add these properties to ``cdap-site.xml``:

================================================= ==================== ======================================================
Property                                           Default Value        Description
================================================= ==================== ======================================================
``kerberos.auth.enabled``                         ``security.enabled`` ``true`` to enable Kerberos authentication
``cdap.master.kerberos.keytab``                        *None*          Kerberos keytab file path, either absolute or relative
``cdap.master.kerberos.principal``                     *None*          Kerberos principal associated with the keytab
================================================= ==================== ======================================================

Configuring Zookeeper (required)
................................
To configure Zookeeper to enable SASL authentication, add the following to your ``zoo.cfg``::

  authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
  jaasLoginRenew=3600000
  kerberos.removeHostFromPrincipal=true
  kerberos.removeRealmFromPrincipal=true

This will let Zookeeper use the ``SASLAuthenticationProvider`` as an auth provider, and the ``jaasLoginRenew`` line
will cause the Zookeeper server to renew its Kerberos ticket once an hour.

Then, create a ``jaas.conf`` file for your Zookeeper server::

  Server {
       com.sun.security.auth.module.Krb5LoginModule required
       useKeyTab=true
       keyTab="/path/to/zookeeper.keytab"
       storeKey=true
       useTicketCache=false
       principal="<your-zookeeper-principal>";
  };

The keytab file must be readable by the Zookeeper server, and ``<your-zookeeper-principal>`` must correspond
to the keytab file.

Finally, start Zookeeper server with the following JVM option::

  -Djava.security.auth.login.config=/path/to/jaas.conf

.. _running_servers_with_ssl:

Running Servers with SSL
........................

To enable running servers with SSL in CDAP, add this property to ``cdap-site.xml``:

================================================= ===========================================================================
   Property                                          Value
================================================= ===========================================================================
``ssl.enabled``                                      ``true``
================================================= ===========================================================================

Default Ports
.............

Without SSL:

================================================= ===========================================================================
   Property                                          Default Value
================================================= ===========================================================================
``router.bind.port``                                 ``10000``
``security.auth.server.bind.port``                   ``10009``
``dashboard.bind.port``                              ``9999``
================================================= ===========================================================================

With SSL:

================================================= ===========================================================================
   Property                                          Default Value
================================================= ===========================================================================
``router.ssl.bind.port``                             ``10443``
``security.auth.server.ssl.bind.port``               ``10010``
``dashboard.ssl.bind.port``                          ``9443``
================================================= ===========================================================================


Configuring SSL for the Authentication Server
.............................................
To configure the granting of ``AccessToken``\s via SSL, add these properties to ``cdap-security.xml``:

================================================= ==================== ======================================================
   Property                                          Default Value         Description
================================================= ==================== ======================================================
``security.auth.server.ssl.keystore.path``           *None*             Keystore file location, either absolute
                                                                        or relative; the file should be owned and 
                                                                        readable only by the CDAP user
``security.auth.server.ssl.keystore.password``       *None*             Keystore password
``security.auth.server.ssl.keystore.keypassword``    *None*             Keystore key password
``security.auth.server.ssl.keystore.type``           ``JKS``            Keystore file type
================================================= ==================== ======================================================


Configuring SSL for the Router
..............................
To configure SSL for the Router, add these properties to ``cdap-security.xml``:

================================================= ==================== ======================================================
   Property                                          Default Value         Description
================================================= ==================== ======================================================
``router.ssl.keystore.path``                         *None*             Keystore file location, either absolute
                                                                        or relative; the file should be owned and 
                                                                        readable only by the CDAP user
``router.ssl.keystore.password``                     *None*             Keystore password
``router.ssl.keystore.keypassword``                  *None*             Keystore key password
``router.ssl.keystore.type``                         ``JKS``            Keystore file type
================================================= ==================== ======================================================

Configuring SSL for the CDAP Console
....................................
To enable SSL for the CDAP Console, add these properties to ``cdap-security.xml``:

================================================= ===========================================================================
   Property                                          Default Value
================================================= ===========================================================================
``dashboard.ssl.cert``                            SSL cert file location, either absolute or relative; 
                                                  the file should be owned and readable only by the CDAP user
``dashboard.ssl.key``                             SSL key file location, either absolute or relative; 
                                                  the file should be owned and readable only by the CDAP user
================================================= ===========================================================================

**Note:** To allow self-signed certificates, set the ``dashboard.ssl.disable.cert.check``
property to ``true`` in ``cdap-site.xml``.

.. _enable-access-logging:

Enabling Access Logging
.......................

.. highlight:: console

To enable access logging, add the following to ``logback.xml`` (typically under ``/etc/cdap/conf/``) ::

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

    <appender name="EXTERNAL_AUTH_AUDIT" class="ch.qos.logback.core.rolling.RollingFileAppender">
      <file>external_auth_access.log</file>
      <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
        <fileNamePattern>external_auth_access.log.%d{yyyy-MM-dd}</fileNamePattern>
        <maxHistory>30</maxHistory>
      </rollingPolicy>
      <encoder>
        <pattern>%msg%n</pattern>
      </encoder>
    </appender>
    <logger name="external-auth-access" level="TRACE" additivity="false">
      <appender-ref ref="EXTERNAL_AUTH_AUDIT" />
    </logger>

You may also configure the file being logged to by changing the path under ``<file>...</file>``.

Configuring Authentication Mechanisms
-------------------------------------
CDAP provides several ways to authenticate a client’s identity:

- :ref:`installation-basic-authentication`
- :ref:`installation-ldap-authentication`
- :ref:`installation-jaspi-authentication`
- :ref:`installation-custom-authentication`

.. _installation-basic-authentication:

Basic Authentication
....................
The simplest way to identity a client is to authenticate against a realm file.
To configure basic authentication add the following properties to ``cdap-site.xml``:

========================================================== ==================================================================
   Property                                                   Value
========================================================== ==================================================================
``security.authentication.handlerClassName``               ``co.cask.cdap.security.server.BasicAuthenticationHandler``
``security.authentication.basic.realmfile``                ``<path>`` *(either absolute or relative)*
========================================================== ==================================================================

The realm file is of the following format::

  username: password[,rolename ...]

Note that it is not advisable to use this method of authentication. In production, we recommend using any of the
other methods described below.

.. _installation-ldap-authentication:

LDAP Authentication
...................
You can configure CDAP to authenticate against an LDAP instance by adding these
properties to ``cdap-site.xml``:

========================================================== ==================================================================
   Property                                                   Value
========================================================== ==================================================================
``security.authentication.handlerClassName``               ``co.cask.cdap.security.server.LDAPAuthenticationHandler``
``security.authentication.loginmodule.className``          ``co.cask.cdap.security.server.LDAPLoginModule``
``security.authentication.handler.debug``                  ``true/false``
``security.authentication.handler.hostname``               ``<hostname>``
``security.authentication.handler.port``                   ``<port>``
``security.authentication.handler.userBaseDn``             ``<userBaseDn>``
``security.authentication.handler.userRdnAttribute``       ``<userRdnAttribute>``
``security.authentication.handler.userObjectClass``        ``<userObjectClass>``
========================================================== ==================================================================

In addition, you may configure these optional properties in ``cdap-site.xml``:

========================================================== ==================================================================
   Property                                                   Value
========================================================== ==================================================================
``security.authentication.handler.bindDn``                    ``<bindDn>``
``security.authentication.handler.bindPassword``              ``<bindPassword>``
``security.authentication.handler.userIdAttribute``           ``<userIdAttribute>``
``security.authentication.handler.userPasswordAttribute``     ``<userPasswordAttribute>``
``security.authentication.handler.roleBaseDn``                ``<roleBaseDn>``
``security.authentication.handler.roleNameAttribute``         ``<roleNameAttribute>``
``security.authentication.handler.roleMemberAttribute``       ``<roleMemberAttribute>``
``security.authentication.handler.roleObjectClass``           ``<roleObjectClass>``
========================================================== ==================================================================

To enable SSL between the authentication server and the LDAP instance, configure
these properties in ``cdap-site.xml``:

========================================================== ================= ================================================
   Property                                                   Value             Default Value
========================================================== ================= ================================================
``security.authentication.handler.useLdaps``                ``true/false``          ``false``
``security.authentication.handler.ldapsVerifyCertificate``  ``true/false``          ``true``
========================================================== ================= ================================================

.. _installation-jaspi-authentication:

JASPI Authentication
....................
To authenticate a user using JASPI (Java Authentication Service Provider Interface) add 
the following properties to ``cdap-site.xml``:

========================================================== ==================================================================
   Property                                                   Value
========================================================== ==================================================================
``security.authentication.handlerClassName``               ``co.cask.cdap.security.server.JASPIAuthenticationHandler``
``security.authentication.loginmodule.className``          ``<custom-login-module>``
========================================================== ==================================================================

In addition, any properties with the prefix ``security.authentication.handler.``,
such as ``security.authentication.handler.hostname``, will be provided to the handler.
These properties, stripped of the prefix, will be used to instantiate the 
``javax.security.auth.login.Configuration`` used by the ``LoginModule``.

.. _installation-custom-authentication:

Custom Authentication
.....................

To use a Custom Authentication mechanism, set the
``security.authentication.handlerClassName`` in ``cdap-site.xml`` with the custom
handler's classname. Any properties set in ``cdap-site.xml`` are available through a
``CConfiguration`` object and can be used to configure the handler. 

To make your custom handler class available to the authentication service, copy your
packaged jar file (and any additional dependency jars) to the ``security/lib/`` directory
within your CDAP installation (typically under ``/opt/cdap``).

The Developers’ Manual :ref:`Custom Authentication <developers-custom-authentication>` section shows
how to create a Custom Authentication Mechanism.


Testing Security
----------------

.. highlight:: console

As described in the :ref:`CDAP Reference Manual <http-restful-api-conventions>`, the
**base URL** (represented by ``<base-url>``) that clients can use for the HTTP RESTful API is::

  http://<host>:<port>/v2

Note that if :ref:`SSL is enabled for CDAP Servers<running_servers_with_ssl>`, then the
base URL will use ``https``.

To ensure that you've configured security correctly, run these simple tests to verify that the
security components are working as expected:

.. highlight:: console

- After configuring CDAP as described above, start (or restart) CDAP and attempt to make a request::

    curl -v <base-url>/apps
	
 such as::
	
    curl -vw '\n' http://localhost:10000/v2/apps

 This should return a ``401 Unauthorized`` response with a list of authentication URIs in
 the response body. For example::

    {"auth_uri":["http://localhost:10009/token"]}

- Submit a username and password to one of the URLs to obtain an ``AccessToken``::

    curl -vw '\n' -u username:password <auth-url>
	
 such as (assuming an authentication server at the above URI and that you have defined a 
 username:password pair such as *cdap:realtime*)::
	
    curl -vw '\n' -u cdap:realtime http://localhost:10009/token

 This should return a ``200 OK`` response with the ``AccessToken`` string in the response
 body (formatted to fit)::

    {"access_token":"AghjZGFwAI7e8p65Uo7OpfG5UrD87psGQE0u0sFDoqxtacdRR5GxEb6bkTypP7mXdqvqqnLmfxOS",
      "token_type":"Bearer","expires_in":86400}

- Reattempt the first command, but this time include the ``AccessToken`` as a header in the request::

    curl -vw '\n' -H "Authorization: Bearer <AccessToken>" <base-url>/apps
	  
 such as (formatted to fit)::
	
    curl -vw '\n' -H "Authorization: Bearer 
      AghjZGFwAI7e8p65Uo7OpfG5UrD87psGQE0u0sFDoqxtacdRR5GxEb6bkTypP7mXdqvqqnLmfxOS" 
      http://localhost:10000/v2/apps

 This should return a ``200 OK`` response.

- Visiting the CDAP Console should redirect you to a login page that prompts for credentials.
  Entering the credentials that you have configured should let you work with the CDAP Console as normal.

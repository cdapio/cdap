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

.. _enabling-security:

Enabling Security
-----------------
To enable security in CDAP, add these properties to ``cdap-site.xml``:

============================================= ===============================================================
   Property                                     Value
============================================= ===============================================================
security.enabled                                true
security.auth.server.address                    <hostname>
============================================= ===============================================================

Configuring Kerberos (required)
...............................
To configure Kerberos authentication for various CDAP services, add these properties to ``cdap-site.xml``:

============================================= ===================== =========================================
   Property                                     Default Value         Description
============================================= ===================== =========================================
kerberos.auth.enabled                          ``security.enabled``   true to enable Kerberos authentication
cdap.master.kerberos.keytab                    None                   Kerberos keytab file location
cdap.master.kerberos.principal                 None                   Kerberos principal associated with
                                                                      the keytab
============================================= ===================== =========================================

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

Running Servers with SSL
........................

To enable running servers with SSL in CDAP, add this property to ``cdap-site.xml``:

============================================= ===============================================================
   Property                                     Value
============================================= ===============================================================
ssl.enabled                                     true
============================================= ===============================================================

Default Ports
.............

Without SSL:

============================================= ===============================================================
   Property                                     Default Value
============================================= ===============================================================
router.bind.port                                10000
security.auth.server.bind.port                  10009
dashboard.bind.port                             9999
============================================= ===============================================================

With SSL:

============================================= ===============================================================
   Property                                     Default Value
============================================= ===============================================================
router.ssl.bind.port                            10443
security.auth.server.ssl.bind.port              10010
dashboard.ssl.bind.port                         9443
============================================= ===============================================================


Configuring SSL for the Authentication Server
.............................................
To configure the granting of ``AccessToken``\s via SSL, add these properties to ``cdap-security.xml``:

============================================= ===================== =========================================
   Property                                     Default Value         Description
============================================= ===================== =========================================
security.auth.server.ssl.keystore.path              None              Keystore file location; the file should
                                                                      be owned and readable only by the
                                                                      CDAP user
security.auth.server.ssl.keystore.password          None              Keystore password
security.auth.server.ssl.keystore.keypassword       None              Keystore key password
security.auth.server.ssl.keystore.type              JKS               Keystore file type
============================================= ===================== =========================================


Configuring SSL for the Router
..............................
To configure SSL for the Router, add these properties to ``cdap-security.xml``:

============================================= ===================== =========================================
   Property                                     Default Value         Description
============================================= ===================== =========================================
router.ssl.keystore.path                             None             Keystore file location; the file should
                                                                      be owned and readable only by the
                                                                      CDAP user
router.ssl.keystore.password                         None             Keystore password
router.ssl.keystore.keypassword                      None             Keystore key password
router.ssl.keystore.type                             JKS              Keystore file type
============================================= ===================== =========================================

Configuring SSL for UI
......................
To enable SSL for the Web-UI, add these properties to ``cdap-security.xml``:

============================================= ===============================================================
   Property                                     Default Value
============================================= ===============================================================
dashboard.ssl.cert                             SSL cert file location; the file should
                                               be owned and readable only by the CDAP user
dashboard.ssl.key                              SSL key file location; the file should
                                               be owned and readable only by the CDAP user
============================================= ===============================================================

**Note:** To allow self signed certificates, set dashboard.ssl.disable.cert.check field to true in cdap-site.xml

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
CDAP provides several ways to authenticate a clients’s identity:

- :ref:`installation-basic-authentication`
- :ref:`installation-ldap-authentication`
- :ref:`installation-jaspi-authentication`
- :ref:`installation-custom-authentication`

.. _installation-basic-authentication:

Basic Authentication
....................
The simplest way to identity a client is to authenticate against a realm file.
To configure basic authentication add the following properties to ``cdap-site.xml``:

====================================================== =========================================================
   Property                                             Value
====================================================== =========================================================
security.authentication.handlerClassName                co.cask.cdap.security.server.BasicAuthenticationHandler
security.authentication.basic.realmfile                 <path>
====================================================== =========================================================

The realm file is of the following format::

  username: password[,rolename ...]

Note that it is not advisable to use this method of authentication. In production, we recommend using any of the
other methods described below.

.. _installation-ldap-authentication:

LDAP Authentication
...................
You can configure CDAP to authenticate against an LDAP instance by adding these
properties to ``cdap-site.xml``:

====================================================== =========================================================
   Property                                             Value
====================================================== =========================================================
security.authentication.handlerClassName                co.cask.cdap.security.server.LDAPAuthenticationHandler
security.authentication.loginmodule.className           co.cask.cdap.security.server.LDAPLoginModule
security.authentication.handler.debug                   true/false
security.authentication.handler.hostname                <hostname>
security.authentication.handler.port                    <port>
security.authentication.handler.userBaseDn              <userBaseDn>
security.authentication.handler.userRdnAttribute        <userRdnAttribute>
security.authentication.handler.userObjectClass         <userObjectClass>
====================================================== =========================================================

In addition, you may configure these optional properties in ``cdap-site.xml``:

====================================================== =========================================================
   Property                                               Value
====================================================== =========================================================
security.authentication.handler.bindDn                    <bindDn>
security.authentication.handler.bindPassword              <bindPassword>
security.authentication.handler.userIdAttribute           <userIdAttribute>
security.authentication.handler.userPasswordAttribute     <userPasswordAttribute>
security.authentication.handler.roleBaseDn                <roleBaseDn>
security.authentication.handler.roleNameAttribute         <roleNameAttribute>
security.authentication.handler.roleMemberAttribute       <roleMemberAttribute>
security.authentication.handler.roleObjectClass           <roleObjectClass>
====================================================== =========================================================

To enable SSL between the authentication server and the LDAP instance, configure
these properties in ``cdap-site.xml``:

====================================================== ================= =======================================
   Property                                                Value                     Default Value
====================================================== ================= =======================================
security.authentication.handler.useLdaps                   true/false                   false
security.authentication.handler.ldapsVerifyCertificate     true/false                   true
====================================================== ================= =======================================

.. _installation-jaspi-authentication:

JASPI Authentication
....................
To authenticate a user using JASPI (Java Authentication Service Provider Interface) add 
the following properties to ``cdap-site.xml``:

====================================================== =========================================================
   Property                                             Value
====================================================== =========================================================
security.authentication.handlerClassName                co.cask.cdap.security.server.JASPIAuthenticationHandler
security.authentication.loginmodule.className           <custom-login-module>
====================================================== =========================================================

In addition, any properties with the prefix ``security.authentication.handler.``,
such as ``security.authentication.handler.hostname``, will be provided to the handler.
These properties, stripped off the prefix, will be used to instantiate the ``javax.security.auth.login.Configuration`` used
by the ``LoginModule``.

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

The Developers’ Manual :ref:`Custom Authentication <custom-authentication>` section shows
how to create a Custom Authentication Mechanism.


Testing Security
----------------

.. highlight:: console

From here on out we will use::

  <base-url>

to represent the base URL that clients can use for the HTTP REST API::

  http://<host>:<port>

and::

  <base-auth-url>

to represent the base URL that clients can use for obtaining access tokens::

  http://<host>:<auth-port>

where ``<host>`` is the host name of the CDAP server, ``<port>`` is the port that is set as the ``router.bind.port``
in ``cdap-site.xml`` (default: ``10000``), and ``<auth-port>`` is the port that is set as the
``security.auth.server.bind.port`` (default: ``10009``).

Note that if SSL is enabled for CDAP, then the base URL uses ``https``, ``<port>`` becomes the port that is set
as the ``router.ssl.bind.port`` in ``cdap-site.xml`` (default: ``10443``), and ``<auth-port>`` becomes the port that
is set as the ``security.auth.server.ssl.bind.port`` (default: ``10010``).

To ensure that you've configured security correctly, run these simple tests to verify that the
security components are working as expected:

- After configuring CDAP as described above, restart CDAP and attempt to use a service::

	curl -v <base-url>/apps

- This should return a 401 Unauthorized response. Submit a username and password to obtain an ``AccessToken``::

	curl -v -u username:password <base-auth-url>/token

- This should return a 200 OK response with the ``AccessToken`` string in the response body.
  Reattempt the first command, but this time include the ``AccessToken`` as a header in the command::

	curl -v -H "Authorization: Bearer <AccessToken>" <base-url>/apps

- This should return a 200 OK response.

- Visiting the CDAP Console should redirect you to a login page that prompts for credentials.
  Entering the credentials should let you work with the CDAP Console as normal.

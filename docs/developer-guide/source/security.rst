.. :Author: Continuuity, Inc.
   :Description: Reactor Security

=====================================
Reactor Security
=====================================

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
security.authentication.loginmodule.className       <custom login module>
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

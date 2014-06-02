.. :Author: Continuuity, Inc.
   :Description: Reactor Security

=====================================
Reactor Security
=====================================

Enabling Security
==================
To enable security add this property to ``continuuity-site.xml``:

==========================================  ===========
   Property                                   Value
==========================================  ===========
security.enabled                              true
==========================================  ===========


Configuring SSL
================
To configure SSL add these properties to ``continuuity-site.xml``:

==========================================  ===========
   Property                                   Value
==========================================  ===========
security.server.ssl.enabled                   true
security.server.ssl.keystore.path            <path>
security.server.ssl.keystore.password        <password>
==========================================  ===========

Enabling Access Logging
========================
To enable access logging add the following to ``logback.xml`` (typically under ``/etc/continuuity/conf/``) ::

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
You may also configure Reactor to authenticate against an LDAP instance.
To configure LDAP authentication add these properties to ``continuuity-site.xml``:

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



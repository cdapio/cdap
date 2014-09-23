.. :author: Cask Data, Inc.
   :description: Operating Cask Data Application Platform and its Console
   :copyright: Copyright © 2014 Cask Data, Inc.

==================================================================
CASK Data Application Platform Administrative and Operations Guide
==================================================================

Installation and Configuration
==============================

Introduction
------------

This guide is to help you install and configure Cask Data Application Platform (CDAP). It provides the
`system <#system-requirements>`__,
`network <#network-requirements>`__, and
`software requirements <#software-prerequisites>`__,
`packaging options <#packaging>`__, and
instructions for
`installation <#installation>`__ and
`verification <#verification>`__ of
the CDAP components so they work with your existing Hadoop cluster.

These are the CDAP components:

- **CDAP Web-App:** User interface—the *Console*—for managing
  CDAP applications;
- **CDAP Gateway:** Service supporting REST endpoints for CDAP;
- **CDAP-Master:** Service for managing runtime, lifecycle and resources of
  CDAP applications;
- **CDAP Kafka:** Metrics and logging transport service,
  using an embedded version of *Kafka*; and
- **CDAP Authentication Server:** Performs client authentication for CDAP when security
  is enabled.

  ``                                 ``

.. literal above is used to force an extra line break after list in PDF

Before installing the CDAP components, you must first install a Hadoop cluster
with *HDFS*, *YARN*, *HBase*, and *Zookeeper*. In order to use the ad-hoc querying capabilities
of CDAP, you will also need *Hive*. All CDAP components can be installed on the
same boxes as your Hadoop cluster, or on separate boxes that can connect to the Hadoop services.

Our recommended installation is to use two boxes for the CDAP components; the
`hardware requirements <#hardware-requirements>`__ are relatively modest,
as most of the work is done by the Hadoop cluster. These two
boxes provide high availability; at any one time, one of them is the leader
providing services while the other is a follower providing failover support.

Some CDAP components run on YARN, while others orchestrate the Hadoop cluster.
The CDAP Gateway service starts a router instance on each of the local boxes and instantiates
one or more gateway instances on YARN as determined by the gateway service configuration.

We have specific
`hardware <#hardware-requirements>`_,
`network <#network-requirements>`_ and
`prerequisite software <#software-prerequisites>`_ requirements detailed
`below <#system-requirements>`__
that need to be met and completed before installation of the CDAP components.


Conventions
...........
In this document, *client* refers to an external application that is calling the
CDAP using the HTTP interface.

*Application* refers to a user Application that has been deployed into the CDAP.

Text that are variables that you are to replace is indicated by a series of angle brackets (``< >``). For example::

  https://<username>:<password>@repository.cask.co

indicates that the texts ``<username>`` and  ``<password>`` are variables
and that you are to replace them with your values,
perhaps username ``john_doe`` and password ``BigData11``::

  https://john_doe:BigData11@repository.cask.co


System Requirements
-------------------

Hardware Requirements
.....................
Systems hosting the CDAP components must meet these hardware specifications,
in addition to having CPUs with a minimum speed of 2 GHz:

+---------------------------------------+--------------------+-----------------------------------------------+
| CDAP Component                        | Hardware Component | Specifications                                |
+=======================================+====================+===============================================+
| **CDAP Web-App**                      | RAM                | 1 GB minimum, 2 GB recommended                |
+---------------------------------------+--------------------+-----------------------------------------------+
| **CDAP Gateway**                      | RAM                | 2 GB minimum, 4 GB recommended                |
+---------------------------------------+--------------------+-----------------------------------------------+
| **CDAP-Master**                       | RAM                | 2 GB minimum, 4 GB recommended                |
+---------------------------------------+--------------------+-----------------------------------------------+
| **CDAP Kafka**                        | RAM                | 1 GB minimum, 2 GB recommended                |
+                                       +--------------------+-----------------------------------------------+
|                                       | Disk Space         | *CDAP Kafka* maintains a data cache in        |
|                                       |                    | a configurable data directory.                |
|                                       |                    | Required space depends on the number of       |
|                                       |                    | CDAP applications deployed and running        |
|                                       |                    | in the CDAP and the quantity                  |
|                                       |                    | of logs and metrics that they generate.       |
+---------------------------------------+--------------------+-----------------------------------------------+
| **CDAP Authentication Server**        | RAM                | 1 GB minimum, 2 GB recommended                |
+---------------------------------------+--------------------+-----------------------------------------------+


Network Requirements
....................
CDAP components communicate over your network with *HBase*, *HDFS*, and *YARN*.
For the best performance, CDAP components should be located on the same LAN,
ideally running at 1 Gbps or faster. A good rule of thumb is to treat CDAP
components as you would *Hadoop DataNodes*.  

.. rst2pdf: PageBreak

Software Prerequisites
......................
You'll need this software installed:

- Java runtime (on CDAP and Hadoop nodes)
- Node.js runtime (on CDAP nodes)
- Hadoop, HBase (and possibly Hive) environment to run against

Java Runtime
++++++++++++
The latest `JDK or JRE version 1.6.xx <http://www.java.com/en/download/manual.jsp>`__
for Linux and Solaris must be installed in your environment.

Once you have installed the JDK, you'll need to set the JAVA_HOME environment variable.

Node.js Runtime
+++++++++++++++
You can download the latest version of Node.js from `nodejs.org <http://nodejs.org>`__:

1. Download the appropriate Linux or Solaris binary ``.tar.gz`` from
   `nodejs.org/download/ <http://nodejs.org/download/>`__. #. Extract somewhere such as ``/opt/node-[version]/``
#. Build node.js; instructions that may assist are available at
   `github <https://github.com/joyent/node/wiki/Installing-Node.js-via-package-manager>`__
#. Ensure that ``nodejs`` is in the ``$PATH``. One method is to use a symlink from the installation:
   ``ln -s /opt/node-[version]/bin/node /usr/bin/node``

 
Hadoop/HBase Environment
++++++++++++++++++++++++

For a distributed enterprise, you must install these Hadoop components:

+---------------+-------------------+---------------------------------------------+
| Component     | Distribution      | Required Version                            |
+===============+===================+=============================================+
| **HDFS**      | Apache Hadoop DFS | 2.0.2-alpha or later                        |
+               +-------------------+---------------------------------------------+
|               | CDH or HDP        | (CDH) 4.2.x or later or (HDP) 2.0 or later  |
+---------------+-------------------+---------------------------------------------+
| **YARN**      | Apache Hadoop DFS | 2.0.2-alpha or later                        |
+               +-------------------+---------------------------------------------+
|               | CDH or HDP        | (CDH) 4.2.x or later or (HDP) 2.0 or later  |
+---------------+-------------------+---------------------------------------------+
| **HBase**     |                   | 0.94.2+, 0.96.0+, 0.98.0+                   |
+---------------+-------------------+---------------------------------------------+
| **Zookeeper** |                   | Version 3.4.3 or later                      |
+---------------+-------------------+---------------------------------------------+
| **Hive**      |                   | Version 12.0 or later                       |
+               +-------------------+---------------------------------------------+
|               | CDH or HDP        | (CDH) 4.3.x or later or (HDP) 2.0 or later  |
+---------------+-------------------+---------------------------------------------+

CDAP nodes require Hadoop and HBase client installation and configuration. No Hadoop
services need to be running.

Certain CDAP components need to reference your *Hadoop*, *HBase*, *YARN* (and possibly *Hive*)
cluster configurations by adding your configuration to their class paths.

.. rst2pdf: PageBreak

Prepare the Cluster
...................
To prepare your cluster so that CDAP can write to its default namespace,
create a top-level ``/cdap`` directory in HDFS, owned by an HDFS user ``yarn``::

  hadoop fs -mkdir /cdap && hadoop fs -chown yarn /cdap

In the CDAP packages, the default HDFS namespace is ``/cdap``
and the default HDFS user is ``yarn``. If you set up your cluster as above, no further changes are
required.

To make alterations to your setup, create an `.xml` file ``conf/cdap-site.xml``
(see the `Appendix <#appendix>`__) and set appropriate properties.

- If you want to use an HDFS directory with a name other than ``/cdap``:

  1. Create the HDFS directory you want to use, such as ``/myhadoop/myspace``.
  #. Create an ``hdfs.namespace`` property for the HDFS directory in ``conf/cdap-site.xml``::

       <property>
         <name>hdfs.namespace</name>
         <value>/myhadoop/myspace</value>
         <description>Default HDFS namespace</description>
       </property>


  #. Ensure that the default HDFS user ``yarn`` owns that HDFS directory.

- If you want to use a different HDFS user than ``yarn``:

  1. Check that there is—and create if necessary—a corresponding user on all machines
     in the cluster on which YARN is running (typically, all of the machines).
  #. Create an ``hdfs.user`` property for that user in ``conf/cdap-site.xml``::

       <property>
         <name>hdfs.user</name>
         <value>my_username</value>
         <description>User for accessing HDFS</description>
       </property>

  #. Check that the HDFS user owns the HDFS directory described by ``hdfs.namespace`` on all machines.

- To use the ad-hoc querying capabilities of CDAP, enable the CDAP Explore Service in
  ``conf/cdap-site.xml`` (by default, it is disabled)::

    <property>
      <name>cdap.explore.enabled</name>
      <value>true</value>
      <description>Enable Explore functionality</description>
    </property>

  **Note:** This feature cannot be used unless the cluster has a correct version of Hive installed.
  See *Hadoop/HBase Environment* above. This feature is currently not supported on secure Hadoop clusters.

.. rst2pdf: PageBreak

Secure Hadoop
+++++++++++++
When running CDAP on top of Secure Hadoop and HBase (using Kerberos
authentication), the CDAP Master process will need to obtain Kerberos credentials in order to
authenticate with Hadoop and HBase.  In this case, the setting for ``hdfs.user`` in
``cdap-site.xml`` will be ignored and the CDAP Master process will be identified as the
Kerberos principal it is authenticated as.

In order to configure CDAP Master for Kerberos authentication:

- Create a Kerberos principal for the user running CDAP Master.
- Install the ``k5start`` package on the servers where CDAP Master is installed.  This is used
  to obtain Kerberos credentials for CDAP Master on startup.
- Generate a keytab file for the CDAP Master Kerberos principal and place the file in
  ``/etc/security/keytabs/cdap.keytab`` on all the CDAP Master hosts.  The file should
  be readable only by the user running the CDAP Master process.
- Edit ``/etc/default/cdap-master``::

   REACTOR_KEYTAB="/etc/security/keytabs/cdap.keytab"
   REACTOR_PRINCIPAL="<cdap principal>@EXAMPLE.REALM.COM"

- When CDAP Master is started via the init script, it will now start using ``k5start``, which will
  first login using the configured keytab file and principal.

ULIMIT Configuration
++++++++++++++++++++
When you install the CDAP packages, the ``ulimit`` settings for the
CDAP user are specified in the ``/etc/security/limits.d/cdap.conf`` file.
On Ubuntu, they won't take effect unless you make changes to the ``/etc/pam.d/common-session file``.
For more information, refer to the ``ulimit`` discussion in the
`Apache HBase Reference Guide <https://hbase.apache.org/book.html#os>`__.

Packaging
---------
CDAP components are available as either Yum ``.rpm`` or APT ``.deb`` packages.
There is one package for each CDAP component, and each component may have multiple
services. Additionally, there is a base CDAP package with two utility packages
installed which creates the base configuration and the ``cdap`` user.
We provide packages for *Ubuntu 12* and *CentOS 6*.

Available packaging types:

- RPM: YUM repo
- Debian: APT repo
- Tar: For specialized installations only

CDAP packages utilize a central configuration, stored by default in ``/etc/cdap``.

When you install the CDAP base package, a default configuration is placed in
``/etc/cdap/conf.dist``. The ``cdap-site.xml`` file is a placeholder
where you can define your specific configuration for all CDAP components.

Similar to Hadoop, CDAP utilizes the ``alternatives`` framework to allow you to
easily switch between multiple configurations. The ``alternatives`` system is used for ease of
management and allows you to to choose between different directories to fulfill the
same purpose.

Simply copy the contents of ``/etc/cdap/conf.dist`` into a directory of your choice
(such as ``/etc/cdap/conf.mycdap``) and make all of your customizations there.
Then run the ``alternatives`` command to point the ``/etc/cdap/conf`` symlink
to your custom directory.

.. rst2pdf: PageBreak

RPM using Yum
.............
Create a file ``cdap.repo`` at the location::

  /etc/yum.repos.d/cdap.repo

The RPM packages are accessible using Yum at this authenticated URL::

  [cdap]
  name=CDAP Packages
  baseurl=https://<username>:<password>@repository.cask.co/content/groups/restricted
  enabled=1
  protect=0
  gpgcheck=0
  metadata_expire=30s
  autorefresh=1
  type=rpm-md

:where:
  :<username>: Username provided by your Cask representative
    :<password>: Password provided by your Cask representative

Debian using APT
................
Debian packages are accessible via APT on *Ubuntu 12*.

Create a file ``cdap.list`` at the location::

  /etc/apt/sources.list.d/cdap.list

Use this authenticated URL (on one line)::

  deb [ arch=amd64 ] https://<username>:<password>@repository.cask.co/content/sites/apt
            precise release

:where:
  :<username>: Username provided by your Cask representative
    :<password>: Password provided by your Cask representative


.. _installation:

Installation
------------
Install the CDAP packages by using either of these methods:

Using Yum::

  sudo yum install cdap-gateway cdap-kafka cdap-cdap-master cdap-security cdap-web-app

Using APT::

  sudo apt-get install cdap-gateway cdap-kafka cdap-cdap-master cdap-security cdap-web-app

Do this on each of the boxes that are being used for the CDAP components; our
recommended installation is a minimum of two boxes.

This will download and install the latest version of CDAP
with all of its dependencies. When all the packages and dependencies have been installed,
you can start the services on each of the CDAP boxes by running this command::

  for i in `ls /etc/init.d/ | grep cdap` ; do service $i restart ; done

When all the services have completed starting, the CDAP Console should then be
accessible through a browser at port 9999. The URL will be ``http://<app-fabric-ip>:9999`` where
``<app-fabric-ip>`` is the IP address of one of the machine where you installed the packages
and started the services.

Upgrading From a Previous Version
---------------------------------
When upgrade an existing CDAP installation from a previous version, you will need
to make sure the CDAP table definitions in HBase are up-to-date.

These steps will stop CDAP, update the installation, run an upgrade tool for the table definitions,
and then restart CDAP.

1. Stop all CDAP processes::

     for i in `ls /etc/init.d/ | grep cdap` ; do service $i stop ; done

#. Update the CDAP packages by running either of these methods:

   - Using Yum (on one line)::

       sudo yum install cdap cdap-gateway
                              cdap-hbase-compat-0.94 cdap-hbase-compat-0.96
                              cdap-kafka cdap-master
                              cdap-security cdap-web-app

   - Using APT (on one line)::

       sudo apt-get install cdap cdap-gateway
                              cdap-hbase-compat-0.94 cdap-hbase-compat-0.96
                              cdap-kafka cdap-master
                              cdap-security cdap-web-app

#. Run the upgrade tool (on one line)::

     /opt/cdap/cdap-master/bin/svc-master run
       com.cdap.data.tools.Main upgrade

#. Restart the CDAP processes::

     for i in `ls /etc/init.d/ | grep cdap` ; do service $i start ; done

Verification
------------
To verify that the CDAP software is successfully installed and you are able to use your
Hadoop cluster, run an example application.
We provide in our SDK pre-built ``.JAR`` files for convenience:

#. Download and install the latest CDAP Developer Suite from
   http://accounts.cask.co.

#. Extract to a folder (``CDAP_HOME``).
#. Open a command prompt and navigate to ``CDAP_HOME/examples``.
#. Each example folder has in its ``target`` directory a .JAR file.
   For verification, we will use the ``TrafficAnalytics`` example.
#. Open a web browser to the CDAP Web-App ("Console").
   It will be located on port ``9999`` of the box where you installed CDAP.
#. On the Console, click the button *Load an App.*
#. Find the pre-built JAR (`TrafficAnalytics-1.0.jar`) by using the dialog box to navigate to
   ``CDAP_HOME/examples/TrafficAnalytics/target/TrafficAnalytics-1.0.jar``
#. Once the application is deployed, instructions on running the example can be found at the
   `TrafficAnalytics example
   </http://docs.cask.co/cdap/current/examples/trafficAnalytics#building-and-running-the-application-and-example>`__.
#. You should be able to start the application, inject log entries,
   run the ``MapReduce`` job and see results.
#. When finished, stop and remove the application as described in the
   `TrafficAnalytics example
   <http://docs.cask.co/cdap/current/examples/trafficAnalytics#stopping-the-application>`__.

.. rst2pdf: PageBreak

Security
========

Cask Data Application Platform (CDAP) supports securing clusters using a perimeter security model.  With perimeter
security, access to cluster nodes is restricted through a firewall.  Cluster nodes can communicate
with each other, but outside clients can only communicate with the cluster through a secured
host.  Using CDAP security, the CDAP authentication server issues credentials (access
tokens) to authenticated clients.  Clients then send these credentials on requests to CDAP.
Calls that lack valid access tokens will be rejected, limiting access to only authenticated
clients.

Authentication in CDAP consists of two components:

- **Authentication Server** - the authentication server integrates with different authentication
  backends (LDAP, JASPI plugins) using a plugin API.  Clients must first authenticate with the
  authentication server through this configured backend.  Once authenticated, clients are issued
  an access token representing their identity.
- **CDAP Router** - the CDAP router serves as the secured host in the perimeter security
  model.  All client calls to the cluster go through the router, and must present a valid access
  token when security is enabled.

For more details on the authentication process, see `Client Authentication`_.

By enabling perimeter security for CDAP, you can prevent access by any clients without valid
credentials.  In addition, access logging can be enabled in CDAP to provide an audit log of all
operations.

We recommend that in order for CDAP to be secure, CDAP security should always be used in conjunction with
`secure Hadoop clusters <http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SecureMode.html>`__.
In cases where secure Hadoop is not or cannot be used, it is inherently insecure and any applications
running on the cluster are effectively "trusted”. Though there is still value in having the perimeter access
be authenticated in that situation, whenever possible a secure Hadoop cluster should be employed with CDAP security.

Enabling Security
-----------------
To enable security in CDAP, add these properties to ``cdap-site.xml``:

==========================================  ==============
   Property                                   Value
==========================================  ==============
security.enabled                              true
security.auth.server.address                  <hostname>
==========================================  ==============

Running Servers with SSL
........................

To enable running servers with SSL in CDAP, add this property to ``cdap-site.xml``:

==========================================  ==============
   Property                                   Value
==========================================  ==============
ssl.enabled                                   true
==========================================  ==============

Default Ports
.............
Without SSL:

=======================================   =================
   Property                               Default Value
=======================================   =================
router.bind.port                            10000
security.auth.server.bind.port              10009
dashboard.bind.port                         9999
=======================================   =================

With SSL:

==========================================  =================
   Property                                  Default Value
==========================================  =================
router.ssl.bind.port                          10443
security.auth.server.ssl.bind.port            10010
dashboard.ssl.bind.port                       9443
==========================================  =================


Configuring SSL for the Authentication Server
.............................................
To configure the granting of ``AccessToken``\s via SSL, add these properties to ``cdap-security.xml``:

=============================================     =====================     =======================================
   Property                                        Default Value                Description
=============================================     =====================     =======================================
security.auth.server.ssl.keystore.path              None                      Keystore file location. The file should
                                                                              be owned and readable only by the
                                                                              CDAP user
security.auth.server.ssl.keystore.password          None                      Keystore password
security.auth.server.ssl.keystore.keypassword       None                      Keystore key password
security.auth.server.ssl.keystore.type              JKS                       Keystore file type
=============================================     =====================     =======================================


Configuring SSL for the Router
..............................
To configure SSL for the Router, add these properties to ``cdap-security.xml``:

================================    =======================      ================================================
   Property                           Default Value                Description
================================    =======================      ================================================
router.ssl.keystore.path              None                         Keystore file location. The file should
                                                                   be owned and readable only by the
                                                                   CDAP user
router.ssl.keystore.password          None                         Keystore password
router.ssl.keystore.keypassword       None                         Keystore key password
router.ssl.keystore.type              JKS                          Keystore file type
================================    =======================      ================================================

Configuring SSL for UI
......................
To enable SSL for the Web-UI, add these properties to ``cdap-security.xml``:

=======================================          ============================================
   Property                                        Description
=======================================          ============================================
dashboard.ssl.cert                                  SSL cert file location. The file should
                                                    be owned and readable only by the CDAP
                                                    user
dashboard.ssl.key                                   SSL key file location. The file should
                                                    be owned and readable only by the CDAP
                                                    user
=======================================          ============================================

**Note:** To allow self signed certificates, set dashboard.ssl.disable.cert.check field to true in cdap-site.xml

Configuring Kerberos (required)
...............................
To configure Kerberos authentication for various CDAP services, add these properties to ``cdap-site.xml``:

==========================================  ========================  ==========================================
   Property                                   Default Value            Description
==========================================  ========================  ==========================================
kerberos.auth.enabled                         ``security.enabled``     true to enable Kerberos authentication
cdap.master.kerberos.keytab                   None                     Kerberos keytab file location
cdap.master.kerberos.principal                None                     Kerberos principal associated with
                                                                       the keytab
==========================================  ========================  ==========================================

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
CDAP provides several ways to authenticate a user's identity.

Basic Authentication
-------------------_
The simplest way to identity a user is to authenticate against a realm file.
To configure basic authentication add the following properties to ``cdap-site.xml``:

==========================================  ===========
   Property                                   Value
==========================================  ===========
security.authentication.handlerClassName     co.cask.cdap.security.server.BasicAuthenticationHandler
security.authentication.basic.realmfile      <path>
==========================================  ===========

The realm file is of the following format::

  username: password[,rolename ...]

Note that it is not advisable to use this method of authentication. In production, we recommend using any of the
other methods described below.

LDAP Authentication
...................
You can configure CDAP to authenticate against an LDAP instance by adding these
properties to ``cdap-site.xml``:

================================================  ===========
   Property                                         Value
================================================  ===========
security.authentication.handlerClassName            co.cask.cdap.security.server.LDAPAuthenticationHandler
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
.....................................................................
To authenticate a user using JASPI add the following properties to ``cdap-site.xml``:

================================================  ===========
   Property                                         Value
================================================  ===========
security.authentication.handlerClassName            co.cask.cdap.security.server.JASPIAuthenticationHandler
security.authentication.loginmodule.className       <custom-login-module>
================================================  ===========

In addition, any properties with the prefix ``security.authentication.handler.``,
such as ``security.authentication.handler.hostname``, will also be used by the handler.
These properties, without the prefix, will be used to instantiate the ``javax.security.auth.login.Configuration`` used
by the ``LoginModule``.

.. highlight:: java

Custom Authentication
.....................
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
additional dependency jars) to the ``security/lib/`` directory within your CDAP installation
(typically under ``/opt/cdap``).

Example Configuration
---------------------

.. highlight:: xml

This is what your ``cdap-site.xml`` could include when configured to enable security, SSL, and
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
    <value>co.cask.cdap.security.server.LDAPAuthenticationHandler</value>
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


Client Authentication
---------------------

CDAP provides support for authenticating clients using OAuth 2 Bearer tokens, which are issued
by the CDAP authentication server.  The authentication server provides the integration point
for all external authentication systems.  Clients authenticate with the authentication server as
follows:

.. image:: _images/auth_flow_simple.png
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

Obtaining an Access Token
.........................
Obtain a new access token by calling::

   GET <base-auth-url>/token

The required header and request parameters may vary according to the
external authentication mechanism that has been configured.  For username and password based mechanisms, the
``Authorization`` header may be used::

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
     - In order to conform with the OAuth 2.0 Bearer Token Usage specification (`RFC 6750`__), this
       value must be "Bearer".
   * - ``expires_in``
     - Token validity lifetime in seconds.

.. _rfc6750: http://tools.ietf.org/html/rfc6750

__ rfc6750_

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
- Only ``Bearer`` tokens (`RFC 6750`__) are currently supported

__ rfc6750_


Authentication with RESTful Endpoints
.....................................
When security is enabled on a CDAP cluster, only requests with a valid access token will be
allowed by CDAP.  Clients accessing CDAP RESTful endpoints will first need to obtain an access token
from the authentication server, as described above, which will be passed to the Router daemon on
subsequent HTTP requests.

The following request and response descriptions apply to all CDAP RESTful endpoints::

   GET <base-url>/<resource> HTTP/1.1

In order to authenticate, all client requests must supply the ``Authorization`` header::

   Authorization: Bearer wohng8Xae7thahfohshahphaeNeeM5ie

For CDAP issued access tokens, the authentication scheme must always be ``Bearer``.


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

.. rst2pdf: PageBreak

Monitoring
==========

CDAP collects logs and metrics for all of its internal services. Being able to view these details can be really
helpful in debugging CDAP Applications as well as analyzing their performance. CDAP gives access to its logs, metrics,
and other monitoring information through REST APIs as well as a Java Client.

See the `Logging <api.html#logging-http-api>`__, `Metrics <api.html#metrics-http-api>`__,
and `Monitoring <api.html#monitor-http-api>`__ APIs for more information.

.. rst2pdf: PageBreak

Troubleshooting
===============
Here are some selected examples of potential problems and possible resolutions.

Application Won't Start
-----------------------
Check HDFS write permissions. It should show an obvious exception in the YARN logs.
 
No Metrics/logs
---------------
Make sure the *Kafka* server is running, and make sure local the logs directory is created and accessible.
On the initial startup, the number of available seed brokers must be greater than or equal to the
*Kafka* default replication factor.

In a two-box setup with a replication factor of two, if one box fails to startup,
metrics will not show up though the application will still run::

  [2013-10-10 20:48:46,160] ERROR [KafkaApi-1511941310]
        Error while retrieving topic metadata (kafka.server.KafkaApis)
        kafka.admin.AdministrationException:
               replication factor: 2 larger than available brokers: 1

Only the First Flowlet Showing Activity
---------------------------------------
Check that YARN has the capacity to start any of the remaining containers.
 
YARN Application Shows ACCEPTED For Some Time But Then Fails
------------------------------------------------------------
It's possible that YARN can't extract the .JARs to the ``/tmp``,
either due to a lack of disk space or permissions.


Log Saver Process Throws an Out-of-Memory Error, CDAP Console Shows Service Not OK
----------------------------------------------------------------------------------

The CDAP Log Saver uses an internal buffer that may overflow and result in Out-of-Memory
Errors when applications create excessive amounts of logs. One symptom of this is that the CDAP
Console *Services Explorer* shows the ``log.saver`` Service as not OK, in addition to seeing error
messages in the logs.

By default, the buffer keeps 8 seconds of logs in memory and the Log Saver process is limited to 1GB of
memory. When it's expected that logs exceeding these settings will be produced, increase the memory
allocated to the Log Saver or increase the number of Log Saver instances. If the cluster has limited
memory or containers available, you can choose instead to decrease the duration of logs buffered in
memory. However, decreasing the buffer duration may lead to out-of-order log events.

In the ``cdap-site.xml``, you can:

- Increase the memory by adjusting ``log.saver.run.memory.megs``;
- Increase the number of Log Saver instances using ``log.saver.num.instances``; and
- Adjust the duration of logs with ``log.saver.event.processing.delay.ms``.

Note that it is recommended that ``log.saver.event.processing.delay.ms`` always be kept greater than
``log.saver.event.bucket.interval.ms`` by at least a few hundred (300-500) milliseconds.

See the ``log.saver`` parameter section of the `Appendix <#appendix>`__ for a list of these
configuration parameters and their values that can be adjusted.

.. rst2pdf: PageBreak

**Introduction to Running Applications and Operating the Cask Data Application Platform (CDAP)**

Putting CDAP into Production
============================

The Cask Data Application Platform (CDAP) can be run in different modes: in-memory mode for unit testing, 
Standalone CDAP for testing on a developer's laptop, and Distributed CDAP for staging and production.

Regardless of the runtime edition, CDAP is fully functional and the code you develop never changes. 
However, performance and scale are limited when using in-memory or standalone CDAPs.

In-memory CDAP
--------------
The in-memory CDAP allows you to easily run CDAP for use in unit tests. In this mode, the underlying Big Data infrastructure is emulated using in-memory data structures and there is no persistence. The CDAP Console is not available in this mode.

Standalone CDAP
---------------

The Standalone CDAP allows you to run the entire CDAP stack in a single Java Virtual Machine on your local machine and includes a local version of the CDAP Console. The underlying Big Data infrastructure is emulated on top of your local file system. All data is persisted.

The Standalone CDAP by default binds to the localhost address, and is not available for remote access by any outside process or application outside of the local machine.

See the `Cask Data Application Platform Quick Start <quickstart.html>`__ and
the *Cask Data Application Platform SDK* for information on how to start and manage your Standalone CDAP.


Distributed Data Application Platform
-------------------------------------

The Distributed CDAP runs in fully distributed mode. In addition to the system components of the CDAP, distributed and highly available deployments of the underlying Hadoop infrastructure are included. Production applications should always be run on a Distributed CDAP.

To learn more about getting your own Distributed CDAP, see `Cask Products <http://cask.co/products>`__.


.. _console:

CDAP Console
============

Overview
--------

The **CDAP Console** is available for deploying, querying and managing the Cask Data Application Platform in all modes of CDAP except an 
`In-memory CDAP <#in-memory-data-application-platform>`__.

.. image:: _images/console/console_01_overview.png
   :width: 600px

Here is a screen-capture of the CDAP Console running on a Distributed CDAP.

Down the left sidebar, underneath the **Cask** logo, is the CDAP mode identifier (in this case, *Distributed CDAP*), followed by four buttons:
*Application*, `Process`_, `Store`_ and `Query`_. These buttons gives you access to CDAP Console facilities for managing each of these CDAP elements.

In the far upper-right are two buttons: the `Metrics <#metrics-explorer>`__ and
`Services <#services-explorer>`__ buttons, which take you to their respective explorers.

.. _sampling-menu:

In the upper right portion of the display is a menu and, in the Distributed version of 
CDAP, two buttons (*CDAP* and *Resources*).
The menu is the **Sampling Menu**, which appears on almost every pane of the
CDAP Console. 

The menu determines how much data is sampled in the presentation shown on the CDAP Console in
realtime:

.. image:: _images/console/console_10_app_crawler_detail_menu.png
   :width: 200px

By default, the sampling menu is set at "Last 1 Minute", indicating that the graphs are showing
the last one minute of activity. Be aware that changing the menu to a longer period (such as "Last 
1 Hour") can adversely affect the performance of the CDAP Instance and should only be used for short sessions before returning the setting to the default "Last 1 Minute".

This starting overview, showing which Applications (*Apps*) are currently
installed, and realtime graphs of *Collect*, *Process*, *Store*, and *Query*.
Each statistic is per unit of time—events per second, bytes (or larger) per second, queries per second—and
are sampled and reported based on the sampling menu in the upper right. (In Distributed CDAP, this starting overview can be reached by the **CDAP** button.)

The lower portion of the screen shows all the Apps along with their name, description, and what is happening with each:

- *Collect*, the number of Streams consumed by the Application;

- *Process*, the number of Flows created by the Application;

- *Store*, the number of DataStores used by the Application;

- *Query*, the number of Procedures in the Application; and

- *Busyness*, the percentage of time spent processing events by the Application.

.. _busyness:

Busyness—the percentage of time spent processing events—is a concept that is used extensively in the CDAP Console.

Clicking on the name of an Application will take you to the `App's pane <#application>`__, with details about the application.

:Note: Because of the interconnections in the CDAP Console, you can arrive at the same pane by different paths.
       Remember that the left pane buttons will always take you back to the initial summary panes.

The **Resources** button (available in Distributed CDAP) gives a look at what is being used by the CDAP:

.. image:: _images/console/console_02_overview_resources.png
   :width: 600px

Once again, the top half shows four different elements, all in realtime:
*AppFabric* consumption (in Yarn containers),
*Processors* used (in the number of cores),
*Memory* consumption (total bytes available and used memory), and
*DataFabric* storage (total bytes available and used disk space).

*Containers* refers to the number of Yarn containers; for example, each Flowlet instance uses a single container.

Statistics and graphs are sampled based on the setting of the sampling menu in the upper-right.

The lower half shows the list of deployed applications, their descriptions, along with each app's container, core
and memory use in realtime.

The triangles to the left of each application turn to reveal the hierarchy of resources being used by each
application's Flows and Flowlets. You can use this tree to drill down into any part of the CDAP.

The footer of each pane gives—below the *Cask Data, Inc.* copyright—five buttons
and the version of the CDAP that you are using.

.. _reset:

The five buttons provide access to the `terms of use <http://cask.co/terms>`__,
the `privacy policy <http://cask.co/privacy>`__,
contacting `Cask <http://cask.co/contact-us>`__,
contacting Cask support, and *Reset*, for resetting the CDAP.

*Reset* deletes all data and applications from the
CDAP, is irreversible, and returns the CDAP to an original state. The button is only visible and
available if the CDAP has been started with the system property ``enable.unrecoverable.reset`` as ``true``. 

Metrics Explorer
----------------

In the top portion of the `Overview image <#console>`__ you can see the **Metrics** button, which takes you to the
*Metrics Explorer:*

.. image:: _images/console/console_18_metrics_explorer1.png
   :width: 600px

Here you can monitor a variety of different statistics for elements of the CDAP.
You add a metric by clicking the *Add* button; it will give you a dialog
where you can specify an element and then pick from a list of appropriate metrics.

.. image:: _images/console/console_20_metrics_explorer3.png
   :width: 200px

As with other CDAP Console realtime graphs, you specify the sampling rate through a pop-down menu in the
upper-right. You can *Pause* the sampling to prevent excessive load on the CDAP.

If you move your mouse over the graph, you will get detailed information about the statistics presented:

.. image:: _images/console/console_19_metrics_explorer2.png
   :width: 600px

System Services Explorer
------------------------
In the top portion of the `Overview image <#console>`__, to the right of the **Metrics** button is the
**Services** button, which takes you to the *Services Explorer:*


.. image:: _images/console/console_31_services_explorer.png
   :width: 600px

Here you can monitor a variety of different System Services of the CDAP. For each service name, status
is given, if logs are available (and link to them if so), the number of instances requested and
provisioned.

.. _Collect:

Collect
-------
.. image:: _images/console/console_03_collect.png
   :width: 600px

The **Collect** pane shows all the Streams collecting data and their details: name, storage, number of events and the arrival rate, with a graph showing arrivals based on the sampling rate menu setting.

.. _Stream:

Clicking on a Stream's name will take you to the Stream's pane:

.. image:: _images/console/console_21_stream.png
   :width: 600px

The Stream pane shows the details of the number of events per second currently in the Stream,
the storage and a graph of events over the last sampling period, and a list of all the Flows
that are attached to the Stream, with processing rate and `busyness`_ for each Flow.
Clicking on a Flow name will take you to that `Flow's pane <#flow>`__.


.. _Process:

Process
-------

.. image:: _images/console/console_04_process.png
   :width: 600px

The **Process** pane shows all the
`Flows <#flow>`__,
`MapReduce <#mapreduce>`__ and
`Workflows <#workflow>`__ in the CDAP
with their name and status (either *Running* or *Stopped*).
Each name links to the individual elements detail pane.
Graphs show statistics based on the sampling rate menu setting.

In the case of Flows, it shows the processing rate in events per second and `busyness`_. For MapReduce, it shows the mapping status and the reducing status.


.. _Store:

Store
-----

.. image:: _images/console/console_05_store.png
   :width: 600px

The **Store** pane shows all the Datasets currently specified in the CDAP, along with their name
(a link to the detail pane for the Dataset), type (the Java class), storage in use,
a realtime write-rate graph and the current write rate (bytes per second). It has button that accesses the
`Dataset Explorer`_.


Dataset Explorer
................
From within the `Store`_ pane you can access the Dataset Explorer, which allows for SQL-like
queries of the datasets' underlying Hive tables. Details on the requirements for formulating and
performing these queries can be found in the Developer Guide `Querying Datasets with SQL <query.html>`__.

Using the information supplied for each Hive table (schema, keys, properties) you can generate a
SQL-like query and then execute it.

.. image:: _images/console/console_33_query_explorer.png
   :width: 600px

When the query has completed, it will be listed on the *Results* pane of the Explorer. The results
can either be viewed directly or downloaded to your computer.

.. image:: _images/console/console_35_query_explorer.png
   :width: 600px

Double-clicking on the results will reveal them in the browser:

.. image:: _images/console/console_37_query_explorer.png
   :width: 600px

If no results are available, the "Download" icon will be greyed-out and hovering over it will display a
message "Results Not Available".

.. image:: _images/console/console_36_query_explorer.png
   :width: 600px


.. _Query:

Query
-----

.. image:: _images/console/console_06_query.png
   :width: 600px

The **Query** pane shows all the Procedures currently specified in the CDAP, along with their name
(a link to the detail pane for the Procedure), status and realtime graphs
of their request and error rates.


.. _application:

Application
-----------

.. image:: _images/console/console_14_app_crawler.png
   :width: 600px

The Application pane shows details for an individual application deployed in the CDAP:

- **Summary graphs:** across the top, left to right, a summary of events per second processed,
  `busyness`_ and storage;

- **Collect:** Streams, with name (a link to details) and summary statistics;

- **Process:** Flows, with name (a link to details), summary statistics,
  and a management button to start and stop all the Flows associated with this app;

- **Store:** Datasets defined by this Application, with name (a link to details)
  and summary statistics; and

- **Query:** Procedures, with name (a link to details) and summary statistics,
  and a management button to start and stop all the Procedures associated with this app;

- **Service:** Services, with name (a link to details) and number of components,
  and a management button to start and stop all the Services associated with this app.

Deleting an Application
.......................

The button in the upper right of the pane allows you to delete the current Application:

.. image:: _images/console/console_22_app_crawler_detail_delete.png
   :width: 200px

However, before an Application can be deleted, all Process—Flows and MapReduce Jobs—and Queries (Procedures), must be stopped.
An error message will be given if you attempt to delete an Application with running components.

Note that Streams and Datasets, even though they are specified and created at the time of deployment of the Application,
are persistent and are not deleted when an Application is deleted.

To delete these, the CDAP needs to be reset using the `Reset button <#reset>`__ located at the bottom of each pane.


.. _flow:

Flow
----

Each Flow has a management pane, which shows the status, log and history of a Flow.


Flow Status
...........
Start by looking at the status of a Flow:

.. image:: _images/console/console_07_app_crawler_flow_rss.png
   :width: 600px

It shows all of the Streams and Flowlets of the Flow with their connections and icons arranged in a
directed acyclic graph or DAG.

Across the top are two realtime graphs of processing rate and `busyness`_ with
current Flow status and management controls.

.. image:: _images/console/console_11_app_crawler_detail.png
   :width: 200px

The upper-right portion has a cluster of buttons:

- Status, Log and History buttons that switch you between the panes of the Flow presentation;

- `Sampling menu <#sampling-menu>`__;

- Current status (*Running* or *Paused*);

- Gear icon for runtime configuration settings; and

- Start and stop buttons for the Flow.

The gear icon brings up a dialog for setting the runtime configuration parameters
that have been built into the Flow:

.. image:: _images/console/console_23_app_crawler_detail_config.png
   :width: 400px

The directed acyclic graph (DAG) shows all the Streams and Flowlets:

.. image:: _images/console/console_24_app_crawler_detail_dag.png
   :width: 600px

A Stream icon shows the name of the Stream and the number of events processed in the current sampling period:

.. image:: _images/console/console_12_stream_icon.png
   :width: 200px

A Flowlet icon shows the name of the Flowlet, the number of events processed
in the current sampling period,
and—in a small circle in the upper right of the icon—the number of instances of that Flowlet:

.. image:: _images/console/console_13_flowlet_icon.png
   :width: 200px


DAG Icon Dialogs
................

Clicking on an icon in the DAG brings up the icon's dialog. This dialog contains numerous buttons and panes,
and allows you to traverse the DAG completely by selecting appropriate inputs and outputs.

.. image:: _images/console/console_27_dag1.png
   :width: 400px

Here we have clicked on a Flowlet named *counter*, and are seeing the first
(*Inputs*) of three panes in this dialog. On the left is a list of inputs to the Flowlet,
in this case a single input Stream named *parser*, and realtime statistics for the flowlet.

Clicking the name *parser* would take you—without leaving the dialog—backwards on the path
of the DAG, and allow you to traverse towards the start of the path.

If you go all the way to the beginning of the path, you will reach a Stream, and the dialog will change:

.. image:: _images/console/console_30_dag4.png
   :width: 400px

Here, you can inject an Event into the Stream simply by typing and pressing the *Inject* button.
(Notice that once you have reached a Stream, there is no way to leave on the DAG. There
is no list of consumers of the Stream.)

Returning to the `original dialog <#dag-icon-dialogs>`__, clicking the "Processed" button in the center takes you to the second pane of the dialog.

.. image:: _images/console/console_28_dag2.png
   :width: 400px

Here are realtime statistics for the processing rate, `busyness`_, data operations and errors.

Clicking the "Outputs" button on the right takes you to the third pane of the dialog.

.. image:: _images/console/console_29_dag3.png
   :width: 400px

On the right are all the output connections of the Flowlet, if any, and clicking any of
the names would take you to that Flowlet’s input pane, allowing you to traverse the graph
in the direction of data flow. The realtime statistics for the outbound events are shown.

In the upper right portion of this dialog you can set the requested number of instances.
The current number of instances is shown for reference.


.. _log-explorer:

Flow Log Explorer
.................

The Flow Log Explorer pane shows a sample from the logs, with filters for a standard set of filters: *Info*, *Warning*, *Error*, *Debug*, and *Other:*

.. image:: _images/console/console_08_app_crawler_flow_rss_log.png
   :width: 600px

Flow History
................

The Flow History pane shows started and ended events for the Flow and the results:

.. image:: _images/console/console_09b_app_crawler_flow_rss_history.png
   :width: 600px


MapReduce
---------
For a MapReduce, the Mapping and Reducing activity is shown, along with status and management controls for starting,
stopping and configuration. Buttons for logs and history, similar to those for
`Flows <#flow-history>`__ and `Workflows <#workflow>`__, are also available:


.. image:: _images/console/console_26_mapreduce.png
   :width: 600px


Spark
-----
For a Spark program, the status is shown along with the management controls for starting,
stopping and configuration. Buttons for logs and history, similar to those for
`Flows <#flow-history>`__ and `Workflows <#workflow>`__, are also available:

.. image:: _images/console/console_26a_spark.png
   :width: 600px


Workflow
--------
For a Workflow, the time until the next scheduled run is shown, along with status and management controls for starting, stopping and configuration.

.. image:: _images/console/console_25_workflow.png
   :width: 600px


Workflow History
................
The Workflow History pane shows started and ended events for the Workflow and the results:

.. image:: _images/console/console_09_app_crawler_flow_rss_history.png
   :width: 600px

Dataset
-------
For a Dataset, write rate (in both bytes and operations per second), read rate and total storage is shown
along with a list of Flows attached to the Dataset, their processing rate, and `busyness`_.

.. image:: _images/console/console_15_dataset.png
   :width: 600px

Procedure
---------
For a Procedure, request statistics are shown, along with status and management controls for starting, stopping and configuration. The dialog box shown allows for the sending of requests to Procedures, where
JSON string parameters are passed to the Procedure when calling its methods.

For details of making requests and using Procedures, including configuring the parameters and calling
methods, see the `Cask Data Application Platform HTTP RESTful API <rest.html>`__.

In a fashion similar to the `Flow Log Explorer`_, you can examine the logs associated with each Procedure.


.. image:: _images/console/console_17_procedure_ranker.png
   :width: 600px

Custom Service
--------------
Each Application can access and use user-defined Custom Services. From an individual Application's panel
you access its Custom Services panel.

For a Custom Service, components of the Service are shown, along with status and management controls for starting, stopping and configuration. The current number of instances requested and active are shown for
each component.

For details of making and using Custom Services, see the Developer Guide `Advanced CDAP Features <advanced.html#custom-services>`__.

.. image:: _images/console/console_32_custom_service.png
   :width: 600px

Logging
=======

CDAP supports logging through standard
`SLF4J (Simple Logging Facade for Java) <http://www.slf4j.org/manual.html>`__ APIs.
For instance, in a Flowlet you can write::

  private static Logger LOG = LoggerFactory.getLogger(WordCounter.class);
  ...
  @ProcessInput
  public void process(String line) {
    LOG.info("{}: Received line {}", this.getContext().getTransactionAwareName(), line);
    ... // processing
    LOG.info("{}: Emitting count {}", this.getContext().getTransactionAwareName(), wordCount);
    output.emit(wordCount);
  }

The log messages emitted by your Application code can be viewed in two different ways.

- Using the `Cask Data Application Platform HTTP RESTful API <rest.html>`__.
  The `RESTful interface <rest.html#logging-http-api>`__ details all the available contexts that
  can be called to retrieve different messages.
- All log messages of an Application can be viewed in the CDAP Console
  by clicking the *Logs* button in the Flow or Procedure screens.
  This launches the `Log Explorer <#log-explorer>`__.

See the `Flow Log Explorer <#log-explorer>`__ in the `CDAP Console <#console>`__
for details of using it to examine logs in the CDAP.
In a similar fashion, `Procedure Logs <#procedure>`__ can be examined from within the CDAP Console.

Metrics
=======

As applications process data, the CDAP collects metrics about the application’s behavior and performance. Some of these metrics are the same for every application—how many events are processed, how many data operations are performed—and are thus called system or CDAP metrics.

Other metrics are user-defined or "custom" and differ from application to application.
To add user-defined metrics to your application, read this section in conjunction with the
details on available system metrics in the
`Cask Data Application Platform HTTP RESTful API <rest.html#metrics-http-api>`__.

You embed user-defined metrics in the methods defining the elements of your application.
They will then emit their metrics and you can retrieve them
(along with system metrics) via the `Metrics Explorer`_ in the CDAP Console or
via the CDAP’s `RESTful interfaces <rest.html>`__.
The names given to the metrics (such as ``names.longnames`` and ``names.bytes`` as in the example below)
should be composed only of alphanumeric characters.

To add metrics to a Flowlet *NameSaver*::

  public static class NameSaver extends AbstractFlowlet {
    static final byte[] NAME = { 'n', 'a', 'm', 'e' };

    @UseDataSet("whom")
    KeyValueTable whom;
    Metrics flowletMetrics; // Declare the custom metrics

    @ProcessInput
    public void processInput(StreamEvent event) {
      byte[] name = Bytes.toBytes(event.getBody());
      if (name != null && name.length > 0) {
        whom.write(NAME, name);
      }
      if (name.length > 10) {
        flowletMetrics.count("names.longnames", 1);
      }
      flowletMetrics.count("names.bytes", name.length);
    }
  }

An example of user-defined metrics is in ``PurchaseStore`` in the `Purchase example <examples/Purchase/index.html>`_.

For details on available system metrics, see the `Metrics section <rest.html#metrics-http-api>`__
in the `CDAP HTTP REST API Guide <rest.html>`__.

Using Metrics Explorer
----------------------
See the `Metrics Explorer`_ in the `CDAP Console <#console>`__
for details of using it to examine and set metrics in the CDAP.

Runtime Arguments
=================

Flows, Procedures, MapReduce and Workflows can receive runtime arguments:

- For Flows and Procedures, runtime arguments are available to the ``initialize`` method in the context.

- For MapReduce, runtime arguments are available to the ``beforeSubmit`` and ``onFinish`` methods in the context.
  The ``beforeSubmit`` method can pass them to the Mappers and Reducers through the job configuration.

- When a Workflow receives runtime arguments, it passes them to each MapReduce in the Workflow.

The ``initialize()`` method in this example accepts a runtime argument for the
``HelloWorld`` Procedure. For example, we can change the greeting from
the default “Hello” to a customized “Good Morning” by passing a runtime argument::

  public static class Greeting extends AbstractProcedure {

    @UseDataSet("whom")
    KeyValueTable whom;
    private String greeting;

    public void initialize(ProcedureContext context) {
      Map<String, String> args = context.getRuntimeArguments();
      greeting = args.get("greeting");
      if (greeting == null) {
        greeting = "Hello";
      }
    }

    @Handle("greet")
    public void greet(ProcedureRequest request,
                      ProcedureResponder responder) throws Exception {
      byte[] name = whom.read(NameSaver.NAME);
      String toGreet = name != null ? new String(name) : "World";
      responder.sendJson(greeting + " " + toGreet + "!");
    }
  }

Scaling Instances
=================

.. highlight:: console

Scaling Flowlets
----------------
You can query and set the number of instances executing a given Flowlet
by using the ``instances`` parameter with HTTP GET and PUT methods::

  GET /v2/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances
  PUT /v2/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

Where:
  :<app-id>: Name of the application
  :<flow-id>: Name of the Flow
  :<flowlet-id>: Name of the Flowlet
  :<quantity>: Number of instances to be used

Example: Find out the number of instances of the Flowlet *saver* in
the Flow *WhoFlow* of the application *HelloWorld*::

  GET /v2/apps/HelloWorld/flows/WhoFlow/flowlets/saver/instances

Example: Change the number of instances of the Flowlet *saver*
in the Flow *WhoFlow* of the application *HelloWorld*::

  PUT /v2/apps/HelloWorld/flows/WhoFlow/flowlets/saver/instances

with the arguments as a JSON string in the body::

  { "instances" : 2 }


Scaling Procedures
------------------
In a similar way to `Scaling Flowlets`_, you can query or change the number of instances of a Procedure
by using the ``instances`` parameter with HTTP GET and PUT methods::

  GET /v2/apps/<app-id>/procedures/<procedure-id>/instances
  PUT /v2/apps/<app-id>/procedures/<procedure-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

Where:
  :<app-id>: Name of the application
  :<procedure-id>: Name of the Procedure
  :<quantity>: Number of instances to be used

Example: Find out the number of instances of the Procedure *saver*
in the Flow *WhoFlow* of the application *HelloWorld*::

  GET /v2/apps/HelloWorld/flows/WhoFlow/procedure/saver/instances

Example: Change the number of instances of the Procedure *saver*
in the Flow *WhoFlow* of the application *HelloWorld*::

  PUT /v2/apps/HelloWorld/flows/WhoFlow/procedure/saver/instances

with the arguments as a JSON string in the body::

  { "instances" : 2 }

.. highlight:: java

Command-Line Interface
======================

Most of the administrative operations are also available more conveniently through the Command Line Interface.
See this :ref:`section<CLI>` for details.

Where to Go Next
================
Now that you've seen how to operate a CDAP, take a look at:

- :ref:`Cask Data Application Platform HTTP RESTful API <restful-api>`,
  a guide to programming CDAP's HTTP interface.
- :ref:`Command-Line Interface <CLI>`,
  a guide to the command-line client interface to CDAP.

.. _appendix:

Appendix: ``cdap-site.xml``
===========================
Here are the parameters that can be defined in the ``cdap-site.xml`` file,
their default values, descriptions and notes.

For information on configuring the ``cdap-site.xml`` file and CDAP for security,
see the online document `CDAP Security Guide
<http://docs.cask.co/cdap/current/security.html>`__.

..   :widths: 20 20 30

.. list-table::
   :widths: 30 35 35
   :header-rows: 1

   * - Parameter name
     - Default Value
     - Description
   * - ``app.bind.address``
     - ``127.0.0.1``
     - App-Fabric server host address
   * - ``app.bind.port``
     - ``45000``
     - App-Fabric server port
   * - ``app.command.port``
     - ``45010``
     - App-Fabric command port
   * - ``app.output.dir``
     - ``/programs``
     - Directory where all archives are stored
   * - ``app.program.jvm.opts``
     - ``${weave.jvm.gc.opts}``
     - Java options for all program containers
   * - ``app.temp.dir``
     - ``/tmp``
     - Temp directory
   * - ``dashboard.bind.port``
     - ``9999``
     - CDAP Console bind port
   * - ``dashboard.ssl.bind.port``
     - ``9443``
     - CDAP Console bind port for HTTPS
   * - ``dashboard.ssl.disable.cert.check``
     - ``false``
     - True to disable SSL certificate check from the CDAP Console
   * - ``data.local.storage``
     - ``${local.data.dir}/ldb``
     - Database directory
   * - ``data.local.storage.blocksize``
     - ``1024``
     - Block size in bytes
   * - ``data.local.storage.cachesize``
     - ``104857600``
     - Cache size in bytes
   * - ``data.queue.config.update.interval``
     - ``5``
     - Frequency, in seconds, of updates to the queue consumer
   * - ``data.queue.table.name``
     - ``queues``
     - Tablename for queues
   * - ``data.tx.bind.address``
     - ``127.0.0.1``
     - Transaction Inet address
   * - ``data.tx.bind.port``
     - ``15165``
     - Transaction bind port
   * - ``data.tx.client.count``
     - ``5``
     - Number of pooled transaction instances
   * - ``data.tx.client.provider``
     - ``thread-local``
     - Provider strategy for transaction clients
   * - ``data.tx.command.port``
     - ``15175``
     - Transaction command port number
   * - ``data.tx.janitor.enable``
     - ``true``
     - Whether or not the TransactionDataJanitor coprocessor
   * - ``data.tx.server.io.threads``
     - ``2``
     - Number of transaction IO threads
   * - ``data.tx.server.threads``
     - ``25``
     - Number of transaction threads
   * - ``data.tx.snapshot.dir``
     - ``${hdfs.namespace}/tx.snapshot``
     - Directory in HDFS used to store snapshots and transaction logs
   * - ``data.tx.snapshot.interval``
     - ``300``
     - Frequency of transaction snapshots in seconds
   * - ``data.tx.snapshot.local.dir``
     - ``${local.data.dir}/tx.snapshot``
     - Snapshot storage directory on the local filesystem
   * - ``data.tx.snapshot.retain``
     - ``10``
     - Number of retained transaction snapshot files
   * - ``enable.unrecoverable.reset``
     - ``false``
     - **WARNING: Enabling this option makes it possible to delete all
       applications and data; no recovery is possible!**
   * - ``explore.active.operation.timeout.secs``
     - ``86400``
     - Timeout value in seconds for a SQL operation whose result is not fetched completely
   * - ``explore.cleanup.job.schedule.secs``
     - ``60``
     - Time in secs to schedule clean up job to timeout operations
   * - ``explore.executor.container.instances``
     - ``1``
     - Number of explore executor instances
   * - ``explore.executor.max.instances``
     - ``1``
     - Maximum number of explore executor instances
   * - ``explore.inactive.operation.timeout.secs``
     - ``3600``
     - Timeout value in seconds for a SQL operation which has no more results to be fetched
   * - ``gateway.boss.threads``
     - ``1``
     - Number of Netty server boss threads
   * - ``gateway.connection.backlog``
     - ``20000``
     - Maximum connection backlog of Gateway
   * - ``gateway.exec.threads``
     - ``20``
     - Number of Netty server executor threads
   * - ``gateway.max.cached.events.per.stream.num``
     - ``5000``
     - Maximum number of a single stream's events cached before flushing
   * - ``gateway.max.cached.stream.events.bytes``
     - ``52428800``
     - Maximum size (in bytes) of stream events cached before flushing
   * - ``gateway.max.cached.stream.events.num``
     - ``10000``
     - Maximum number of stream events cached before flushing
   * - ``gateway.memory.mb``
     - ``2048``
     - Memory in MB for Gateway process in YARN
   * - ``gateway.num.cores``
     - ``2``
     - Cores requested per Gateway container in YARN
   * - ``gateway.num.instances``
     - ``1``
     - Number of Gateway instances in YARN
   * - ``gateway.stream.callback.exec.num.threads``
     - ``5``
     - Number of threads in stream events callback executor
   * - ``gateway.stream.events.flush.interval.ms``
     - ``150``
     - Interval at which cached stream events get flushed
   * - ``gateway.worker.threads``
     - ``10``
     - Number of Netty server worker threads
   * - ``hdfs.lib.dir``
     - ``${hdfs.namespace}/lib``
     - Common directory in HDFS for JAR files for coprocessors
   * - ``hdfs.namespace``
     - ``/${cdap.namespace}``
     - Namespace for files written by CDAP
   * - ``hdfs.user``
     - ``yarn``
     - User name for accessing HDFS
   * - ``hive.local.data.dir``
     - ``${local.data.dir}/hive``
     - Location of hive relative to ``local.data.dir``
   * - ``hive.server.bind.address``
     - ``localhost``
     - Router address hive server binds to
   * - ``kafka.bind.address``
     - ``0.0.0.0``
     - Kafka server hostname
   * - ``kafka.bind.port``
     - ``9092``
     - Kafka server port
   * - ``kafka.default.replication.factor``
     - ``1``
     - Kafka replication factor [`Note 1`_]
   * - ``kafka.log.dir``
     - ``/tmp/kafka-logs``
     - Kafka log storage directory
   * - ``kafka.num.partitions``
     - ``10``
     - Default number of partitions for a topic
   * - ``kafka.seed.brokers``
     - ``127.0.0.1:9092``
     - Kafka brokers list (comma separated)
   * - ``kafka.zookeeper.namespace``
     - ``kafka``
     - Kafka Zookeeper namespace
   * - ``local.data.dir``
     - ``data``
     - Data directory for local mode
   * - ``log.base.dir``
     - ``/logs/avro``
     - Base log directory
   * - ``log.cleanup.run.interval.mins``
     - ``1440``
     - Log cleanup interval in minutes
   * - ``log.publish.num.partitions``
     - ``10``
     - Number of Kafka partitions to publish the logs to
   * - ``log.retention.duration.days``
     - ``7``
     - Log file HDFS retention duration in days
   * - ``log.run.account``
     - ``cdap``
     - Logging service account
   * - ``log.saver.event.bucket.interval.ms``
     - ``4000``
     - Log events published in this interval (in milliseconds) will be processed in a batch.
       Smaller values will increase the odds of log events going out-of-order.
   * - ``log.saver.event.processing.delay.ms``
     - ``8000``
     - Buffer log events in memory for given time, in milliseconds. Log events received after
       this delay will show up out-of-order. This needs to be greater than
       ``log.saver.event.bucket.interval.ms`` by at least a few hundred milliseconds.
   * - ``log.saver.num.instances``
     - ``1``
     - Log Saver instances to run in YARN
   * - ``log.saver.run.memory.megs``
     - ``1024``
     - Memory in MB allocated to the Log Saver process
   * - ``metadata.bind.address``
     - ``127.0.0.1``
     - Metadata server address
   * - ``metadata.bind.port``
     - ``45004``
     - Metadata server port
   * - ``metadata.program.run.history.keepdays``
     - ``30``
     - Number of days to keep metadata run history
   * - ``metrics.data.table.retention.resolution.1.seconds``
     - ``7200``
     - Retention resolution of the 1 second table in seconds
   * - ``metrics.kafka.partition.size``
     - ``10``
     - Number of partitions for metrics topic
   * - ``metrics.query.bind.address``
     - ``127.0.0.1``
     - Metrics query server host address
   * - ``metrics.query.bind.port``
     - ``45005``
     - Metrics query server port
   * - ``cdap.explore.enabled``
     - ``false``
     - Determines if the CDAP Explore Service is enabled
   * - ``cdap.namespace``
     - ``cdap``
     - Namespace for this CDAP instance
   * - ``router.bind.address``
     - ``0.0.0.0``
     - Router server address
   * - ``router.bind.port``
     - ``10000``
     - Port number that the CDAP router should bind to for HTTP Connections
   * - ``router.client.boss.threads``
     - ``1``
     - Number of router client boss threads
   * - ``router.client.worker.threads``
     - ``10``
     - Number of router client worker threads
   * - ``router.connection.backlog``
     - ``20000``
     - Maximum router connection backlog
   * - ``router.server.address``
     - ``localhost``
     - Router address to which Console connects
   * - ``router.server.boss.threads``
     - ``1``
     - Number of router server boss threads
   * - ``router.server.port``
     - ``10000``
     - Router port to which Console connects
   * - ``router.server.worker.threads``
     - ``10``
     - Number of router server worker threads
   * - ``router.ssl.bind.port``
     - ``10443``
     - Port number that the CDAP router should bind to for HTTPS Connections
   * - ``scheduler.max.thread.pool.size``
     - ``30``
     - Size of the scheduler thread pool
   * - ``security.auth.server.address``
     - ``127.0.0.1``
     - IP address that the CDAP Authentication Server should bind to
   * - ``security.auth.server.bind.port``
     - ``10009``
     - Port number that the CDAP Authentication Server should bind to for HTTP
   * - ``security.auth.server.ssl.bind.port``
     - ``10010``
     - Port to bind to for HTTPS on the CDAP Authentication Server
   * - ``security.authentication.basic.realmfile``
     -
     - Username / password file to use when basic authentication is configured
   * - ``security.authentication.handlerClassName``
     -
     - Name of the authentication implementation to use to validate user credentials
   * - ``security.authentication.loginmodule.className``
     -
     - JAAS LoginModule implementation to use when
       ``co.cask.security.server.JAASAuthenticationHandler`` is configured for
       ``security.authentication.handlerClassName``
   * - ``security.data.keyfile.path``
     - ``${local.data.dir}/security/keyfile``
     - Path to the secret key file (only used in single-node operation)
   * - ``security.enabled``
     - ``false``
     - Enables authentication for CDAP.  When set to ``true`` all requests to CDAP must
       provide a valid access token.
   * - ``security.realm``
     - ``cask``
     - Authentication realm used for scoping security.  This value should be unique for each
       installation of CDAP.
   * - ``security.server.extended.token.expiration.ms``
     - ``604800000``
     - Admin tool access token expiration time in milliseconds (defaults to 1 week) (internal)
   * - ``security.server.maxthreads``
     - ``100``
     - Maximum number of threads that the CDAP Authentication Server should use for
       handling HTTP requests
   * - ``security.server.token.expiration.ms``
     - ``86400000``
     - Access token expiration time in milliseconds (defaults to 24 hours)
   * - ``security.token.digest.algorithm``
     - ``HmacSHA256``
     -  Algorithm used for generating MAC of access tokens
   * - ``security.token.digest.key.expiration.ms``
     - ``3600000``
     - Time duration (in milliseconds) after which an active secret key
       used for signing tokens should be retired
   * - ``security.token.digest.keylength``
     - ``128``
     - Key length used in generating the secret keys for generating MAC of access tokens
   * - ``security.token.distributed.parent.znode``
     - ``/${cdap.namespace}/security/auth``
     - Parent node in ZooKeeper used for secret key distribution in distributed mode
   * - ``ssl.enabled``
     - ``false``
     - True to enable SSL
   * - ``stream.flume.port``
     - ``10004``
     -
   * - ``stream.flume.threads``
     - ``20``
     -
   * - ``thrift.max.read.buffer``
     - ``16777216``
     - Maximum read buffer size in bytes used by the Thrift server [`Note 2`_]
   * - ``weave.java.reserved.memory.mb``
     - ``250``
     - Reserved non-heap memory in MB for Weave container
   * - ``weave.jvm.gc.opts``
     - ``-verbose:gc``

       ``-Xloggc:<log-dir>/gc.log``

       ``-XX:+PrintGCDetails``

       ``-XX:+PrintGCTimeStamps``

       ``-XX:+UseGCLogFileRotation``

       ``-XX:NumberOfGCLogFiles=10``

       ``-XX:GCLogFileSize=1M``

     - Java garbage collection options for all Weave containers; ``<log-dir>`` is the location
       of the log directory on each machine
   * - ``weave.no.container.timeout``
     - ``120000``
     - Amount of time in milliseconds to wait for at least one container for Weave runnable
   * - ``weave.zookeeper.namespace``
     - ``/weave``
     - Weave Zookeeper namespace prefix
   * - ``yarn.user``
     - ``yarn``
     - User name for running applications in YARN
   * - ``zookeeper.quorum``
     - ``127.0.0.1:2181/${cdap.namespace}``
     - Zookeeper address host:port
   * - ``zookeeper.session.timeout.millis``
     - ``40000``
     - Zookeeper session time out in milliseconds

.. rst2pdf: PageBreak

.. _note 1:

:Note 1:

    ``kafka.default.replication.factor`` is used to replicate *Kafka* messages across multiple
    machines to prevent data loss in the event of a hardware failure. The recommended setting
    is to run at least two *Kafka* servers. If you are running two *Kafka* servers, set this
    value to 2; otherwise, set it to the number of *Kafka* servers

.. _note 2:

:Note 2:
    Maximum read buffer size in bytes used by the Thrift server: this value should be set to
    greater than the maximum frame sent on the RPC channel.

Appendix: ``cdap-security.xml``
===============================
Here are the parameters that can be defined in the ``cdap-security.xml`` file,
their default values, descriptions and notes.

For information on configuring the ``cdap-security.xml`` file and CDAP for security,
see the online document `CDAP Security Guide
<http://docs.cask.co/cdap/current/security.html>`__.

..   :widths: 20 20 30

.. list-table::
   :widths: 30 35 35
   :header-rows: 1

   * - Parameter name
     - Default Value
     - Description
   * - ``dashboard.ssl.cert``
     -
     - SSL certificate file to be used for the CDAP Console
   * - ``dashboard.ssl.key``
     -
     - SSL key file corresponding to the SSL certificate specified in ``dashboard.ssl.cert``
   * - ``router.ssl.keystore.keypassword``
     -
     - Key password to the Java keystore file specified in ``router.ssl.keystore.path``
   * - ``router.ssl.keystore.password``
     -
     - Password to the Java keystore file specified in ``router.ssl.keystore.path``
   * - ``router.ssl.keystore.path``
     -
     - Path to the Java keystore file containing the certificate used for HTTPS on the CDAP Router
   * - ``router.ssl.keystore.type``
     - ``JKS``
     - Type of the Java keystore file specified in ``router.ssl.keystore.path``
   * - ``security.auth.server.ssl.keystore.keypassword``
     -
     - Key password to the Java keystore file specified in ``security.auth.server.ssl.keystore.path``
   * - ``security.auth.server.ssl.keystore.password``
     -
     - Password to the Java keystore file specified in ``security.auth.server.ssl.keystore.path``
   * - ``security.auth.server.ssl.keystore.path``
     -
     - Path to the Java keystore file containing the certificate used for HTTPS on the CDAP
       Authentication Server
   * - ``security.auth.server.ssl.keystore.type``
     - ``JKS``
     - Type of the Java keystore file specified in ``security.auth.server.ssl.keystore.path``

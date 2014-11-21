.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Installation and Configuration
============================================

.. _install:

Introduction
------------

This manual is to help you install and configure Cask Data Application Platform (CDAP). It provides the
`system, <#system-requirements>`__
`network, <#network-requirements>`__ and
`software requirements, <#software-prerequisites>`__
`packaging options, <#packaging>`__ and
instructions for
`installation <#installation>`__ and
`verification <#verification>`__ of
the CDAP components so they work with your existing Hadoop cluster.

These are the CDAP components:

- **CDAP Web-App:** User interface—the *Console*—for managing CDAP applications;
- **CDAP Gateway:** Service supporting REST endpoints for CDAP;
- **CDAP-Master:** Service for managing runtime, lifecycle and resources of CDAP applications;
- **CDAP Kafka:** Metrics and logging transport service, using an embedded version of *Kafka*; and
- **CDAP Authentication Server:** Performs client authentication for CDAP when security is enabled.

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

Software Prerequisites
......................
You'll need this software installed:

- Java runtime (on CDAP and Hadoop nodes)
- Node.js runtime (on CDAP nodes)
- Hadoop, HBase (and possibly Hive) environment to run against

.. _install-java-runtime:

Java Runtime
++++++++++++
The latest `JDK or JRE version 1.6.xx or 1.7.xx <http://www.java.com/en/download/manual.jsp>`__
for Linux and Solaris must be installed in your environment.

To check the Java version installed, run the command::

  java -version
  
CDAP is tested with the Oracle JDKs; it may work with other JDKs such as 
`Open JDK <http://openjdk.java.net>`__, but it has not been tested with them.

Once you have installed the JDK, you'll need to set the JAVA_HOME environment variable.


Node.js Runtime
+++++++++++++++
You can download the latest version of Node.js from `nodejs.org <http://nodejs.org>`__:

1. Download the appropriate Linux or Solaris binary ``.tar.gz`` from
   `nodejs.org/download/ <http://nodejs.org/download/>`__.
 #. Extract somewhere such as ``/opt/node-[version]/``
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


Deployment Architectures
........................

.. image:: ../_images/cdap-minimal-deployment.png
   :width: 8in
   :align: center

------------

.. image:: ../_images/cdap-ha-hs-deployment.png
   :width: 8in
   :align: center

Prepare the Cluster
...................
To prepare your cluster so that CDAP can write to its default namespace,
create a top-level ``/cdap`` directory in HDFS, owned by an HDFS user ``yarn``::

  hadoop fs -mkdir /cdap && hadoop fs -chown yarn /cdap

In the CDAP packages, the default HDFS namespace is ``/cdap``
and the default HDFS user is ``yarn``. If you set up your cluster as above, no further changes are
required.

To make alterations to your setup, create an `.xml` file ``conf/cdap-site.xml``
(see the :ref:`appendix-cdap-site.xml`) and set appropriate properties.

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

- Set the ``router.server.address`` property in ``conf/cdap-site.xml`` to the hostname of the CDAP Router.
  The CDAP Console uses this property to connect to the Router::

      <property>
        <name>router.server.address</name>
        <value>{router-host-name}</value>
      </property>

- To use the ad-hoc querying capabilities of CDAP, enable the CDAP Explore Service in
  ``conf/cdap-site.xml`` (by default, it is disabled)::

    <property>
      <name>cdap.explore.enabled</name>
      <value>true</value>
      <description>Enable Explore functionality</description>
    </property>

  **Note:** This feature cannot be used unless the cluster has a correct version of Hive installed.
  See *Hadoop/HBase Environment* above. This feature is currently not supported on secure Hadoop clusters.


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

   CDAP_KEYTAB="/etc/security/keytabs/cdap.keytab"
   CDAP_PRINCIPAL="<cdap principal>@EXAMPLE.REALM.COM"

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


RPM using Yum
.............
Download the Cask Yum repo definition file::

  sudo curl -o /etc/yum.repos.d/cask.repo http://repository.cask.co/downloads/centos/6/x86_64/cask.repo

This will create the file ``/etc/yum.repos.d/cask.repo`` with::

  [cask]
  name=Cask Packages
  baseurl=http://repository.cask.co/centos/6/x86_64/releases
  enabled=1
  gpgcheck=1


Add the Cask Public GPG Key to your repository::

  sudo rpm --import http://repository.cask.co/centos/6/x86_64/releases/pubkey.gpg

Debian using APT
................
Download the Cask Apt repo definition file::

  sudo curl -o /etc/apt/sources.list.d/cask.list http://repository.cask.co/downloads/ubuntu/precise/amd64/cask.list

This will create the file ``/etc/apt/sources.list.d/cask.list`` with::

  deb [ arch=amd64 ] http://repository.cask.co/ubuntu/precise/amd64/releases precise releases


Add the Cask Public GPG Key to your repository::

  curl -s http://repository.cask.co/ubuntu/precise/amd64/releases/pubkey.gpg | sudo apt-key add -

.. _installation:

Installation
------------
Install the CDAP packages by using either of these methods:

Using Yum::

  sudo yum install cdap-gateway cdap-kafka cdap-master cdap-security cdap-web-app

Using APT::

  sudo apt-get update
  sudo apt-get install cdap-gateway cdap-kafka cdap-master cdap-security cdap-web-app

Do this on each of the boxes that are being used for the CDAP components; our
recommended installation is a minimum of two boxes.

This will download and install the latest version of CDAP with all of its dependencies. 

For instructions on enabling CDAP Security, see :doc:`CDAP Security; <security>`
and in particular, see the instructions for :ref:`configuring the properties of cdap-site.xml. <enabling-security>`

When all the packages and dependencies have been installed,
you can start the services on each of the CDAP boxes by running this command::

  for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i restart ; done

When all the services have completed starting, the CDAP Console should then be
accessible through a browser at port 9999. The URL will be ``http://<console-ip>:9999`` where
``<console-ip>`` is the IP address of one of the machine where you installed the packages
and started the services.

Upgrading from a Previous Version
---------------------------------
When upgrading an existing CDAP installation from a previous version, you will need
to make sure the CDAP table definitions in HBase are up-to-date.

These steps will stop CDAP, update the installation, run an upgrade tool for the table definitions,
and then restart CDAP.

1. Stop all CDAP processes::

     for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i stop ; done

#. Update the CDAP packages by running either of these methods:

   - Using Yum (on one line)::

       sudo yum install cdap cdap-gateway
                              cdap-hbase-compat-0.94 cdap-hbase-compat-0.96
                              cdap-hbase-compat-0.98 cdap-kafka cdap-master
                              cdap-security cdap-web-app

   - Using APT (on one line)::

       sudo apt-get install cdap cdap-gateway
                              cdap-hbase-compat-0.94 cdap-hbase-compat-0.96
                              cdap-hbase-compat-0.98 cdap-kafka cdap-master
                              cdap-security cdap-web-app

#. Run the upgrade tool (on one line)::

     /opt/cdap/cdap-master/bin/svc-master run
       com.cdap.data.tools.Main upgrade

#. Restart the CDAP processes::

     for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i start ; done

Verification
------------
To verify that the CDAP software is successfully installed and you are able to use your
Hadoop cluster, run an example application.
We provide in our SDK pre-built ``.JAR`` files for convenience:

#. Download and install the latest CDAP Software Development Kit (SDK) from
   http://cask.co/downloads/#cdap\ .
#. Extract to a folder (``CDAP_HOME``).
#. Open a command prompt and navigate to ``CDAP_HOME/examples``.
#. Each example folder has a ``.jar`` file in its ``target`` directory.
   For verification, we will use the ``WordCount`` example.
#. Open a web browser to the CDAP Console.
   It is located on port ``9999`` of the box where you installed CDAP.
#. On the Console, click the button *Load an App*.
#. Find the pre-built ``WordCount-<cdap-version>.jar`` using the dialog box to navigate to
   ``CDAP_HOME/examples/WordCount/target/``, substituting your version for *<cdap-version>*. 
#. Once the application is deployed, instructions on running the example can be found at the
   :ref:`WordCount example. <examples-word-count>`
#. You should be able to start the application, inject sentences,
   run the Flow and the Procedure, and see results.
#. When finished, you can stop and remove the application as described in the section on
   :ref:`cdap-building-running`.

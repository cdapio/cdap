.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _ambari-adding-cdap-service:

=================================
Adding the CDAP Service to Ambari
=================================

To install CDAP on a cluster managed by Ambari, we have provided packages for
RHEL-compatible and Ubuntu systems, which are installed onto the Ambari management server.
These packages add CDAP to the list of available services which Ambari can install. 

.. highlight:: console

Preparing Ambari
----------------
To install the ``cdap-ambari-service`` package, first add the appropriate CDAP repository
to your system’s package manager by following the steps below. These steps will install a
Cask repository on your Ambari server.

The *repository version* (shown in the commands below as "``cdap/``\ |literal-short-version|" ) must match the
*CDAP version* which you’d like installed on your cluster. To get the *CDAP 3.0 series,*
you would install the *CDAP 3.0 repository.* The default is to use CDAP 3.2, which has the
widest compatibility with Ambari-supported Hadoop distributions.

+-----------------------------------------------------------------------+
| Supported Hortonworks Data Platform (HDP) Distributions               |
+----------------+-----------------+------------------------------------+
| CDAP Version   | CDAP Repository | Hadoop Distributions               |
+================+=================+====================================+
| CDAP 3.0.x     | ``cdap/3.0``    | HDP 2.0, HDP 2.1                   |
+----------------+-----------------+------------------------------------+
| CDAP 3.1.x     | ``cdap/3.1``    | HDP 2.0, HDP 2.1, HDP 2.2          |
+----------------+-----------------+------------------------------------+
| CDAP 3.2.x     | ``cdap/3.2``    | HDP 2.0, HDP 2.1, HDP 2.2, HDP 2.3 |
+----------------+-----------------+------------------------------------+

**Note:** The CDAP Ambari service has been tested on Ambari Server 2.0 and 2.1, as
supplied from Hortonworks.

.. include:: /../target/_includes/ambari-installation.rst
  :start-after: .. _ambari-install-rpm-using-yum:
  :end-before: .. _ambari-install-package-installation:


Installing the CDAP Service
---------------------------
Now, install the ``cdap-ambari-service`` package from the repo you specified above:

Installing the CDAP Service via YUM
...................................
::

  $ sudo yum install -y cdap-ambari-service
  $ sudo ambari-server restart

Installing the CDAP Service via APT
...................................
::

  $ sudo apt-get install -y cdap-ambari-service
  $ sudo ambari-server restart
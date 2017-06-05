.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2017 Cask Data, Inc.

.. _cdap-jdbc:

CDAP JDBC Driver
================

Overview
--------

CDAP provides a JDBC driver to make integrations with external programs and third-party BI
(business intelligence) tools easier.

The JDBC driver is a JAR that is bundled with the CDAP Sandbox. You can find it in the ``libexec``
directory of your Sandbox installation at::

  libexec/co.cask.cdap.cdap-explore-jdbc-<version>.jar

If you don't have a CDAP Sandbox and only want to connect to an existing instance of CDAP,
you can download the CDAP JDBC driver from `this link
<https://repo1.maven.org/maven2/co/cask/cdap/cdap-explore-jdbc/>`__.
Go to the directory matching the version of your running CDAP instance, and download the file
with the matching version number::

  cdap-explore-jdbc-<version>.jar

Using the CDAP JDBC Driver in your Java Code
-----------------------------------------------------------

To use CDAP JDBC driver in your code, place ``cdap-jdbc-driver.jar`` in the classpath of your application.
If you are using Maven, you can simply add a dependency in your file ``pom.xml``::

  <dependencies>
    ...
    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>cdap-explore-jdbc</artifactId>
      <version><!-- Version of CDAP you want the JDBC driver to query --></version>
    </dependency>
    ...
  </dependencies>

Here is a snippet of Java code that uses the CDAP JDBC driver to connect to a running instance of CDAP,
and executes a query over a CDAP dataset ``mydataset``::

  // First, register the driver once in your application
  Class.forName("co.cask.cdap.explore.jdbc.ExploreDriver");

  // If your CDAP instance requires an authentication token for connection, specify it here.
  // Replace <cdap-host> and <authentication_token> as appropriate to your installation.
  String connectionUrl = "jdbc:cdap://<cdap-host>:11015" +
    "?auth.token=<authentication_token>";

  // Connect to the CDAP instance
  Connection connection = DriverManager.getConnection(connectionUrl);

  // Execute a query over CDAP datasets and retrieve the results
  ResultSet resultSet = connection.prepareStatement("select * from dataset_mydataset").executeQuery();
  ...

Here are the parameters that can be used in the connection URL:

- ``namespace``: CDAP namespace to run in
- ``auth.token``: authentication token for the connection
- ``ssl.enabled``: boolean; whether SSL is enabled or not
- ``verify.ssl.cert``: boolean; false to suspend certificate checks and allow self-signed certificates
- ``fetch.size``: int; number of rows to fetch at a time from the database cursor. Defaults to 1000. 0 means no limit.

JDBC drivers are a standard in the Java ecosystem, with many `resources about them available
<http://docs.oracle.com/javase/tutorial/jdbc/>`__.

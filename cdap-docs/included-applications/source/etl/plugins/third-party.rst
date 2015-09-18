.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-third-party-jars:

======================
Using Third-Party JARs 
======================

.. highlight:: json  

**Prebuilt JARs:** In a case where you'd like to use prebuilt third-party JARs (such as a
JDBC driver) as a plugin, you will need to create a JSON file to describe the JAR.

For information on the format of the JSON, please refer to the section on the
:ref:`Plugin Packaging <included-apps-custom-etl-plugins-plugin-packaging>` in
:ref:`Creating a Custom ETL Plugin <included-apps-custom-etl-plugins>`.

Copy the JAR and the JSON file to the :ref:`Plugin directory
<included-apps-custom-etl-plugins-installation-directory>` and then update the system
artifacts by using the Artifacts HTTP RESTful API :ref:`Load System Artifacts
<http-restful-api-artifact-system-load>` endpoint.

A sample JDBC Driver Plugin configuration::

  [
    {
      "type" : "JDBC",
      "name" : "MySQL JDBC",
      "description" : "Plugin for MySQL JDBC driver",
      "className" : "com.mysql.jdbc.Driver",
    },
    {
      "type" : "JDBC",
      "name" : "PostgreSQL JDBC",
      "description" : "Plugin for PostgreSQL JDBC driver",
      "className" : "org.postgresql.Driver",
    }
  ]

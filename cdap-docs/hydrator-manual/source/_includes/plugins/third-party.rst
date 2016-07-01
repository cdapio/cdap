.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-hydrator-third-party-plugins:

==========================
Adding Third-Party Plugins
==========================

.. highlight:: json  

**Prebuilt JARs:** In a case where you'd like to use pre-built third-party JARs (such as a
JDBC driver) as a plugin, you will need to create a JSON file to describe the JAR.

For information on the format of the JSON, please refer to the sections on
:ref:`Third Party Plugins <plugins-third-party>` and :ref:`Plugin Deployment <plugins-deployment>`.

A sample JDBC Driver Plugin configuration:

.. container:: highlight

  .. parsed-literal::
  
    {
      "parents": [ "cdap-etl-batch[|version|,\ |version|]" ],
      "plugins": [
        {
          "name" : "mysql",
          "type" : "jdbc",
          "className" : "com.mysql.jdbc.Driver",
          "description" : "Plugin for MySQL JDBC driver"
        },
        {
          "name" : "postgresql",
          "type" : "jdbc",
          "className" : "org.postgresql.Driver",
          "description" : "Plugin for PostgreSQL JDBC driver"
        }
      ]
    }

.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-market-api:
================
Cask Market APIs
================

The Cask Market APIs are simply a contract about the directory structure of the marketplace.
All APIs are relative to a base path. For example, the base path for the public Cask hosted market
is 'market.cask.co'. The directory structure must be::

  <base>/v1/packages.json
  <base>/v1/packages/<package-name>/<version>/icon.png
  <base>/v1/packages/<package-name>/<version>/spec.json
  <base>/v1/packages/<package-name>/<version>/spec.json.asc
  <base>/v1/packages/<package-name>/<version>/<resource1>
  <base>/v1/packages/<package-name>/<version>/<resource1>.asc
  <base>/v1/packages/<package-name>/<version>/<resource2>
  <base>/v1/packages/<package-name>/<version>/<resource2>.asc
  ...

.. _cask-market-get-catalog:

Get Market Catalog
==================
To retrieve a list of available packages, submit an HTTP GET request::

  GET /v1/packages.json

This will return a JSON array that lists each package and its metadata::

  [
    {
      name: "access-log",
      version: "1.0.0",
      description: "Sample access logs in Combined Log Format (CLF)",
      label: "Access Log Sample",
      author: "Cask",
      org: "Cask Data, Inc.",
      cdapVersion: "[4.0.0-SNAPSHOT,4.1.0)",
      created: 1473901763,
      categories: [ "datapack" ]
    },
    {
      name: "bulk-data-transfer",
      version: "1.0.0",
      description: "Moving data from structured data source likes traditional relational database into Hadoop is very common in building Data Lakes.
                    This data application allows you to set-up periodic full data dumps from RDBMS into Hadoop cluster.
                    Data on Hadoop is stored as DB table snapshot. Supports other relational databases.",
      label: "Bulk Data Transfer",
      author: "Cask",
      org: "Cask Data, Inc.",
      cdapVersion: "[4.0.0-SNAPSHOT,4.1.0)",
      created: 1473901763,
      categories: [ "usecase" ]
    },
    ...
  ]

Get Package Specification
=========================
To retrieve a package specification, submit an HTTP GET request::

  GET /v1/packages/<package-name>/<version>/spec.json

This will return a JSON object that contains metadata about the package,
and a list of actions required to install the package::

  {
    "label": "Bulk Data Transfer",
    "description": "Moving data from structured data source likes traditional relational database into Hadoop is very common in building Data Lakes.
                    This data application allows you to set-up periodic full data dumps from RDBMS into Hadoop cluster.
                    Data on Hadoop is stored as DB table snapshot. Supports other relational databases.",
    "author": "Cask",
    "org": "Cask Data, Inc.",
    "created": 1473901763,
    "categories": [ "usecase" ],
    "cdapVersion": "[4.0.0-SNAPSHOT,4.1.0)",
    "actions": [
      {
        "type": "informational",
        "label": "Download MySQL JDBC Driver",
        "arguments": [
          {
            "name": "steps",
            "value": [
              "Download the zip file from Mysql at https://dev.mysql.com/downloads/file/?id=462850",
              "Unzip the file",
              "In the next step, upload the 'mysql-connector-java-5.1.39-bin.jar' file from the zip"
            ]
          }
        ]
      },
      {
        "type": "create_artifact",
        "label": "MySQL Driver Plugin",
        "arguments": [
          {
            "name": "name",
            "value": "mysql-connector-java"
          },
          {
            "name": "version",
            "value": "5.1.39"
          },
          {
            "name": "scope",
            "value": "user"
          },
          {
            "name": "config",
            "value": "mysql-connector-java.json"
          }
        ]
      },
      {
        "type": "create_pipeline",
        "label": "Bulk Data Transfer Pipeline",
        "arguments": [
          {
            "name": "artifact",
            "value": {
              "scope": "system",
              "name": "cdap-data-pipeline",
              "version": "4.0.0-SNAPSHOT"
            }
          },
          {
            "name": "name",
            "value": "bulkDataTransfer",
            "canModify": true
          },
          {
            "name": "config",
            "value": "pipeline.json"
          }
        ]
      }
    ]
  }

There are several supported actions, each with its own specification.
If an action fails for any reason, actions completed before it are
not rolled back. However, each action is idempotent, which means the
installation can simply be retried once the underylying failure cause has
been fixed.

Action Specification
--------------------
Each action contains a label, type, and arguments::

  {
    "label": [display label],
    "type": [action type],
    "arguments": [
      "name": [argument name],
      "value": [argument value],
      "canModify": true | false (defaults to false)
    ]
  }

The label is short description that will be displayed to users during the install process.
Some arguments will reference package resources.
Descriptions of each action type and their supported arguments are listed below.

informational
^^^^^^^^^^^^^
Displays information for the user. Does not perform any actions against the CDAP RESTful APIs.
This can be used, for example, to tell the user to download a jar from a 3rd party website.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - steps
     - JSON array of strings listing steps the user should take
     - yes
     -

Example action::

  {
    "type": "informational",
    "label": "Download MySQL JDBC Driver",
    "arguments": [
      {
        "name": "steps",
        "value": [
          "Download the zip file from Oracle at https://dev.mysql.com/downloads/file/?id=462850",
          "Unzip the file",
          "In the next step, upload the 'mysql-connector-java-5.1.39-bin.jar' file from the zip"
        ]
      }
    ]
  }

create_artifact
^^^^^^^^^^^^^^^
Creates a CDAP artifact.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - name
     - artifact name
     - yes
     -
   * - jar
     - package resource containing the artifact jar contents
     - yes
     -
   * - scope
     - artifact scope
     - no
     - user
   * - version
     - artifact version
     - no
     - version contained in the jar manifest
   * - config
     - package resource containing artifact parents, plugins, and properties
     - no
     -

If the artifact is a plugin artifact, the config argument is used to specify its
parent artifacts, any 3rd party plugins contained in the artifact, and any properties
of the artifact.

Example action::

  {
    "type": "create_artifact",
    "label": "MySQL Driver Plugin",
    "arguments": [
      {
        "name": "name",
        "value": "mysql-connector-java"
      },
      {
        "name": "version",
        "value": "5.1.39"
      },
      {
        "name": "scope",
        "value": "user"
      },
      {
        "name": "config",
        "value": "mysql-connector-java.json"
      }
    ]
  }

where mysql-connector-java.json is a package resource with content::

  {
    "parents": [
      "system:cdap-data-pipeline[3.0.0,10.0.0]",
      "system:cdap-data-streams[3.0.0,10.0.0]"
    ],
    "plugins": [
      {
        "name" : "mysql",
        "type" : "jdbc",
        "className" : "com.mysql.jdbc.Driver",
        "description" : "Plugin for MySQL JDBC driver"
      }
    ],
    "properties": { }
  }

create_stream
^^^^^^^^^^^^^
Creates a CDAP stream.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - name
     - stream name
     - yes
     -
   * - description
     - stream description
     - no
     -
   * - properties
     - package resource containing stream properties like the format, ttl, and notification threshold
     - no
     -

Example action::

  {
    "type": "create_stream",
    "label": "Labeled SMS Texts",
    "arguments": [
      {
        "name": "name",
        "value": "labeledSMS"
      },
      {
        "name": "description",
        "value": "SMS texts that have been labeled as spam or not"
      },
      {
        "name": "properties",
        "value": "properties.json"
      }
    ]
  }

where properties.json is a package resource with content::

  {
    "ttl": 9223372036854775,
    "format": {
      "name": "tsv",
      "schema": {
        "type": "record",
        "name": "labeledSMS",
        "fields": [
          { "name": "label", "type": "string" },
          { "name": "message", "type": "string" }
        ]
      }
    },
    "notification.threshold.mb": 1024
  }

load_datapack
^^^^^^^^^^^^^
Loads a datapack into a CDAP entity, like a stream or dataset.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - name
     - the name of the CDAP entity to load the data into
     - yes
     -
   * - files
     - a JSON array of package resources to load into the CDAP entity
     - yes
     -

Example action::

  {
    "type": "load_datapack",
    "label": "Labeled SMS Text Data",
    "arguments": [
      {
        "name": "name",
        "value": "labeledSMS"
      },
      {
        "name": "files",
        "value": [ "texts1.tsv", "texts2.tsv" ]
      }
    ]
  }

where texts1.tsv and texts2.tsv are package resources containing the data to load into the stream.

create_app
^^^^^^^^^^
Creates a CDAP application from an existing CDAP artifact.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - artifact
     - JSON Object containing the application's artifact scope, name, and version
     - yes
     -
   * - name
     - application name
     - yes
     -
   * - config
     - package resource containing the application config
     - no
     -

Example action::

  {
    "type": "create_app",
    "label": "Word Count Example App",
    "arguments": [
      {
        "name": "artifact",
        "value": {
          "scope": "user",
          "name": "WordCount",
          "version": "4.0.0"
        }
      },
      {
        "name": "name",
        "value": "WordCount",
        "canModify": true
      },
      {
        "name": "config",
        "value": "config.json"
      }
    ]
  }

where config.json is a package resource that contains the application configuration::

  {
    "stream": "wordStream",
    "wordStatsTable": "wordStats",
    "wordCountTable": "wordCounts",
    "uniqueCountTable": "uniqueCount",
    "wordAssocTable": "wordAssocs"
  }

create_pipeline
^^^^^^^^^^^^^^^
Creates a Hydrator pipeline. Very similar to the create_app pipeline,
except the config is required and the UI will take the user to the Hydrator UI
instead of the CDAP UI after installation is complete.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - artifact
     - JSON Object containing the pipeline's artifact scope, name, and version
     - yes
     -
   * - name
     - pipeline name
     - yes
     -
   * - config
     - package resource containing the pipeline config
     - yes
     -

Example action::

  {
    "type": "create_pipeline",
    "label": "Omniture Hits Pipeline",
    "arguments": [
      {
        "name": "artifact",
        "value": {
          "scope": "system",
          "name": "cdap-data-pipeline",
          "version": "4.0.0"
        }
      },
      {
        "name": "name",
        "value": "omnitureHitsPipeline",
        "canModify": true
      },
      {
        "name": "config",
        "value": "pipeline.json"
      }
    ]
  }

where pipeline.json is a package resource containing the pipeline config.

create_pipeline_draft
^^^^^^^^^^^^^^^^^^^^^
Creates a Hydrator pipeline draft. Similar to create_pipeline, except the pipeline
will not be published. Instead, a draft will be created that the user can then
go and modify.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - artifact
     - JSON Object containing the pipeline's artifact scope, name, and version
     - yes
     -
   * - name
     - pipeline name
     - yes
     -
   * - config
     - package resource containing the pipeline config
     - yes
     -

Example action::

  {
    "type": "create_pipeline",
    "label": "Omniture Hits Pipeline",
    "arguments": [
      {
        "name": "artifact",
        "value": {
          "scope": "system",
          "name": "cdap-data-pipeline",
          "version": "4.0.0-SNAPSHOT"
        }
      },
      {
        "name": "name",
        "value": "omnitureHitsPipeline",
        "canModify": true
      },
      {
        "name": "config",
        "value": "pipeline.json"
      }
    ]
  }

Get Package Specification Signature
===================================
To retrieve the signature for a package specification, submit an HTTP GET request::

  GET /v1/packages/<package-name>/<version>/spec.json.asc

The signature is a PGP signature that can be used to validate a package resource. The
package publisher signs the package specification with their private key. The signature can
then be used in conjunction with the publisher's public key to validate that the specification
was signed by the publisher.

Get Package Resource
====================
To retrieve a package resource, submit an HTTP GET request::

  GET /v1/packages/<package-name>/<version>/<resource-name>

The resource can contain arbitrary data. They can be artifact jars, configuration files,
sample data, or anything else a package action may require.

Get Package Resource Signature
==============================
To retrieve the signature for a package resource, submit an HTTP GET request::

  GET /v1/packages/<package-name>/<version>/<resource-name>.asc

The signature is a PGP signature that can be used to validate a package resource. The
package publisher signs the package resource with their private key. The signature can
then be used in conjunction with the publisher's public key to validate that a package
resource was signed by the publisher.

Get Package Icon
================
To retrieve the icon for a package, submit an HTTP GET request::

  GET /v1/packages/<package-name>/<version>/icon.png


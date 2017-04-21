.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. :hide-toc: true

.. _cask-market:

===========
Cask Market
===========

Overview
========

The Cask Market allows CDAP users to create and update CDAP artifacts, applications, and
datasets using simple wizards. Instead of building code, deploying artifacts, and
configuring applications, users can simply point and click. This allows users of varying
technical skill the ability to deploy and run common use-cases in a self-service manner.

The Cask Market allows system administrators to distribute re-usable applications, data,
and code to all CDAP users in their organization. Though there is currently no method for
publishing packages to the public Cask-hosted market, administrators can host their own
market and then configure their CDAP instances to use their own market instead of the
public Cask Market.

.. rubric:: Terminology

**Package:** A collection of entities (artifacts, applications, datasets, streams,
configuration) to add to CDAP. A package is identified by a name and version, and can be
tagged with one or more categories. Each package must contain a specification, and may
contain resources.

**Package Specification:** Defines metadata about the package, such as display label,
description, creation time, and CDAP compatibilities. It contains a list of actions that
must be performed to install the package. Each action corresponds to a wizard in the
installation process.

**Package Resource:** A file that can be used during the installation process. It can be a
configuration file, a JAR, or data that should be loaded into a dataset. Resources are
referenced by name in the actions defined in the package specification.

**Catalog:** The catalog is a list of all the packages in the Cask Market. The catalog
contains metadata about each package.

.. rubric:: Architecture

The Cask Market is a service that is separate from a CDAP instance. Each CDAP UI instance
can be configured to read from a separate Cask Market instance. By default, the UI points
to the public Cask-hosted market. During package installation, the UI will make calls to
the Cask Market and to a CDAP Router instance to create or update various CDAP entities.
For example, a package may contain an action to create a stream, and then load data to a
stream. The Cask Market UI will interact with the CDAP RESTful APIs to create the stream,
then fetch the data from the Cask Market and add it to the CDAP stream through the CDAP
RESTful APIs.

A market is essentially just a server that serves static package specifications and
resources. As such, administrators can easily set up their own markets in the same way
they would serve any static content. For example, an Apache web server can be placed on
top of a local directory structure that matches the expected market directory structure.


.. _cask-market-api:

Cask Market HTTP RESTful API
============================

.. highlight:: console

The Cask Market HTTP RESTful APIs are simply a contract about the directory structure of the marketplace.
All APIs are relative to a base path. For example, the base path for the public Cask-hosted market
is ``market.cask.co``.

.. directory-structure-start

The directory structure must be:

.. parsed-literal::

  <base>/|cask-market-version|/packages.json
  <base>/|cask-market-version|/packages/<package-name>/<version>/icon.png
  <base>/|cask-market-version|/packages/<package-name>/<version>/spec.json
  <base>/|cask-market-version|/packages/<package-name>/<version>/spec.json.asc
  <base>/|cask-market-version|/packages/<package-name>/<version>/<resource1>
  <base>/|cask-market-version|/packages/<package-name>/<version>/<resource1>.asc
  <base>/|cask-market-version|/packages/<package-name>/<version>/<resource2>
  <base>/|cask-market-version|/packages/<package-name>/<version>/<resource2>.asc
  ...

.. directory-structure-end

.. Base URL explanation
.. --------------------
.. include:: ../../reference-manual/source/http-restful-api/base-url.txt

.. _cask-market-get-catalog:

Get Market Catalog
------------------

.. highlight:: console

To retrieve a list of available packages, submit an HTTP GET request:

.. parsed-literal::

  GET /|cask-market-version|/packages.json

.. highlight:: json-ellipsis

This will return a JSON array that lists each package and its metadata:

.. container:: highlight

  .. parsed-literal::

    [
      {
        "name": "access-log",
        "version": "1.0.0",
        "description": "Sample access logs in Combined Log Format (CLF)",
        "label": "Access Log Sample",
        "author": "Cask",
        "org": "Cask Data, Inc.",
        "cdapVersion": "|release-range|",
        "created": 1473901763,
        "beta": false,
        "categories": [ "datapack" ]
      },
      {
        "name": "bulk-data-transfer",
        "version": "1.0.0",
        "description": "Moving data from structured data source such as a traditional relational
                      database into Hadoop is very common in building Data Lakes. This
                      data application allows you to set-up periodic full data dumps from
                      RDBMS into Hadoop cluster. Data on Hadoop is stored as DB table
                      snapshot. Supports other relational databases.",
        "label": "Bulk Data Transfer",
        "author": "Cask",
        "org": "Cask Data, Inc.",
        "cdapVersion": "|release-range|",
        "created": 1473901763,
        "beta": false,
        "license": "license.txt",
        "categories": [ "usecase" ]
      },
      ...
    ]


Get Package Specification
-------------------------

.. highlight:: console

To retrieve a package specification, submit an HTTP GET request:

.. parsed-literal::

  GET /|cask-market-version|/packages/<package-name>/<version>/spec.json

.. highlight:: json-ellipsis

This will return a JSON object that contains metadata about the package,
and a list of actions required to install the package:

.. container:: highlight

  .. parsed-literal::

    {
      "label": "Bulk Data Transfer",
      "description": "Moving data from structured data source such as a traditional relational
                    database into Hadoop is very common in building Data Lakes. This data
                    application allows you to set-up periodic full data dumps from RDBMS
                    into Hadoop cluster. Data on Hadoop is stored as DB table snapshot.
                    Supports other relational databases.",
      "author": "Cask",
      "org": "Cask Data, Inc.",
      "created": 1473901763,
      "categories": [ "usecase" ],
      "beta": false,
      "license": "license.txt",
      "cdapVersion": "|release-range|",
      "actions": [
        {
          "type": "informational",
          "label": "Download MySQL JDBC Driver",
          "arguments": [
            {
              "name": "steps",
              "value": [
                "Download the ZIP file from MySQL at https://dev.mysql.com/downloads/file/?id=462850",
                "Unzip the file",
                "In the next step, upload the 'mysql-connector-java-5.1.39-bin.jar' file from the ZIP"
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
                "version": "|release|"
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


Action Specification
--------------------
There are several supported actions, each with its own specification.
If an action fails for any reason, actions completed before it are
not rolled back. However, each action is idempotent, which means the
installation can simply be retried once the underlying cause of the failure has
been fixed.

Each action contains a label, type, and arguments::

  {
    "label": "<display-label>",
    "type": "<action-type>",
    "arguments": [
      {
        "name": "<argument-name>",
        "value": "<argument-value>",
        "canModify": "true | false (defaults to false)"
      },
      ...
    ]
  }

The label is a short description that will be displayed to users during the install process.
Some arguments will reference package resources.

These actions are available:

- `informational`_
- `create_driver_artifact`_
- `create_plugin_artifact`_
- `one_step_deploy_plugin`_
- `create_stream`_
- `load_datapack`_
- `deploy_app`_
- `one_step_deploy_app`_
- `create_pipeline`_
- `create_pipeline_draft`_

Descriptions of each action type and their supported arguments follow.

informational
.............
Displays information for the user. Does not perform any actions against the CDAP RESTful APIs.
This can be used, for example, to tell the user to download a JAR from a 3rd-party website.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - ``steps``
     - JSON array of strings listing steps the user should take
     - Yes
     -

.. highlight:: json-ellipsis

Example action::

  {
    "type": "informational",
    "label": "Download MySQL JDBC Driver",
    "arguments": [
      {
        "name": "steps",
        "value": [
          "Download the ZIP file from Oracle at https://dev.mysql.com/downloads/file/?id=462850",
          "Unzip the file",
          "In the next step, upload the 'mysql-connector-java-5.1.39-bin.jar' file from the ZIP"
        ]
      }
    ]
  }

create_driver_artifact
......................
Creates a CDAP artifact containing a third-party JDBC Driver.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - ``name``
     - Artifact name
     - Yes
     -
   * - ``jar``
     - JAR resource containing the artifact
     - Yes
     -
   * - ``scope``
     - Artifact scope
     - No
     - ``user``
   * - ``version``
     - Artifact version
     - No
     - Version contained in the JAR manifest
   * - ``config``
     - Package resource containing artifact parents, plugins, and properties
     - No
     -

.. highlight:: json-ellipsis

Example action::

  {
    "type": "create_driver_artifact",
    "label": "MySQL Driver Plugin",
    "arguments": [
      {
        "name": "name",
        "value": "mysql-connector-java"
      },
      {
        "name": "scope",
        "value": "user"
      },
      {
        "name": "version",
        "value": "5.1.39"
      },
      {
        "name": "config",
        "value": "mysql-connector-java.json"
      }
    ]
  }

where ``mysql-connector-java.json`` is a package resource with content such as::

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

create_plugin_artifact
......................
Creates a CDAP artifact that contains plugins that extend another artifact. For example,
it may contain plugins for CDAP pipelines. Prompts the user to upload the artifact JAR.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - ``name``
     - Artifact name
     - Yes
     -
   * - ``scope``
     - Artifact scope
     - No
     - ``user``
   * - ``version``
     - Artifact version
     - No
     - Version contained in the JAR manifest
   * - ``config``
     - Package resource containing artifact parents, plugins, and properties
     - No
     -

.. highlight:: json-ellipsis

Example action::

  {
    "type": "create_plugin_artifact",
    "label": "CDAP Pipeline Solr Plugin",
    "arguments": [
      {
        "name": "name",
        "value": "solrsearch-plugins"
      },
      {
        "name": "scope",
        "value": "user"
      },
      {
        "name": "version",
        "value": "1.5.0"
      },
      {
        "name": "config",
        "value": "solrsearch-plugins.json"
      }
    ]
  }

where ``solrsearch-plugins.json`` is a package resource with content such as::

  {
    "parents": [
      "system:cdap-data-pipeline[3.0.0,10.0.0]",
      "system:cdap-data-streams[3.0.0,10.0.0]"
    ],
    "properties": { }
  }

one_step_deploy_plugin
......................
Deploys a CDAP artifact that contains plugins that extend another artifact.
For example, it may contain plugins for CDAP pipelines. It deploys the
artifact without prompting users.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - ``name``
     - Artifact name
     - Yes
     -
   * - ``jar``
     - JAR resource containing the artifact
     - Yes
     -
   * - ``scope``
     - Artifact scope
     - No
     - ``user``
   * - ``version``
     - Artifact version
     - No
     - Version contained in the JAR manifest
   * - ``config``
     - Package resource containing artifact parents, plugins, and properties
     - No
     -

.. highlight:: json-ellipsis

Example action::

  {
    "type": "one_step_deploy_plugin",
    "label": "Deploy CDAP Pipeline Solr Plugin",
    "arguments": [
      {
        "name": "name",
        "value": "solrsearch-plugins"
      },
      {
        "name": "jar",
        "value": "solrsearch-plugins-1.5.0.jar"
      },
      {
        "name": "scope",
        "value": "user"
      },
      {
        "name": "version",
        "value": "1.5.0"
      },
      {
        "name": "config",
        "value": "solrsearch-plugins.json"
      }
    ]
  }

where ``solrsearch-plugins.json`` is a package resource with content such as::

  {
    "parents": [
      "system:cdap-data-pipeline[3.0.0,10.0.0]",
      "system:cdap-data-streams[3.0.0,10.0.0]"
    ],
    "properties": { }
  }

create_stream
.............
Creates a CDAP stream.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - ``name``
     - Stream name
     - Yes
     -
   * - ``description``
     - Stream description
     - No
     -
   * - ``properties``
     - Package resource containing stream properties such as format, TTL, and notification threshold
     - No
     -

.. highlight:: json-ellipsis

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

where ``properties.json`` is a package resource with content such as::

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
.............
Loads a datapack into a CDAP entity, such as a stream or dataset.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - ``name``
     - The name of the CDAP entity to load the data into
     - Yes
     -
   * - ``files``
     - A JSON array of package resources to load into the CDAP entity
     - Yes
     -

.. highlight:: json-ellipsis

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

where ``texts1.tsv`` and ``texts2.tsv`` are package resources containing the data to load into the stream.

deploy_app
..........
Deploys a CDAP application by prompting the user to upload the application JAR.
Does not take any arguments.

.. highlight:: json-ellipsis

Example action:

.. container:: highlight

  .. parsed-literal::

    {
      "type": "deploy_app",
      "label": "Word Count Example App"
    }

one_step_deploy_app
...................
Deploys a CDAP application.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - ``jar``
     - JAR resource containing the application
     - Yes
     -

.. highlight:: json-ellipsis

Example action:

.. container:: highlight

  .. parsed-literal::

    {
      "type": "one_step_deploy_app",
      "label": "Deploy Word Count Example App",
      "arguments": [
        {
          "name": "jar",
          "value": "WordCount-|release|.jar",
        }
      ]
    }


create_pipeline
...............
Creates a CDAP pipeline.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - ``artifact``
     - JSON Object containing the pipeline's artifact scope, name, and version
     - Yes
     -
   * - ``name``
     - Pipeline name
     - Yes
     -
   * - ``config``
     - Package resource containing the pipeline config
     - Yes
     -

.. highlight:: json-ellipsis

Example action:

.. container:: highlight

  .. parsed-literal::

    {
      "type": "create_pipeline",
      "label": "Omniture Hits Pipeline",
      "arguments": [
        {
          "name": "artifact",
          "value": {
            "scope": "system",
            "name": "cdap-data-pipeline",
            "version": "|release|"
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

where ``pipeline.json`` is a package resource containing the pipeline config.

create_pipeline_draft
.....................
Creates a CDAP pipeline draft. Similar to `create_pipeline`_, except that the pipeline
will not be published. Instead, a draft will be created that the user can then modify.

.. list-table::
   :widths: 20 50 10 20
   :header-rows: 1

   * - Argument
     - Description
     - Required?
     - Default
   * - ``artifact``
     - JSON Object containing the pipeline's artifact scope, name, and version
     - Yes
     -
   * - ``name``
     - Pipeline name
     - Yes
     -
   * - ``config``
     - Package resource containing the pipeline config
     - Yes
     -

.. highlight:: json-ellipsis

Example action:

.. container:: highlight

  .. parsed-literal::

    {
      "type": "create_pipeline_draft",
      "label": "Omniture Hits Pipeline",
      "arguments": [
        {
          "name": "artifact",
          "value": {
            "scope": "system",
            "name": "cdap-data-pipeline",
            "version": "|release|"
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

where ``pipeline.json`` is a package resource containing the pipeline config.

Get Package Specification Signature
-----------------------------------

.. highlight:: console

To retrieve the signature for a package specification, submit an HTTP GET request:

.. parsed-literal::

  GET /|cask-market-version|/packages/<package-name>/<version>/spec.json.asc

The signature is a PGP signature that can be used to validate a package resource. The
package publisher signs the package specification with their private key. The signature can
then be used in conjunction with the publisher's public key to validate that the specification
was signed by the publisher.

Get Package Resource
--------------------

.. highlight:: console

To retrieve a package resource, submit an HTTP GET request:

.. parsed-literal::

  GET /|cask-market-version|/packages/<package-name>/<version>/<resource-name>

The resource can contain arbitrary data. They can be artifact JARs, configuration files,
sample data, or anything else that a package action may require.

Get Package Resource Signature
------------------------------

.. highlight:: console

To retrieve the signature for a package resource, submit an HTTP GET request:

.. parsed-literal::

  GET /|cask-market-version|/packages/<package-name>/<version>/<resource-name>.asc

The signature is a PGP signature that can be used to validate a package resource. The
package publisher signs the package resource with their private key. The signature can
then be used in conjunction with the publisher's public key to validate that a package
resource was signed by the publisher.

Get Package Icon
----------------

.. highlight:: console

To retrieve the icon for a package, submit an HTTP GET request:

.. parsed-literal::

  GET /|cask-market-version|/packages/<package-name>/<version>/icon.png


Hosting a Custom Cask Market
============================

.. highlight:: console

The Cask Market APIs are simply a contract about the directory structure of the marketplace.

.. Directory structure
.. --------------------
.. include:: cask-market.rst
    :start-after: .. directory-structure-start
    :end-before: .. directory-structure-end

As such, hosting a custom market can be done by setting up a server that follows the same
path structure.

One possible setup is to keep your packages in a source control repository whose directory
structure matches the one required by the market. The repository can be checked out onto one or
more machines, with an Apache server configured to serve content from that directory. A tool
can be run to create the catalog file from all the package specifications, and to create all
the signature files.

Another possible setup is to serve the market catalog and packages from Amazon S3.

To configure the location of the Cask Market, when you are using a custom market instead
of the public Cask Market, configure the property ``market.base.url`` in the
``cdap-site.xml`` file to point to the location of your custom market. See the
:ref:`Market <appendix-cdap-default-market>` section in the :ref:`Appendix
<appendix-cdap-site.xml>` of the :ref:`Administration Manual <admin-index>`
for details on configuring the file.

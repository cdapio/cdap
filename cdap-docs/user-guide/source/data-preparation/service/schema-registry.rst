.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===============
Schema Registry
===============

Schema Registry provides a serving layer for all types of metadata. It
provides a RESTful interface for storing and retrieving schemas (AVRO,
Protobuf, etc). It stores a versioned history of all schemas,

Schema Information
------------------

+---------------------------------------+------------------------------------+
| Field                                 | Description                        |
+=======================================+====================================+
| ID                                    | Id of the schema as provided by    |
|                                       | the user.                          |
+---------------------------------------+------------------------------------+
| Name                                  | Display name for the schema.       |
+---------------------------------------+------------------------------------+
| Description                           | User facing description about the  |
|                                       | schema                             |
+---------------------------------------+------------------------------------+
| Created Date                          | Time in seconds about when the     |
|                                       | schema was created.                |
+---------------------------------------+------------------------------------+
| Updated Date                          | Time in seconds about when the     |
|                                       | schema was last updated.           |
+---------------------------------------+------------------------------------+
| Version                               | Auto-incremented version of        |
|                                       | schema. This version is            |
|                                       | incremented everytime the schema   |
|                                       | is updated.                        |
+---------------------------------------+------------------------------------+
| Type                                  | Type of the schmea, currently      |
|                                       | supports AVRO and Protobuf-desc    |
+---------------------------------------+------------------------------------+
| Specification                         | Byte array of the specification of |
|                                       | schema                             |
+---------------------------------------+------------------------------------+

RESTful APIs
------------

+---------------+------------------+---------------+---------------+---------------+
| API           | Method           | Path          | Response      | Description   |
+===============+==================+===============+===============+===============+
| Create a      | ``PUT``          | /schemas      | 200 - OK, 500 | Creates an    |
| schema entry  |                  |               | - Error in    | entry in the  |
|               |                  |               | backend store | schema        |
|               |                  |               |               | registry. No  |
|               |                  |               |               | schema is     |
|               |                  |               |               | registred.    |
+---------------+------------------+---------------+---------------+---------------+
| Add a schema  | ``POST``         | /schemas/{id} | 200 - OK, 500 | Adds a        |
| to schema     |                  |               | - Error       | versioned     |
| entry         |                  |               | adding schema | schema to     |
|               |                  |               | to schema     | schema        |
|               |                  |               | registry      | registry.     |
|               |                  |               |               | POST should   |
|               |                  |               |               | use           |
|               |                  |               |               | ``Content-Typ |
|               |                  |               |               | e: applicatio |
|               |                  |               |               | n/octet-strea |
|               |                  |               |               | m``           |
+---------------+------------------+---------------+---------------+---------------+
| Delete all    | ``DELETE``       | /schemas/{id} | 200 - OK, 500 | Deletes the   |
| version of    |                  |               | - Error       | entire schema |
| schema        |                  |               | deleting      | entry         |
|               |                  |               | schema        | including all |
|               |                  |               |               | the versions  |
|               |                  |               |               | of schema.    |
+---------------+------------------+---------------+---------------+---------------+
| Delete a      | ``DELETE``       | /schemas/{id} | 200 - OK, 500 | Deletes a     |
| sepecific     |                  | /versions/{ve | - Error       | specific      |
| version of    |                  | rsion}        | deleting a    | version of    |
| schema        |                  |               | version of    | schema, if    |
|               |                  |               | schema        | schema is not |
|               |                  |               |               | found then a  |
|               |                  |               |               | 404 is        |
|               |                  |               |               | returned.     |
+---------------+------------------+---------------+---------------+---------------+
| GET           | ``GET``          | /schemas/{id} | 200 - OK, 500 | Information   |
| information   |                  | /versions/{ve | - Backend     | about schema  |
| about a a     |                  | rsion}        | error, 404 -  | version and   |
| version of    |                  |               | Schema id not | schema entry  |
| schema        |                  |               | found         |               |
+---------------+------------------+---------------+---------------+---------------+
| GET           | ``GET``          | /schemas/{id} | 200 - OK, 500 | Information   |
| information   |                  |               | - Error       | about schema  |
| about schema  |                  |               |               | entry         |
| entry         |                  |               |               |               |
+---------------+------------------+---------------+---------------+---------------+
| List version  | ``GET``          | /schemas/{id} | 200 - OK, 500 | List the      |
| for schema    |                  | /versions     | - Error       | versions of   |
| available     |                  |               |               | schema.       |
+---------------+------------------+---------------+---------------+---------------+

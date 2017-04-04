.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===========================
Parse AVRO Encoded Messages
===========================

The PARSE-AS-AVRO directive parses messages encoded as binary or json
AVRO records or file. This directive requires the registration of schema
to be decoded with `Schema Registry <../service/schema-registry.md>`__

Syntax
------

::

    parse-as-avro <column> <schema-id> <json|binary> [<version>]

The ``<column>`` is the name of the column whoes values will be decoded
using the schema defined in `Schema
Registry <../service/schema-registry.md>`__ registered with id
``<schema-id>``. Optionally a specific ``<version>`` of registered
schema can be specified.

Usage Notes
-----------

The PARSE-AS-XML directive efficiently parses and represents an XML
document using an in-memory structure that can then be queried using
other directives.

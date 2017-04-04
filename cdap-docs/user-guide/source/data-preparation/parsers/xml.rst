.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-parsers-xml:

==========
XML Parser
==========

#
Parse XML.

PARSE-AS-XML is a directive for parsing an XML document. The directive operates on column to parse that into to XML VTD
object that can then be queried using [XPATH](xpath.md) or [XPATH-ARRAY](xpath.md) or [XPATH-ATTR](xpath-attr.md) or
[XPATH-ARRAY-ATTR](xpath-attr.md)

## Syntax

```
parse-as-xml <column>
```

```column``` name of the column in the record that is a XML document.

## Usage Notes

PARSE-AS-XML directive efficiently parses and represents the XML document into a in-memory structure that can then be
efficiently queried using other directives.

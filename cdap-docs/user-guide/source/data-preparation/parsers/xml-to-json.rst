.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-parsers-xml-to-json:

==================
XML to JSON Parser
==================

Parse XML to JSON.

PARSE-XML-TO-JSON is a directive for parsing an XML document into JSON structure. The
directive operates on input column that is of type String. Application of this directive
transforms the XML into JSON document on fly simplifying further parsing
using :ref:`PARSE-AS-JSON <user-guide-data-preparation-parsers-json>`.

Syntax
======
::

  parse-xml-to-json <column> [depth]


- ``column``: name of the column in the record that is a XML document.
- ``depth``: indicates the depth at which XML document should terminate processing.

Usage Notes
===========
PARSE-XML-TO-JSON directive efficently the XML document and presents as JSON object to
further transform it. The XML document contains elements, attributes, and content text.
Sequence of similar elements are turned into JSONArray |---| which can then be further
parsed using the :ref:`PARSE-AS-JSON <user-guide-data-preparation-parsers-json>`
directive. During parsing, the comments, prologs, DTDs and`<[ [ ]]>`are ignored.

.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=================
Parse XML to JSON
=================

The PARSE-XML-TO-JSON directive parses an XML document into a JSON
structure. The directive operates on an input column of type string.
Application of this directive transforms the XML into a JSON document,
simplifying further parsing using the
`PARSE-AS-JSON <parse-as-json.md>`__ directive.

Syntax
------

::

    parse-xml-to-json <column-name> [<depth>]

-  ``<column-name>`` is the name of the column in the record that is an
   XML document.
-  ``<depth>`` indicates the depth at which the XML document parsing
   should terminate processing.

Usage Notes
-----------

The PARSE-XML-TO-JSON directive efficiently parses an XML document and
presents it as a JSON object for further transformation.

The XML document contains elements, attributes, and content text. A
sequence of similar elements is turned into a JSON array, which can then
be further parsed using the `PARSE-AS-JSON <parse-as-json.md>`__
directive.

During parsing, comments, prologs, DTDs, and ``<[[ ]]>`` notations are
ignored.

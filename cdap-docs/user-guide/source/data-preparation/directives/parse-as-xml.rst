.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

============
Parse as XML
============

The PARSE-AS-XML directive parses an XML document. The directive
operates on a column to parse that into an XML VTD object; that can be
further queried using the `XPATH <xpath.md>`__ directive.

Syntax
------

::

    parse-as-xml <column>

The ``<column>`` is the name of the column in the record that is an XML
document.

Usage Notes
-----------

The PARSE-AS-XML directive efficiently parses and represents an XML
document using an in-memory structure that can then be queried using
other directives.

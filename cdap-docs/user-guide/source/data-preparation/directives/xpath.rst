.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

================
XPath Directives
================

The XPATH and XPATH-ARRAY directives navigate the XML elements and
attributes of an XML document.

Syntax
------

::

    xpath <column> <destination> <xpath>
    xpath-array <column> <destination> <xpath>

The ``<column>`` is the source XML to be navigated with an XPath.

Usage Notes
-----------

The source XML ``<column>`` should first have the
`PARSE-AS-XML <parse-as-xml.md>`__ directive applied before applying
either of the XPATH directives.

Applying the PARSE-AS-XML directive will generate a
`VTDNav <http://vtd-xml.sourceforge.net/javadoc/com/ximpleware/VTDNav.html>`__
object that you can then apply an XPATH directive on. *VTD Nav* is a
memory-efficient random-access XML parser.

The ``<destination>`` specifies the column name to use for storing the
results from the XPath navigation. If the XPath does not yield a value,
then a ``null`` value is written to the column.

The XPATH-ARRAY directive should be used when the XPath could result in
multiple values. The resulting ``<destination>`` value is a JSON array
of all the matches for the XPath.

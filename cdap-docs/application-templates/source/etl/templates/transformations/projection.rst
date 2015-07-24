.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Transformations: Projection
===============================

.. rubric:: Description: Projection transform tha

Projection transform that lets you drop, rename, and cast fields to a different type.

**Drop:** Comma separated list of fields to drop. For example: 'field1,field2,field3'.

**Rename:** List of fields to rename. This is a comma separated list of key-value pairs,
where each pair is separated by a colon and specifies the input name and the output name.

For example: 'datestr:date,timestamp:ts' specifies that the 'datestr' field should be
renamed to 'date' and the 'timestamp' field should be renamed to 'ts'.

**Convert:** List of fields to convert to a different type. This is a comma-separated list
of key-value pairs, where each pair is separated by a colon and specifies the field name
and the desired type.

For example: 'count:long,price:double' specifies that the 'count' field should be
converted to a long and the 'price' field should be converted to a double.

Only simple types are supported (boolean, int, long, float, double, bytes, string). Any
simple type can be converted to bytes or a string. Otherwise, a type can only be converted
to a larger type. For example, an int can be converted to a long, but a long cannot be
converted to an int.

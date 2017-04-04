# Projection


Description
-----------
The Projection transform lets you drop, keep, rename, and cast fields to a different type.
Fields are first dropped based on the drop or keep field, then cast, then renamed.

For example, suppose the transform is configured to drop field 'B' and rename field 'A' to 'B'.
If the transform receives this input record:

    +============================+
    | field name | type | value  |
    +============================+
    | A          | int  | 10     |
    | B          | int  | 20     |
    +============================+

field 'B' will first be dropped:

    +============================+
    | field name | type | value  |
    +============================+
    | A          | int  | 10     |
    +============================+

and then field 'A' will be renamed to 'B':

    +============================+
    | field name | type | value  |
    +============================+
    | B          | int  | 10     |
    +============================+

Similarly, the transfrom will first check if it should keep a field, and then rename it if configured to do so.

Use Case
--------
The transform is used when you need to drop fields, keep specific fields, change field types, or rename fields.

For example, you may want to rename a field from ``'timestamp'`` to ``'ts'`` because you want
to write to a database where ``'timestamp'`` is a reserved keyword. You might want to
drop a field named ``'headers'`` because you know it is always empty for your particular
data source. Or, you might want to only keep fields named ``'ip'`` and ``'timestamp'`` and discard 
all other fields.


Properties
----------
**drop:** Comma-separated list of fields to drop. For example: ``'field1,field2,field3'``.

**keep:** Comma-separated list of fields to keep. For example: ``'field1,field2,field3'``.

Note: Drop and keep fields cannot *both* be specified. At least one must be null or empty.

**rename:** List of fields to rename. This is a comma-separated list of key-value pairs,
where each pair is separated by a colon and specifies the input and output names.

For example: ``'datestr:date,timestamp:ts'`` specifies that the ``'datestr'`` field should be
renamed to ``'date'`` and the ``'timestamp'`` field should be renamed to ``'ts'``.

**convert:** List of fields to convert to a different type. This is a comma-separated list
of key-value pairs, where each pair is separated by a colon and specifies the field name
and the desired type.

For example: ``'count:long,price:double'`` specifies that the ``'count'`` field should be
converted to a long and the ``'price'`` field should be converted to a double.

Only simple types are supported (boolean, int, long, float, double, bytes, string). Any
simple type can be converted to bytes or a string. Otherwise, a type can only be converted
to a larger type. For example, an int can be converted to a long, but a long cannot be
converted to an int.


Example
-------
This example keeps only the ``'id'`` and ``'cost'`` fields. It also changes the type of the ``'cost'``
field to a double and renames it ``'price'``.

    {
        "name": "Projection",
        "type": "transform",
        "properties": {
            "drop": "",
            "convert": "cost:double",
            "rename": "cost:price",
	          "keep": "id,cost"
        }
    }
 
For example, if the transform receives this input record:

    +=========================================================+
    | field name | type                | value                |
    +=========================================================+
    | id         | string              | "abc123"             |
    | ts         | long                | 1234567890000        |
    | headers    | map<string, string> | { "user": "samuel" } |
    | cost       | float               | 8.88                 |
    +=========================================================+

It will transform it to this output record:

    +=========================================================+
    | field name | type                | value                |
    +=========================================================+
    | id         | string              | "abc123"             |
    | price      | double              | 8.88                 |
    +=========================================================+

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0

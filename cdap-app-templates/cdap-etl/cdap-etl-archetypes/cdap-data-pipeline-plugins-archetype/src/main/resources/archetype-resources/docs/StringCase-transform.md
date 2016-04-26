# String Case Transform

Description
-----------

Changes configured fields to lowercase or uppercase.

Use Case
--------

This transform is used whenever you need to uppercase or lowercase one or more fields.

Properties
----------

**lowerFields:** Comma separated list of fields to lowercase.

**upperFields:** Comma separated list of fields to uppercase.

Example
-------

This example lowercases the 'name' field and uppercases the 'id' field:

    {
        "name": "StringCase",
        "type": "transform",
        "properties": {
            "lowerFields": "name",
            "upperFields": "id"
        }
    }

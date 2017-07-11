# Value Mapper


Description
-----------
Value Mapper is a transform plugin that maps string values of a field in the input record
to a mapping value using a mapping dataset.

Mappings for the values are usually stored in a key-value dataset. The ValueMapper transform
provides a simple method for manipulating input data, both a field and its values, using a mapping.


Use Case
--------
One use is to replace language codes in the input record field with an
appropriate language description:

     **Source field name:** language_code
     **Target field name:** language_desc
     **Mappings, source to target:** DE/German, ES/Spanish, EN/English

This will replace the source column *language_code* with the target column
*language_desc*, replacing values found in the source field using the mappings
"DE" to "German", "ES" to "Spanish", and so on.


Properties
----------
**mapping:** A comma-separated list that defines the mapping of a source
field to a target field and the mapping table name for looking up values.
Contains three properties separated by a colon (":") as the source field, the
mapping table name, and the target field:

         <source-field>:<mapping-table-name>:<target-field>

Note: **source field** supports only STRING types.

**defaults:** A comma-separated list that contains key-value pairs of a
source field and its default value for cases where the source field
value is either null or empty or if the mapping key-value is not present. If
a default value has not been provided, the source field value will be
mapped to the target field. Only STRING NULLABLE type values are accepted.
Example: <source field>:<defaultValue>


Example
-------
As an example, take employee details as input data through a stream and then apply
the ValueMapper transform on the *designation* field in the input data.

The plugin JSON Representation will be:

    {
        "name": "ValueMapper",
        "type": "transform",
        "properties": {
            "mapping": "designation:designationLookupTableName:designationName",
            "defaults": "designation:DefaultDesignation"
        }
    }


If the transform receives as an input record:

First Row:

    +=========================================================+
    | field name | type                | value                |
    +=========================================================+
    | id         | string              | "1234"               |
    | name       | string              | "John"               |
    | salary     | int                 | 9000                 |
    | designation| string              | "2"                  |
    +=========================================================+

Second Row:

    +=========================================================+
    | field name | type                | value                |
    +=========================================================+
    | id         | string              | "1967"               |
    | name       | string              | "Kelly"              |
    | salary     | int                 | 5000                 |
    | designation| string              | "5"                  |
    +=========================================================+

with this as the mapping dataset, *designationLookupTableName*:

    +======================+
    | key       | value    |
    +======================+
    | "1"       | "SE"     |
    | "2"       | "SSE"    |
    | "3"       | "ML"     |
    +======================+


After transformation by the ValueMapper plugin, the output will have this structure and contents, with the
*designation* column replaced by the *designationName* column, using values looked up from the
mapping database and default key-value pair:

First Row:

    +=========================================================+
    | field name      | type                | value           |
    +=========================================================+
    | id              | string              | "1234"          |
    | name            | string              | "John"          |
    | salary          | int                 | 9000            |
    | designationName | string              | "SSE"           |
    +=========================================================+

Second Row:

    +=============================================================+
    | field name      | type                | value               |
    +=============================================================+
    | id              | string              | "1967"              |
    | name            | string              | "Kelly"             |
    | salary          | int                 | 5000                |
    | designationName | string              | "DefaultDesignation"|
    +=============================================================+

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0

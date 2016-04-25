# Word Count Spark Sink

Description
-----------

For the configured input string field, counts the number of times each word appears in that field.
The results are written to a CDAP KeyValueTable.

Use Case
--------

This plugin is used whenever you want to count and save the number of times each word appears in a field.

Properties
----------

**field:** The name of the string field to count words in.

**tableName:** The name of KeyValueTable to store the results in.

Example
-------

This example counts the words in the 'text' field and stores the results in the 'wordcounts' KeyValueTable:

    {
        "name": "WordCount",
        "type": "sparksink",
        "properties": {
            "field": "text",
            "tableName": "wordcounts"
        }
    }

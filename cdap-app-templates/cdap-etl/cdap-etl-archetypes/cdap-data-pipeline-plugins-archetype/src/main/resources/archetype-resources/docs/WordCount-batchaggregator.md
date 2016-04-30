# Word Count Batch Aggregator

Description
-----------

For the configured input string field, counts the number of times each word appears in that field.
Records output will have two fields -- word (string), and count (long).

Use Case
--------

This plugin is used whenever you want to count the number of times each word appears in a field.

Properties
----------

**field:** The name of the string field to count words in.

Example
-------

This example counts the words in the 'text' field:

    {
        "name": "WordCount",
        "type": "batchaggregator",
        "properties": {
            "field": "text"
        }
    }

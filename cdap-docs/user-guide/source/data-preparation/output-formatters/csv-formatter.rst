.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-write-as-csv:

================
Write as CSV
================

#
Write as CSV

WRITE-AS-CSV directive converts the record into CSV.

## Syntax
```
write-as-csv <column>
```

```column``` will contain the CSV representation of the record.

## Usage Notes

The WRITE-AS-CSV directive provides an easy way to convert the record
into a CSV. If the ```column``` already exists, it will overwrite it.


## Example

Let's consider a very simple record
```
{
"int" : 1,
"string" : "this is string",
}
```

running the directive
```
write-as-csv body
```

would generate the following record

```
{
"body" : "1,\"this is, string\",
"int" : 1,
"string" : "this is, string",
}
```

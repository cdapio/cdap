.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-notations:

================
Notations
================

#
Notations

### Directives

A directive is represented as simple text in the format as specified below

```
<command> <argument> <argument> ... <argument>
```

### Record

A record in this documentation will be represented as JSON object with object key representing the column name and the value representing the data without any mention of types.

E.g.

```
{
"id" : 1,
"fname" : "root",
"lname" : "joltie",
"address" : {
"housenumber" : "678",
"street" : "Mars Street",
"city" : "Marcity",
"state" : "Maregon",
"country" : "Mari"
}
"gender", "M"
}
```



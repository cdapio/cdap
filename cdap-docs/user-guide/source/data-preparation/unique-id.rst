.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-unique-id:

=========
Unique ID
=========

Generate UUID
=============

#


GENERATE-UUID directive generates universally unique identifier (UUID)

## Syntax
```
generate-uuid <column>
```

```column``` is set to the UUID generated.

## Usage Notes

Generates type 4 - pseudo randomly generated UUID. The UUID is generated using a cryptographically
strong pseudo random number generator.

## Examples

Let's say you would like to generate a random identifier for each record to uniquely identify the record, so let's
start with a simple record as shown below:

```
{
"x" : 1
"y" : 2
}
```

Applying the directive below

```
generate-uuid uuid
```

would produce the following result

```
{
"x" : 1
"y" : 2
"uuid" : "57126d32-8c91-4c00-9697-8abda450e836"
}
```

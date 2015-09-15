.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Transformations: ScriptFilter 
===============================

.. rubric:: Description

A transform that filters records using a custom Javascript function.

.. rubric:: Use Case

The transform is used when you need to filter records.
For example, you may want to filter out records that have null values for an important field.

.. rubric:: Properties

**script:** Javascript that implements a function 'shouldFilter', taking a JSON object
representation of the input record, and returning true if the input record should be
filtered and false if not.

.. rubric:: Example

::

  {
    "name": "ScriptFilter",
    "properties": {
      "script": "function shouldFilter(input) { return input.count > 100; }",
    }
  }

This example filters out any records whose 'count' field contains a value greater than 100.

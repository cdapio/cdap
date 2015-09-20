.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-plugins-transformations-scriptfilter:

===============================
Transformations: ScriptFilter 
===============================

.. rubric:: Description

A transform plugin that filters records using a custom Javascript provided in the plugin's config.

.. rubric:: Use Case

The transform is used when you need to filter records. For example, you may want to filter
out records that have null values for an important field.

.. rubric:: Properties

**script:** Javascript that must implement a function ``'shouldFilter'``, that takes a
JSON object representation of the input record, and returns true if the input record
should be filtered and false if not.

.. rubric:: Example

::

  {
    "name": "ScriptFilter",
    "properties": {
      "script": "function shouldFilter(input) { return input.count > 100; }",
    }
  }

This example filters out any records whose ``'count'`` field contains a value greater than 100.

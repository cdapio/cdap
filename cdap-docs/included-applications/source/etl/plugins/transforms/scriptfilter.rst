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

**script:** Javascript that must implement a function ``'shouldFilter'``, takes a
JSON object (representing the input record) and a context object (which encapsulates CDAP metrics and logger),
and returns true if the input record should be filtered and false if not.

.. rubric:: Example

::

  {
    "name": "ScriptFilter",
    "properties": {
      "script": "function shouldFilter(input, context) {
       if (input.count < 0) {
          context.getLogger().info("Got input record with negative count");
          context.getMetrics().count("negative.count", 1);
        }
       return input.count > 100; }",
    }
  }

This example filters out any records whose ``'count'`` field contains a value greater than 100.

**Note:** These default metrics are emitted by this transform:

.. csv-table::
   :header: "Metric Name","Description"
   :widths: 40,60

   "``records.in``","Input records processed by this transform stage"
   "``records.out``","Output records sent to the next stage"
   "``filtered``","Input records filtered at this stage"

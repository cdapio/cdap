.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Transformations: ScriptFilter 
===============================

.. rubric:: Description: A transform plugin that filters records

A transform plugin that filters records using a custom Javascript provided in the plugin's
config.

Javascript that must implement a function 'shouldFilter' that takes a JSON object
representation of the input record, and returns true if the input record should be
filtered and false if not.

For example: 'function shouldFilter(input) { return input.count > 100'; } will filter out
any records whose 'count' field is greater than 100.


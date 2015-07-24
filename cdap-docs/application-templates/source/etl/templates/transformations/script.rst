.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Transformations: Script 
===============================

.. rubric:: Description: Executes user-provided Javascript

Executes user-provided Javascript in order to transform one record into another.

**Script:** Javascript defining how to transform one record into another. The script must
implement a function called 'transform', which take as input a JSON object that represents
the input record, and returns a JSON object that respresents the transformed input.

For example: 'function transform(input) { input.count = input.count * 1024; return
input; }' will scale the 'count' field by 1024."


**Schema:** The schema of output objects. If no schema is given, it is assumed that the
output schema is the same as the input schema.

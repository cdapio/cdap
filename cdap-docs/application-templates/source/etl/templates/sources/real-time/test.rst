.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sources: Real-time: Test 
===============================

.. rubric:: Description: Source that can generate test data

Source that can generate test data for Real-time Stream and Table Sinks.

**Type:** The type of data to be generated. Currently, only two types |---| 'stream' and
'table' |---| are supported. By default, it generates a structured record containing one
field named 'data' of type String with the value 'Hello'.
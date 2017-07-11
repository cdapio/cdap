# Window Plugin


Description
-----------
The Window plugin is used to window a part of a streaming pipeline.


Use Case
--------
Windows are used when you wish to group smaller batches of events into larger batches of events.
For example, if your streaming pipeline has a batch interval of one second, you may want to accumulate
thirty seconds of events before running an aggregation, in order to accumulate enough events to generate
useful aggregates. You may also want to slide the window at a slower frequency. For example, you may want to
calculate those aggregates over every five seconds instead of every second.

Properties
----------
**width:** The width of the window in seconds. Must be a multiple of the batch interval.
Each window generated will contain all the events from this many seconds.

**slideInterval:** The sliding interval of the window in seconds. Must be a multiple of the batch interval.
Every this number of seconds, a new window will be generated.

Example
-------
This example creates windows of width five seconds that slides every two seconds. In order to use this
configuration, the batch interval for the entire pipeline must be set to one second, as both the width
and slide interval must be multiples of the batch interval.

    {
        "name": "XMLReaderBatchSource",
        "plugin":{
            "name": "Window",
            "type": "windower",
            "properties":{
                "width": "5",
                "slideInterval": "2"
            }
        }
    }

With this setup, the plugin would generate these windowed batches:

    +====================================+
    | time | input | output              |
    +====================================+
    | 1    | x1    |                     |
    | 2    | x2    | [ - - - x1 x2 ]     |
    | 3    | x3    |                     |
    | 4    | x4    | [ - x1 x2 x3 x4 ]   |
    | 5    | x5    |                     |
    | 6    | x6    | [ x2 x3 x4 x5 x6 ]  |
    | 7    | x7    |                     |
    | 8    | x8    | [ x4 x5 x6 x7 x8 ]  |
    | 9    | x9    |                     |
    | 10   | x10   | [ x6 x7 x8 x9 x10 ] |
    +====================================+

---
- CDAP Pipelines Plugin Type: windower
- CDAP Pipelines Version: 1.7.0

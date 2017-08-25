.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide: Getting Started

:hide-toc: true

============================================== 
Example: Analyzing and Masking IoT Device Data
==============================================

Introduction
------------
This tutorial demonstrates how to use CDAP's Data Preparation and Data Pipelines to clean, prepare, mask, and store IoT device data sent in JSON format.

Scenario
---------
You receive FitBit device data in JSON format. You are interested in sharing the data with an outside contractor, but you need to mask the data to remove personally identifying information before sharing (i.e., data masking). 

- You will parse FitBit JSON data, extract the UNIX timestamp, mask the devices' IDs, hash the results, and store into a CDAP table

- You will write masked results to a database that will be used by the contractor

Data
----
Click below to download a `.json` file containing the data necessary to complete the tutorial.

:download:`FitBit_Device.json </_include/tutorials/FitBit_Device.json>`

Video Tutorial
--------------

..  youtube:: V8e6yr8hpZA

Step-by-Step Walkthrough
------------------------

Loading the Data
~~~~~~~~~~~~~~~~
Download the data linked in the Data section above. Open Data Preparation, and upload `FitBit_Device.json` as a "File."

Once the data has been loaded into the `body` column, choose Parse > JSON with Depth 1 from the `body` column drop-down menu. This will create a row that contains each JSON object from the array.

.. figure:: /_images/tutorials/fitbit/parse_body.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

To split the JSON fields into columns, apply the Parse > JSON directive one more time to the `body` column.

You will now have four columns: `body_device_id`, `body_calories_burnt`, `body_duration`, `body_timestamp`.

Drop the `body_duration` column by selecting the drop-down menu and choosing Delete Column. You don't need this column since every row has the same value of `60`.

Masking the Device IDs
~~~~~~~~~~~~~~~~~~~~~~~
This data contains FitBit device IDs. This is personally identifying information and potentially compromising to the users whose data has been collected. You want to mask this data to ensure that important personal information cannot be stolen by malicious actors.

To do this, you can apply the `Mask Data` directive from the drop-down menu of the `body_device_id` column. 

.. figure:: /_images/tutorials/fitbit/mask.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Select `Show last 4 characters only`. This will mask all characters with the hash, except for the last four characters. `By Shuffling` will randomly shuffle the numbers/characters in the column so that the original information cannot be reconstructed.

.. figure:: /_images/tutorials/fitbit/masked_data.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image


Getting the Time of Day from the UNIX Timestamp
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
`body_timestamp` is formatted as a UNIX timestamp, which represents the number of seconds that have elapsed since the Epoch, which is January 1st, 1970. 

To find the time of data, you can calculate the modulo of the timestamp by 86400, which is the number of seconds per day. The exact operation is:

``body_timestamp % 86400``

which will yield the time since midnight.

First, you will notice that the type of `body_timestamp` is a String. You cannot perform mathematical operations on a String! To get around this, You need to convert `body_timestamp` to an appropriate data type, such as a float. 

.. figure:: /_images/tutorials/fitbit/string.jpeg
	:figwidth: 100%
	:width: 250px
	:align: center
	:class: bordered-image

To do, so type the following directive into the prompt at the bottom of the screen:

.. figure:: /_images/tutorials/fitbit/float.jpeg
	:figwidth: 100%
	:width: 800px
	:align: center
	:class: bordered-image

The `set-type` directive is used for converting between different data types. Here, you have converted a String to a Float, which is used to represent floating-point decimal numbers.

Now that you have the timestamp in the proper data type, you want to calculate the modulo as described above. You can use the `body_timestamp` drop-down menu to apply the modulo operation. Select Calculate > Modulo, then specify 86400.

.. figure:: /_images/tutorials/fitbit/mod.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

You will see the following data appear below. These values represent seconds past midnight. For example, the first row contains the value 3070.0, which means that the FitBit data was read 3070.0 seconds after midnight.

.. figure:: /_images/tutorials/fitbit/seconds_after.jpeg
	:figwidth: 100%
	:width: 300px
	:align: center
	:class: bordered-image


Examining Options for Handling Invalid Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Scanning your data, you will see that not all the data is valid. Row 13 lists `body_calories_burnt` as -7. While it is definitely possible to burn negative calories (such as by eating a donut), it is more likely that this is an erroneous reading from the FitBit.

Before you can handle this erroneous data, you need to change the data type of `body_calories_burnt` from String to Float. You can accomplish this by applying the directive `set-type body_calories_burnt Float`.

Now, you can handle the invalid data. 

First, you will look at the `send-to-error` directive. `send-to-error` marks a record as erroneous when it is processed in a pipeline, and results in the record being written to an error node, rather than the next nodes in a data flow.

.. figure:: /_images/tutorials/fitbit/sendtoerror.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

When you apply this directive, you will see the following:

.. figure:: /_images/tutorials/fitbit/nodata.jpeg
	:figwidth: 100%
	:width: 700px
	:align: center
	:class: bordered-image

What happened here? 

In Data Preparation, you ingested a single "record," which is the FitBit.json file. Although you have split this single record into several output records, it is still a single input record. Hence, when you apply `send-to-error`, you mark the whole record as erroneous. Consequently, no data is shown.

`send-to-error` is very useful when you don't want to accept your data as a whole unless everything is valid. 

In this case, it is OK to have an erroneous reading, as it won't affect the overall outcome of your analysis. 

To remove the `send-to-error` directive, nagivate the right side bar and click the "x" next to `send-to-error`, which is directive number 12.

.. figure:: /_images/tutorials/fitbit/remove.jpeg
	:figwidth: 100%
	:width: 300px
	:align: center
	:class: bordered-image

Your data will reappear.

Instead of sending the record to error, you can apply apply a Filter. While this does not generate an error record in Data Pipelines, it does remove invalid data upon processing. 

Choose `Filter` from the `body_calories_burnt` column, then `Remove Rows` on a `Custom condition`. Specify the condition as less than zero.

.. figure:: /_images/tutorials/fitbit/filter_neg.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

You will see that the erroneous row has now been removed.

Encoding the Data for Transmission
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Plain text in transmission is more resilient to transmission errors when it is encoded in Base64. You would therefore like to encode all your columns in Base64.

First, convert `body_calories_burnt` and `body_timestamp` back to strings by applying the directive `set-type body_calories_burnt String` and `set-type body_timestamp String`. 

Now, from the drop down menu on any column, choose `Encode`, then `Base64`.

.. figure:: /_images/tutorials/fitbit/encode.jpeg
	:figwidth: 100%
	:width: 300px
	:align: center
	:class: bordered-image

Repeat this for all columns. The data will now be encoded in Base64 format. Delete the original columns.

.. figure:: /_images/tutorials/fitbit/encoded_data.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Storing the Results in a Table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Finally, you want to write your data to a CDAP Table Dataset, which can be exported and sent to the analysts who will study the masked data.

A CDAP Table Dataset requires a unique identifier for each row. Because you have masked the unique device IDs, it is possible that the Base64 encoding of the last 4 digits match. To be sure that no rows are overwritten, you will use the `generate-uuid` directive.

A UUID is a unique identifier. The `generate-uuid` generates a UUID for each row. Type `generate-uuid uuid` in the prompt the bottom of the screen, which will create a new column called `uuid`. 

Now, you are ready to ingest the data into a CDAP Table Dataset.

Click `Ingest Data` in the upper right hand corner. 

.. figure:: /_images/tutorials/fitbit/ingest.jpeg
	:figwidth: 100%
	:width: 250px
	:align: center
	:class: bordered-image

Select `Table` and name this table "FitBitTable." The `Row Key` should be specified as `uuid` since you know that this value is unique.

.. figure:: /_images/tutorials/fitbit/ingest_config.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Click `Ingest Data`. Once the task has completed, click `Explore Data`.

.. figure:: /_images/tutorials/fitbit/explore.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Execute the query that you see on the screen. You will see the data that you have just prepared was written to the table!

.. figure:: /_images/tutorials/fitbit/result.jpeg
	:figwidth: 100%
	:width: 700px
	:align: center
	:class: bordered-image

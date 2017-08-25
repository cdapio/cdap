.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide: Getting Started

:hide-toc: true

==========================================================
Example: Creating a Delivery Route from Customer Addresses
==========================================================

Introduction
------------
This tutorial demonstrates how to use CDAP's Data Preparation and Data Pipelines to clean, prepare, and store customer data from a MySQL database. You will learn how to connect CDAP to a data source, how to apply basic transforms, and how write to a CDAP dataset. 

Scenario
---------
Using customer address data, you want to create custom marketing materials for an ongoing promotion and distribute them into mailboxes. However, you put two constraints on your campaign:

- It only targets customers in California, Washington, or Oregon

- In order to save fuel and money, the campaign will only make deliveries to streets that are Avenues (not Roads or Courts), since these are more likely to be easily accessible by car

Data
------
Click below to donwload a `.zip` file containing the data necessary to complete the tutorial.

:download:`Zipfile </_include/tutorials/campaign-data.zip>`

Video Tutorial
--------------

..  youtube:: AzQuoIE-jak

Step-by-Step Walkthrough
------------------------

Loading the Data
~~~~~~~~~~~~~~~~
To start, you need to import the customer data. ``demo.sql`` contains the customer data. In your shell, log into MySQL (for me, this looks like ``mysql -u root``) and create a database called ``demo`` (by running ``CREATE DATABASE demo;``). Then exit mysql ('exit;``).

In your shell, nagivate to the same directory as ``demo.sql,`` and run ``mysql -u root -p demo < demo.sql``. The database ``demo`` should now contain a table ``customer`` with customer data. 

Open CDAP and nagivate to Data Preparation using the top bar. In the left sidebar (this can be accessed through the arrow in the top left corner if it not already visible), click "Add Connection." Select the source "Database."

If you have the MySQL driver installed, skip this step. If not, exit the prompt and click "Cask Market" in the upper right hand corner. On the left menu bar, select "Drivers." Click on the MySQL JDBC driver, and then follow the on-screen wizard to install the driver.

In the "Add Connection" prompt, choose a name for the database (this is a name for your own reference). Specify ``host`` as ``localhost``, ``port`` as ``3306``, and correct username/password (for me, this is ``root`` with no password). Click "Test Connection" to verify the connection works, and select the database ``demo``.

.. figure:: /_images/tutorials/address/address_connect.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Once you have connected to the database, click on the database name you chose, which will be under the "Database" header on the left side panel. Then choose the ``customer`` table. You should see the customer data displayed in row/column form. 

Next, you need to import another file, ``states.json``. Click the gray table with white arrow in the upper left hand corner, and navigate to where ``states.json`` is stored in your system. Simply click the file once to upload.

Abbreviating the State Names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The delivery vehicle's nagivation systems only reognizes address which contain abbreviated state names, such as "CA" rather than "California." However, the customer data only contains the full state names.  
The ``states.json`` file which we just imported contains two columns: one with the full state names and one with the abbreviated state names. We can use this as map to update the state names in our customer data. 

To create our mapping, let's open the ``states.json`` tab. In the Data Prep UI, select this tab. Using the caret icon next to the ``Body`` column, select "Parse" and "JSON". Apply this directive twice. 

We apply it twice because we first must parse the array, then each JSON object in the array. Change the column names ``body_name`` to ``name`` and ``body_abbreviation`` to abbreviation simply by clicking on the title and replacing the text.

.. figure:: /_images/tutorials/address/address_parse_states.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Now, click "Create Pipeline" and select "Batch". You are now in the Pipelines UI, and you will see a "File" stage feeding into a "Wrangler" stage. This "Wrangler" stage represents the directives you just applied in Wrangler.

In the left side bar, click on "Sink" and select both the "CDAP Table Dataset" and "Avro Time Partitioned Dataset" plugins.  Connect the output of the "Wrangler" stage into "CDAP Table Dataset." Click the "CDAP Table Dataset" stage, and add "name" as the "Row Field."

Name the Pipeline "StateNamePipeline." Then, deploy the pipeline by clicking "Deploy." Run the pipeline by clicking "Run".

You have created a CDAP Table Dataset that you can use to update the state names in the customer data from their full to abbreviated versions.

Updating the State Names in the Customer Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You now can now replace the full state names with the abbreviation. Navigate back to Data Preparation, and choose the ``customer`` tab.

Since you cannot perform a lookup on a ``null`` state value, you need to make sure there are no null state values. To do so, select the caret icon on the left side of the ``State Column``. Navigate to ``Filter``, and then ``Remove Rows`` if ``value is empty``, as shown below. 

.. figure:: /_images/tutorials/address/address_clean_null.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

You can now use the :ref:```table-lookup`` <table-lookup>` directive to replace the full state names. 

In CDAP, a directive is a command that is used in Data Preparation to perform a transformation. The ``table-lookup`` directive is a directive that is used to map a value stored in a column to another, using data stored in a CDAP table. For example, you will use the ``StateNameTable`` to lookup the abbreviated state name.

The directive is in the form ``table-lookup <column> <table>``. ``column`` in this case is ``state``, and table is ``StateNameTable``. Apply the full directive (``table-lookup state StateNameTable``) in the command prompt at the bottom of the screen, as shown in the image below.

.. figure:: /_images/tutorials/address/address_lookup.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

You will see a new column, ``state_abbreviation``, appear. 

Directives entered into the the command prompt at the bottom of the screen are applied in the same way as directives applied through each columns' drop-down menu. In fact, when you select a filter, for example, from a drop down menu, Data Preparation automatically generates and applies the equivalent directive. You can see this by selecting ``Directives`` in the right-hand sidebar. Clicking "x" next to a directive removes the correpsonding transformation.

Since you no longer need the full state name, you can delete this column. Select the caret to the left of ``state``, and choose the ``Delete Column`` option. Further, you can rename ``state_abbreviation``. Double-click the column name, and the text will become editable. Replace it with "State."

Choosing the Correct States
~~~~~~~~~~~~~~~~~~~~~~~~~~~
You only want your campaign to target consumers along the Pacific Coast: California, Oregon, or Washington. Therefore, you need to remove all rows which contain values other than ``CA``, ``OR``, or ``WA`` in ``state``.

To do so, navigate to the caret icon to the left of the state name. Select this caret, and choose ``filter``. Choose ``Keep Rows``, and use the drop-down menu to select ``if Value Matches Regex``.

You need your regex to match against ``CA``, ``OR``, or ``WA``. The regex ^(CA|OR|WA)$ accomplishes this, as shown below.

.. figure:: /_images/tutorials/address/address_regex.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Apply the filter and only the states matching your desired condition will remain.

Choosing the Correct Street Type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Because you believe it will be more fuel and cost efficient to only deliver to addresses that are on avenues (since these routes are more centrally-located) you only want to keep addresses that contain the word "Avenue". 

This task is not as simple as it may seem at first. For states, you could simply filter our the state name, since there was no additional text in the column. However, an example street address looks like:

``61 Summit Avenue``

This means you cannot simply filter that requires the column to be equal to the word "Avenue." 

To work around this, we will use the the ``Contains`` features. Select the caret in the address column, and choose ``Filter`` and ``Keep Value if Contains``. Enter ``Avenue``. Also, choose ``ignore case``. Apply the filter. Simple!

.. figure:: /_images/tutorials/address/address_street_type.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

You will see that only one customer remains. At first, you may be alarmed. However, Data Preparation only shows the first 100 values from your dataset. This is because Data Preparation is a playground that allows you to see the effects of transformations on a small subset of your data before dispatching large, parallel-processing jobs on the entire dataset.

Final Steps: Cleaning the Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This last stage is required to ensure the data is cleaned and prepared before we write it to a dataset, which can be accessed by the navigation system.

Your navigation system does not need country name, so there is no use for the ``country`` column. Select the caret next to ``country`` and choose ``Delete Column``. You should see the column containing the value ``USA`` dissapear.

The data is now prepared and ready to be written to a dataset.

Writing to a Dataset
~~~~~~~~~~~~~~~~~~~~
The last stage is to write the clean data to a dataset. Whereas Data Preparation only selects a small subset of your data (100 records) for transformations, Data Pipelines runs a Spark or MapReduce job that parallelizes these same transformations on a cluster of machines. This enables you to apply to complex transforms over vast quantities of data very quickly. 

Click ``Create Pipeline``, and select a ``Batch Pipeline``. You want Batch since your MySQL database is not a real-time source of data. 

In the Data Pipelines UI, you will see a Database (with the annotation ``customer``) stage connected into a Wrangler stage. The Wrangler state contains all the transformations you applied in Data Preparation. 

Navigate to the "Sink" section of the left-side bar, and choose a ``Avro Time Partitioned Dataset`` sink. Connect the output of Wrangler into this sink. Double-click on the ``Avro Time Partitioned Dataset`` sink, and give it the name ``CampaignSink``. Similarly, name your pipeline ``CampaignPipeline``.

You should now be able to deploy the Pipeline. Click ``Deploy`` in the upper right hand corner. When it is deployed, click ``Run``.

Once the Pipeline has run, double click on your ``Avro Time Partitioned Dataset`` sink. In the menu that pops up, you will see a button that says ``View Details``. Once you have chosen this view, select the "Eye" icon. Execute the SQL query that is pre-populated in the field. You will see a SQL Query result appear below. Click the "Eye" next to this query, and you will see the results of the Pipeline.

.. figure:: /_images/tutorials/address/address_results.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

The prepared data is now stored ``CampaignSink`` dataset, and can be accessed directly through a RESTful interface or the CDAP UI.

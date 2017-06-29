.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide: Getting Started

:hide-toc: true

=================================================================
Example: Creating a Mailing Address from Incomplete Customer Data
=================================================================

Question
--------
I have customer data in a MySQL database. I want to create mailing labels from this data. However, I only have street number, city, and state, but no zip. How can I generate a mailing address?

Data
------
Click below to donwload the file containing the required datasets.

:download:`Zipfile </_include/tutorials/address-data.zip>`

Video Tutorial
--------------
..  youtube:: ntOXeYecj7o

Step-by-Step Walkthrough
------------------------
In this tutorial, we will prepare envelope mailing addresses from customer data using two auxilary files, one which maps cities to ZIP codes and one which maps state names to abbreviated state names. 

We want to create mailing addresses in the form:

``Betty\n09150 Mcbride Pass\nEvansville, IN, 47720``	

since our mail printing system recognizes the ``\n`` delimiter.

To start, we need to import the data. ``demo.sql`` contains the customer data. In your shell, log into MySQL (for me, this looks like ``mysql -u root``) and create a database called ``demo`` (by running ``CREATE DATABASE demo;``). Then exit mysql ('exit;``).

In your shell, nagivate to the same directory as ``demo.sql,`` and run ``mysql -u root -p demo < demo.sql``. The database ``demo`` should now contain a table ``customer`` with customer data. 

Open CDAP and nagivate to Data Preparation using the top bar. In the left sidebar (this can be accessed through the arrow in the top left corner if it not already visible), click "Add Connection." Select the source "Database."

If you have the MySQL driver installed, skip this step. If not, exit the prompt and click "Cask Market" in the upper right hand corner. On the left menu bar, select "Drivers." Click on the MySQL JDBC driver, and then follow the on-screen wizard to install the driver.

In the "Add Connection" prompt, choose a name for the database (this is a name for your own reference). Specify ``host`` as ``localhost``, ``port`` as ``3306``, and correct username/password (for me, this is ``root`` and no password). Click "Test Connection" to verify the connection works, and select the database ``demo``.

.. figure:: /_images/tutorials/address/address_connect.jpeg
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Once you have connected to the database, click on the database name you chose, which will be under the "Database" header on the left side panel. Then choose the ``customer`` table. You should see the customer data displayed in row/column form. 

Next, we need to import our two auxilary filesets, ``zips.txt`` and ``states.json``. Click the gray table with white arrow in the upper left hand corner, and navigate to where the files are stored in your system. Click the file to upload, and repeat this process to upload the next file. Both files should be loaded into Data Preparation now. 

Our goal is to create a Table which maps state names to their abbreviations, since our customer data only has the full state names. Similarly, we don't have the ZIP codes of our customers, so we want to create a mapping between cities and ZIP codes.

To create our first mapping, let's start with ``states.json``. In the Data Prep UI, select this tab. Using the caret icon next to the ``Body`` column, select "Parse" and "JSON". Apply this directive twice. We apply it twice because we first must parse the array, then each JSON object in the array. Change the column names ``body_name`` to ``name`` and ``body_abbreviation`` to abbreviation simply by clicking on the title and replacing the text.

.. figure:: /_images/tutorials/address/address_parse_states.jpeg
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Now, click "Create Pipeline" and select "Batch". You are now in the Pipelines UI, and you will see a "File" stage feeding into a "Wrangler" stage. This "Wrangler" stage represents the directives you just applied in Wrangler.

 In the left side bar, click on "Sink" and select both the "Table" and "TPFSAvro" plugins. Also select "Error Collector" which is in the "Error Handlers" section. Connect the output of the "Wrangler" stage into "Table." Click the "Table" stage, and add "name" as the "Row Field." Choose your own name for the "Name" field. Repeat for TPFSAvro plugin (the schema should be "body").

.. figure:: /_images/tutorials/address/address_state_pipeline.jpeg
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Deploy the pipeline by clicking "Deploy." Run the pipeline by clicking "Run".

Now, return to Data Preparation. Select 'zips.txt', and as you did above, parse this file as JSON. 

Here's where things get a little tricky. We want to use this file to create a mapping between cities and zip codes, but there could be cities with the same names in different states. 

To solve this, we will combine the city and state name into a new column so we can lookup on this column. Select the caret next to ``body_city`` and choose "Format", then "lowercase." Repeat with ``body_state``. Now, click the checkbox on the right side of each of these columns. Then, use the drop-down menu for either column to select "Join" with the configuration displayed in the photo below.

.. figure:: /_images/tutorials/address/address_rowkey_zip.jpeg
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Next, go ahead and check the boxes for ``body_loc`` and ``body_pop`` in addition, and choose "Delete Selected Columns" from one of the columns drop down menus. Finally, select the ``Address`` drop-down menu and choose "Find and Replace", finding ``[ ]`` (whitespace in the city names)  and replacing it with ``+``.

Now, click "Create Pipeline," and configure the pipeline in the same way we did above, using the sink name "ZipSink" and "Row Field" as "address".

Once you run this pipeline, we are now reaady to create the mailing addresses. First, return to Data Preparation and select the ``customers`` tab. From here, type the following command into the prompt at the bottom of the page:

.. figure:: /_images/tutorials/address/address_state_table.jpeg
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

This page looks up ``state`` in the StateTable and returns the corresponding abbrevation. It's good that we have this abbreviation, since our "Row Field" for the ZIP Table uses the format ``city,state_abbreviated``. To peform a ``table-lookup`` for the zip, we need to create the corresponding row key. We can repeat the procedure we used above, where we joined the city and state columns, formatting the in lower case and replacing whitespace with ``+``. Place this new key into a column called ``zip_lookup``.

Now we can run ``table-lookup zip_lookup ZipSink``, which will return a new column called zip. 

For our final act, we want to delete all columns except for ``name``, ``address``, ``city``, ``state``, and ``zip``. We want to combine these columns into a format like:

``Betty\n09150 Mcbride Pass\nEvansville, IN, 47720``	

To do this, we can start by checking the boxes alongside ``state`` and ``zip``. We choose the caret next to one of their names, then "Join", separated by ````, ````. We then repeat then repeat this, using the ````\n`````` delimiter where appropriate. Finally, we can drop all the extra columns created, and write to a database by clicking "Create Pipeline" and connecting a "TPFSAvro" plugin to the output of the Wrangler stage.
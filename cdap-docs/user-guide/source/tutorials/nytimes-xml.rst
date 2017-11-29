.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide: Getting Started

:hide-toc: true

.. _tutorials-nytimes:

==================================================
Example: Working with New York Times XML Feed Data
==================================================

Introduction
------------
This tutorial demonstrates how to use CDAP's Data Preparation and Data Pipelines to extract and serve business critical information drawn from the New York Times XML feed.

Scenario
---------
Your organization, a multinational enterprise with holdings across several continents, is interested in monitoring RSS XML data feeds from many different news sites, such as the the New York Times. You want to route stories about different regions to the relevant analysts.

- You want stories about Brazil to be written to dataset (that is displayed via a webapp) to the Latin America analysts

- You want stories about Russia to be written to dataset (that is displayed via a webapp) to the Asia analysts

Data
----
Click below to donwnload a `.xml` file containing the data necessary to complete the tutorial.

:download:`nytimes-world.xml </_include/tutorials/nytimes-world.xml>`

Video Tutorial
--------------

..  youtube:: e-5K4cxwGrc

Step-by-Step Walkthrough
------------------------

Loading the Data
~~~~~~~~~~~~~~~~
First, download the file from the `Data` section above.

To begin, navigate to the Data Preparation tab from the CDAP homepage. In Data Preparation, choose the arrow on the left hand side. Upload the `nytimes-world.xml` from the `File System.` 


Wrangling the XML
~~~~~~~~~~~~~~~~~
You should see a single row of XML data in the `body` column. From the drop down menu of the `body` column, choose Parse > XML to JSON. Apply the transformation with Depth of "1."

.. figure:: /_images/tutorials/nytimes/xmltojson.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

On the right hand side of your screen, there are two tabs used to control data selection and directives: `Columns` and `Directives.` Select the `Columns` table, and check the box next to `body_rss_channel_item.`

.. figure:: /_images/tutorials/nytimes/keep.jpeg
  :figwidth: 100%
  :width: 400px
  :align: center
  :class: bordered-image

You will see that the corresponding column has been highlighted. Using the caret icon next to the column name, select `Keep Selected Columns.`

`Keep Selected Columns` is a useful directive because it allows you to drop a large number of columns by only selecting the small subset you are interested in retaining.

All other columns should be deleted. Now, selecting the caret on `body_rss_channel_item,` apply Parse > JSON with Depth 1. 

Apply this directive on more time. You will see a total of 16 columns appear. Each row contains the metadata of a single New York Times story. 

Finally, you will remove all the columns that do not contain information useful to us. In the `Columns` tab, select:

- `body_rss_channel_item_link`
- `body_rss_channel_item_dc:creator`
- `body_rss_channel_item_title`
- `body_rss_channel_item_category`
- `body_rss_channel_item_pubDate`

Selecting the drop-down caret from one of these columns, choose the `Keep Selected Columns` option.

Rename the columns above (in the same order) to:

- `link`
- `creator`
- `title`
- `category`
- `pubDate`

You can change the column name by clicking on the name to make it editable.

.. figure:: /_images/tutorials/nytimes/prepared_data.jpeg
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Extracting the Country from Category Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Your goal is to send stories about Russia to a database monitored by the Asia team and stories about Brazil to a database monitored by the Latin America team. 

Inspecting the `category` column, you will see that the JSON object contains URLs in the following format:

``http://www.nytimes.com/namespaces/keywords/nyt_geo``

Using the ``nyt_geo`` tag, you can filter down to areas of geographic interest.

You now want to parse this JSON so that you can retrieve the areas of interest. From the caret-drop down option on the `category` column, choose Parse > JSON. Apply this directive to the `category` column one more time.

You will now have two new columns ``category_domain`` and ``category_content``. 

From ``category_domain`` drop-down menu, choose Filter > Keep Rows > If value contains. Specify that you want to retain the row if it contains `nyt_geo`. Click `Apply`.

.. figure:: /_images/tutorials/nytimes/prepared_data.jpeg
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Only stories that are tagged with a geographic category will remain. Since you have filtered the `category_domain` column down to a single value, you can go ahead and delete this column by choosing `Delete Column` from the drop-down menu.

In the pipeline you create at the end of the tutorial, you will direct Russia stories and Brazil stories to different databases.

Cleaning the URLs
~~~~~~~~~~~~~~~~~
First, you see that `link` column is in the format 

`http://www.nytimes.com/2016/09/24/world/asia/chinese-medicine-paul-unschuld.html?partner=rss&emc=rss`

You would prefer to clean the `partner=rss&emc=rss` suffix. Also, you only want to relay the relative path to `http://www.nytimes.com`. For example, the URL above would become:

`/2016/09/24/world/asia/chinese-medicine-paul-unschuld.html`

You can accomplish this by using using the `Extract Fields` feature. `Extract Field` provides a powerful suite of tools for automatically parsing features of your data such as URLs, e-mails, SSNs, and more.

From the `link` column drop-down, select Extract Fields > Using Patterns.

.. figure:: /_images/tutorials/nytimes/pattern.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

In the `Extract Fields Using Patterns` menu, choose `Start/End` pattern. Specify the start pattern to be `http://www.nytimes.com` and the end pattern to be `?partner=rss&emc=rss`.

Click `Extract`. You can now delete the `link` column and rename `link_1_1` to `link`.

Formatting the Date
~~~~~~~~~~~~~~~~~~~
You would like to turn the `pubDate` column, which is a String, into a `Date` object. From the `pubDate` column, choose Parse > Natural Date. The timezone is `GMT`.

When you apply this directive, you will see a new column called `pubDate_1` containing a Date object. You can delete the `pubDate` column and rename this new column to `pubDate`.

Cleaning the Author Names
~~~~~~~~~~~~~~~~~~~~~~~~~
Finally, all the author names are uppercase, such as `KIRK SEMPLE`. You would prefer to serve the names in a more professional format, i.e.,  `Kirk Semple`. 

Choose the `creator` column. Apply Format > To TitleCase. You will see that all the author names have now been transformed into the proper title case.

.. figure:: /_images/tutorials/nytimes/titlecase.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Creating the Pipeline
~~~~~~~~~~~~~~~~~~~~~
Now that you have prepared your data, you can create a pipeline that will send records to the Brazil/Russia databases. 

First, click `Create Pipleline`. Choose `Batch Pipeline`.

.. figure:: /_images/tutorials/nytimes/create_pipeline.jpeg
  :figwidth: 100%
  :width: 300px
  :align: center
  :class: bordered-image

You will see the following pipeline appear on your screen.

.. figure:: /_images/tutorials/nytimes/new_pipeline.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Add two `Avro Time Partitioned Dataset` sinks to the canvas, as well as two `Python` (from the `Transform` menu).

Arrange the canvas in the format shown below.

.. figure:: /_images/tutorials/nytimes/format.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

You can align the nodes by clicking the `Align` button, which is the fourth option from the top.

Open the top `Python` node configuration. Replace the pre-populated code with the following snippet:

.. code-block:: python

  def transform(record, emitter, context):
    if (record['category_content'] == 'Brazil'):
      emitter.emit(record)


This code will only pass a record to the sink if the category is tagged as 'Brazil'.

Change the `Label` to `BrazilEvaluator` and exit out of the settings window.

Now, open the configuration for the sink that the `BrazilEvaluator` write to. Change the `Label` and `Dataset Name` both to `BrazilSink`.

Repeat the above process, for the other branch of the pipeline, replacing `Brazil` with `Russia`. 

Name the pipeline `NewYorkTimesPipeline` by clicking on the `Name your pipeline` text on the top of the page and entering the title.

Your pipeline should now look like the image below.

.. figure:: /_images/tutorials/nytimes/final.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Once you deploy your pipeline, you can no longer edit the nodes or settings. Thus, you want to ensure that you data flow works as intended before you deploy the pipeline.

To do so, click `Preview`:

.. figure:: /_images/tutorials/nytimes/preview.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

When you click `Run`, you will see that the timer will begin tracking how long the pipeline has run. When it completes, you can see the records that have passed through each node. For example, clicking on the `BrazilEvaluator`, you can see that only records matching `Brazil` have passed:

.. figure:: /_images/tutorials/nytimes/brazil.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

You can now click `Deploy` and the run the deployed pipeline.

Querying the Results
~~~~~~~~~~~~~~~~~~~~
You can click on the `BrazilSink` to view the configuration, and select `View Details` in the upper right hand corner.

When the dataset page opens, click the eye icon in the upper right hand corner. Click `Execute` the window that appears. When the query executes, click the eye icon to view a subset of the results.

.. figure:: /_images/tutorials/nytimes/brazil_results.jpeg
  :figwidth: 100%
  :width: 500px
  :align: center
  :class: bordered-image

Congratulations! You have cleaned New York Times XML data, split it according to geographic region, and written to two databases. 

.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide: Getting Started

:hide-toc: true

============================================
Example: Building a Stock Selection Pipeline
============================================

Introduction
------------
This tutorial demonstrates how to use CDAP's Data Preparation and Data Pipelines to build a stock selection pipeline that ingests market data and identifies a set of stocks that should be purchased.

Scenario
---------
You work in the financial industry and trade in U.S. equities markets. To select top stocks for purchase, you need to ingest large amounts of market data with many different variables, process this data, and return your stock picks. 

- You want to write a stock selection pipeline that uses the criteria given by `Greenblatt's Magic Formula <https://en.wikipedia.org/wiki/Magic_formula_investing>`_. 

- You want to write your selections to a database that will be read by a program which submits your selections to the NYSE

Data
----
Click below to download a `.csv` file containing the data necessary to complete the tutorial.

:download:`stock_data.csv </_include/tutorials/stock_data.csv>`

Video Tutorial
--------------

..  youtube:: _LB3_qm5yQM

Step-by-Step Walkthrough
------------------------

Loading the Data
~~~~~~~~~~~~~~~~
First, download the file from the `Data` section above. 

To begin, navigate to the Data Preparation tab from the CDAP homepage. In Data Preparation, choose the arrow on the left hand side. 

Upload the `stock_data.csv` from the `File System.` 

Background
~~~~~~~~~~
You are building a stock selection pipeline. The pipeline takes in all stocks on the NYSE and at each stage in the pipeline and reduces the number of stocks that are under consideration for purchase. 

You are implementing a popular strategy in this tutorial known as `Greenblatt's Magic Formula <https://en.wikipedia.org/wiki/Magic_formula_investing>`_. You will adjust our strategy 
slightly:

1. Establish a minimum market capitalization greater than $50 million.
2. Exclude utility and financial stocks.
3. Determine company's return on capital = EBIT / (net fixed assets + working capital).
4. Rank all companies above chosen market capitalization by highest return on capital.
5. Invest in 20 highest ranked companies.

Establishing the Minimum Market Capitalization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
First, you want to establish a minimum market capitalization.

Start by selecting the `stock_data.csv` tab. Choose the drop-down menu for the `body` column and apply Parse > CSV with the 'Set First Row as Header' option selected. From the `body` column drop-down menu, choose `Delete Column` to delete the `body` column.

To calculate the market capitalization of the company, you want to find the share price multiplied by the number of outstanding shares. You will express this as:

`estimated_shares_outstanding * (high + low) / 2` 

where high and low are the stocks' daily high and lows.

You can use a custom `JEXL <http://commons.apache.org/proper/commons-jexl/reference/examples.html>`_ expression to calculate the `market_capitalization`.

First, you need to set the type of all the involved variables to the correct data type. Currently, `high`, `low`, and `estimated_shares_outstanding` are all strings.

At the bottom of the screen, type the following directive:

.. figure:: /_images/tutorials/stocks/set-type.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

This will change the data type for `high` to a double. Repeat this for `low` and `estimated_shares_outstanding`. All three of these columns should now be doubles.

Now, choose the `estimated_shares_outstanding` drop-down column and select the Custom Transform option. 

.. figure:: /_images/tutorials/stocks/market_cap.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

The result of the calculation was stored in `estimated_shares_outstanding` (since this was the column you selected for the custom expression), so rename it `market_capitalization` by clicking once on the column name and typing the new text.

Finally, you want to filter out all companies with a market capitalization under $50m dollars. Scanning the data on the screen, you will see that no companies displayed in Data Preparation have a market cap under $50m. However, Data Preparation only samples 100 rows from the dataset, so there may be companies that do have a market cap under $50m elsewhere in the data.

To filter out these small cap companies, select the `market_capitalization` column choose the Filter option. Apply the custom condition ">50000000", as shown below.

.. figure:: /_images/tutorials/stocks/under50.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Excluding Financial and Utility Stocks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Your next step is to exclude financial and utility stocks. This is similar step to establishing the minimum market capitalization. 

Select the drop down for the `gics_sector` column and choose Filter. Choose "Remove Rows" if the column contains "Financials" (as shown below). Repeat this setup for "Utilities."

.. figure:: /_images/tutorials/stocks/financials.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Calculate Return on Capital (ROC)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Our final setp is to Calculate Return on Capital (ROC). ROC is defined as:

`EBIT / (net fixed assets + working capital)`

or equivalently (using our columns):

` earnings_before_interest_and_tax / (fixed_assets + (total_equity - total_liabilities))`

Before you can calculate the ROC for each company, you need to convert the type from String to Double for the columns `earnings_before_interest_and_tax`, `fixed_assets`, `current_assets`, and `current_liabilities`. This can be achieved by using the `set-type` directive. For example, you should apply `set-type fixed_assets double` (in the directive prompt at the bottom of the screen).

Once you have converted these columns, select the `total_equity` column drop-down menu and choose "Custom Transformation." Apply the transformation `earnings_before_interest_and_tax / (fixed_assets + (total_equity - total_liabilities))`.

.. figure:: /_images/tutorials/stocks/roc.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Since the result has been stored in the `total_equity` column, rename this column to `roc`.

Finally, you would like to express the ROC as a percentage, rather than a decimal.

Select the drop-down menu for the `roc` column. Choose Calculate > Multiply, and multiply by 100. You will see that the `roc` column now contains the return on capital as a percentage.

.. figure:: /_images/tutorials/stocks/percentage.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Creating the Pipeline for Picking the Top 20 Stocks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Because Data Preparation only displays and operates on 100 records, you need a way to operationalize our logic for the whole dataset. Click "Create Pipeline" and choose Batch.

In this section, you will create a pipeline that will ingest all the stock data, filter by the criteria above, choose the top 20 stocks by ROC, and write to a dataset.

When you initially create the pipeline, you will see the view below.

.. figure:: /_images/tutorials/stocks/percentage.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

You need a way to feed the output from the Wrangler (Data Preparation) node into a node which will select the top 20 stocks.

You can turn to Cask Market for the Top-N plugin. CDAP comes bundled with many useful plugins. However, Cask Market - which is open app store for Big Data Applications - contains many more. 

Click `Cask Market` in the upper right hand corner to open the market.

.. figure:: /_images/tutorials/stocks/percentage.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

In the "Plugins" section, choose "Top-N."

.. figure:: /_images/tutorials/stocks/topn.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Deploy the Top-N application. Save your pipeline - giving it the name "StockPipeline" - and refresh the page. You will see the Top-N plugin appear in the Analytics section of the the plugin menu on the left side of your screen.

Add a Top-N node to the canvas, as well as a Avro Time Partitioned Dataset sink.

Name the Avro Time Partitioned Dataset sink `StockSink` and also specify `StockSink` as the "Database Name."

.. figure:: /_images/tutorials/stocks/stocksink.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

In the Top-N plugin, specify the `field` to be "roc" (since this is the row which you want ranked) and the `size` to be 20 (since you want the top 20 stocks).

Connect the nodes in the order shown below. 

.. figure:: /_images/tutorials/stocks/pipeline.jpeg
	:figwidth: 100%
	:width: 800px
	:align: center
	:class: bordered-image

Now click "Deploy" (found in the upper right hand corner). On the pipeline is deployed, press "Run."

Click the StockSink and choose "View Details." Here, you can run a query to see the top 20 stocks that were selected. You can see that the top 5, in order, are: BBBY, BIIB, AME, AMAT, and BMY.

The database can be queried using RESTful calls for a program which can execute the trades on the NYSE.


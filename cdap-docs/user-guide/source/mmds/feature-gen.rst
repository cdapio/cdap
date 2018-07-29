.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

:hide-toc: true

.. _user-guide-mmds-feature-gen:

==================
Feature Generation
==================

Before you can train a model, you must generate features by using the :ref:`Data Preparation Application <data-prep-user-guide-index>`.
The Data Preparation application enables you to get interactive feedback while you clean and transform your data source.

Each row is a data point, and each column is a feature.
In the example below, we parsed our input, renamed columns, set various data types,
and filtered out real estate prices that are under one million dollars.
The outcome is price, and the features are city, zip, type, beds, baths, sqft, lot, stories, and year_built.

.. figure:: /_images/mmds-feature-gen.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Feature Types
-------------

When generating features, pay close attention to the feature type. Features can be numeric or categorical.

  - String and boolean columns are treated as `categorical` features.
  - Short, integer, long, float, and double columns are treated as `numeric` features.

One common mistake is setting a categorical type on a column that should be numeric.
Categories represent distinct values, and modeling algorithms do not expect a large number of distinct values.
In the example, setting the sqft column to a string instead of a double would result in thousands of distinct values
and cause a failure during model training.

Not all numbers need to be treated as a numeric feature. In the example, the zip is left as a string,
because each zip code represents a distinct value, not a number on a sliding scale.
Additionally, zip is unlike sqft where we expect a higher number to indicate a higher price.
The zip is more like city, where we expect certain values to correlate to certain price ranges.
Therefore, setting zip as an integer is incorrect.

Outliers
--------

Generally, you should analyze your features to identify outliers to filter out or clean.
For example, if gender is a feature, you might expect only `male` or `female` values.
However, your data might contain typos like `femal` or abbreviations like `f` instead of `female`.
You will want to clean your data so that all the values are standardized to `female`.

For numeric features, you should look for outliers that could indicate dirty data.
In the earlier real estate example, you might want to filter out a data point of 100000 square feet.
This type of analysis is difficult to perform when looking at separate data points.
Finding these types of errors is easier using statistics that are calculated over the entire data source.

Aggregate statistics
--------------------

After you split the data, you can view aggregate statistics for each feature.

For categorical features, you can view a histogram of the values.
The example below shows the house type, most of which are the types `Single-Family Home` or `Condo`.
Based on this histogram, you might want to filter out the `Income/Investment` data points and replace `Townhouse` with `Condo`.

.. figure:: /_images/mmds-data-split.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

To modify your features, click the *Edit* button near the top of the screen to go back to the
Data Preparation view where you can perform further cleaning and transformation.
Once you are happy with your features, click the *Done* button at the bottom of the to proceed with model training.


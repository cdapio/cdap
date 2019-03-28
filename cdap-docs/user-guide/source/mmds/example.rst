.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

:hide-toc: true

.. _user-guide-mmds-example:

====================
Example Walk-Through
====================

This example walks you through using CDAP Sandbox to train a model that predicts the home sale prices for unsold homes on the market.

The data source for this example is 2017 real estate sales for a few cities in the San Francisco Bay Area,
which you can download `here <https://hub.cdap.io/v2/packages/datapack-realestate-sales/1.0.0/sales.tsv>`__.
Place the file on your local machine.

Creating an Experiment
----------------------

The first step is to navigate to the *Analytics* tab at the top of CDAP.

After that, you will be asked to create a new experiment.
The first step in creating an experiment is selecting your data source.
Navigate to and select the sales data that you downloaded for this example. This takes you into the Data Preparation view.

.. figure:: /_images/mmds-example-start.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

The Data Preparation view contains a sample of the data from your input file.
Start by parsing the body of our data as CSV with tab as a delimiter and the first row as a header.

.. figure:: /_images/mmds-example-parse.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Notice the immediate feedback about data changes. Delete the original body column, which is not needed.

Next, set the data types. Change the `price`, `sqft`, and `lot_sqft` columns to be of type *double*,
and the `year_built` column to be of type *int*.

.. figure:: /_images/mmds-example-change-type.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

After setting the data types, set the outcome of our new experiment to be `price`.
Then, finish creating the experiment by giving a name and description.

.. figure:: /_images/mmds-example-create-experiment.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Creating a model
----------------

Next, create a model in the experiment by providing a name and a description for the model.

.. figure:: /_images/mmds-example-model-create.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

On the next page, click the button to split the data source into training and test datasets.
The split is generated after several seconds with some computed statistics for examination.

At this point, you can examine information about each feature, as well as the outcome.
Notice that most of the prices are concentrated in a relatively small bucket with a small number of outliers that are way outside the normal range.

.. figure:: /_images/mmds-example-price-split.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Cleaning up the data
--------------------

It is a good idea to filter out the extreme outliers.
Click the *Edit* link near the top of the screen, which takes you to the Data Preparation view.

.. figure:: /_images/mmds-example-filter-price.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Add a filter on `price` to keep only the rows that have a price between one and ten million dollars, and then split the data again.
Notice the price distribution is now more sensible.

.. figure:: /_images/mmds-example-split-good.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Training a model
----------------

At this point, we could go through our features and perform more cleanup.
For the sake of brevity, we move on to train a model.

.. figure:: /_images/mmds-example-dtree-train.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

To start training the model, select the *Decision Tree Regression* algorithm and use the default parameters,
which should complete training in less than a minute and take you to the Experiment Detail view.

.. figure:: /_images/mmds-example-model-list.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-imageo

The Experiment Detail view shows general information about the experiment and lists all the models in the experiment.
You can see the evaluation metrics and explore the predictions the model made during the evaluation process.
For example, you can run a SQL query to see which cities have the worst predictions.

.. figure:: /_images/mmds-example-prediction-explore.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

At this point, you would typically explore the predictions or try training other models with different modeling algorithms
and parameters to try to generate a model with the least amount of error.

After you have created a model that you are happy with, you can create a scoring pipeline with the model in Pipeline Studio.

.. _user-guide-mmds-example-scoring-pipeline:

Making Predicitons
------------------

You will be making predictions on sample real estate listings.
Download the `listing file <https://hub.cdap.io/v2/packages/datapack-realestate-listings/1.0.0/listings.tsv>`__ and place it on your local machine.

Now click the `Creating a scoring pipeline` button to use the model we just trained to create a scoring pipeline.
This brings you to the Pipeline Studio with part of a pipeline preconfigured for you.

.. figure:: /_images/mmds-example-scoring-pipeline-start.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

For illustrative purposes, the pipeline uses the training data as a source.
The first step is to change the File source so that it reads from the listings data instead of the sales data.

.. figure:: /_images/mmds-example-scoring-pipeline-input.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

The listings data is similar to our sales data except that it does not have a field for the price.
Price is what this pipeline will be predicting. Since there is no price field,
you need to update the Wrangler plugin to remove directives that operate on price and remove price from the output schema.

.. figure:: /_images/mmds-example-scoring-pipeline-wrangler.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Next, configure the MLPredictor plugin to add a field called `predicted_price`.
Because the model is a regression model, the prediction field must be of type `double`.
When using a classification model, the prediction field would need to be of type string.

.. figure:: /_images/mmds-example-scoring-pipeline-predictor.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Lastly, you need to add a sink to write the predictions to.
On the left panel, select `Parquet Time Partitioned Dataset` and connect the predictor to the sink.
The sink needs to be configured with just one field: the dataset name. Enter `listings_price_predictions` as the dataset name.

.. figure:: /_images/mmds-example-scoring-pipeline-add-sink.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

To deploy the pipeline, click the `Deploy` button near the top right of the screen.
This brings up the pipeline detail view, where you can manually run the scoring pipeline to test that it works.

.. figure:: /_images/mmds-example-scoring-pipeline-run.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

After the pipeline successfully runs, click the sink. At the top-right of pop-up window, click the button to see the dataset detail page.

.. figure:: /_images/mmds-example-listings-predictions.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Near the top right of this page, click the eye icon to open the SQL view and explore the dataset.
From here, you can query the dataset to explore the predictions that were just made.

.. figure:: /_images/mmds-example-listings-predictions-explore.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

With minor changes, you can schedule a pipeline like this to run on a periodic interval, reading from a different directory each time.
In this way, you can use the model you just trained to make price predictions for new listings as they become available.


.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

:hide-toc: true

.. _user-guide-mmds-concepts:

========================
Concepts and Terminology
========================

This section provides an overview of the CDAP Analytics application and defines key concepts.

Experiments
-----------

CDAP Analytics organizes data into experiments. Each experiment is tied to a *data source*.
Currently, the only supported data source is a file. In CDAP Sandbox, the file is on your local filesystem.
In Distributed CDAP, the file is on the Hadoop Distributed FileSystem (HDFS).
Future versions of the application will integrate with an extended set of data sources,
such as relational databases and cloud storage systems.

In addition to a data source, each experiment specifies an *outcome*.
The outcome is the field in your data that you want to be able to predict.
An outcome is either a categorical outcome or a numeric outcome.

  - `Categorical outcomes` are discrete values: each value represents a different category.
  - `Numerical outcomes` are continuous numbers that represent data on a sliding scale.

For example, if you want to predict whether or not a new hire will leave the company within a year,
the outcome is categorical: true or false. If you want to predict how much money a house will sell for,
the outcome is numeric.

Each experiment contains zero or more models. Because the experiment outcome is fixed,
all models in an experiment will be trained to predict the same thing.

An experiment is a way to group related models together in one place and compare how each model performs.
In the earlier real estate example, all models in the experiment will predict housing prices.
In the spam example, all models in the experiment will predict whether a phone number is spam or not.

Models
------

A model is always created inside an experiment. A *model* is trained on the data source for its experiment
and predicts the outcome of the experiment. A model consists of the following components:

  - A set of features
  - An algorithm
  - Evaluation metrics
  - A predictions dataset

Features
^^^^^^^^

A *feature* is a piece of data that you think should be taken into consideration when making a prediction.

Each model is trained using data points that contain the observed outcome and one or more features.
For example, when making a prediction on a house price, the following pieces of data (features) affect what the house price should be:

  - Location
  - Size
  - Number of bedrooms
  - Number of bathrooms
  - Age of the house

Each data point must always contain the observed outcome --the price-- as well as one or more of those features.

During training, data points without a value for the outcome are filtered out. If some feature values are unknown, that’s ok.
However, the more complete the data, the better the model will be. For example, if you know only the age of half of the houses in your data,
you can still use age as a model feature. It just won't be as accurate as if you knew the age of all the houses.

After you train a model, the model can predict the outcome of a data point given one or more of the model features.
With the earlier real estate example, you can use the trained model to predict a house price as long as you provide the features
(location, size, and so on).

Algorithm
^^^^^^^^^

A *modeling algorithm* is a method of processing observed data points to predict unknown outcomes.

Depending on the outcome type, different algorithms are available to train the model.
For categorical outcomes, you can use classification algorithms.
For numeric outcomes, you can use regression algorithms.
Each algorithm has a set of parameters that you can adjust to produce a better model.
Many times, it’s trial and error that produces a good set of parameters.

Evaluation Metrics
^^^^^^^^^^^^^^^^^^

*Evaluation metrics* are used to measure how good a model is based on the prediction and observed outcome.

During the model training process, the input data is split into two datasets: a training dataset and a test dataset.
By default, the training dataset is 80% of the input data and is chosen randomly.
Any data point not in the training dataset is assigned to the test dataset.

Models are trained only on the training dataset. Models are then evaluated against the test dataset by making a prediction
for each data point in the training dataset and comparing it to the observed outcome for that data point.
Different evaluation metrics are used depending on the outcome type.

Evaluation Metrics for Categorical Outcomes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following evaluation metrics are used for categorical outcomes:

  - `Precision` measures how accurate predictions are.
  - `Recall` measures how complete predictions are.
  - `F1` is the harmonic average of precision and recall.

Higher values are better.

For example, suppose you have emails that have been labeled as spam or not spam.
90% of your data is not spam and 10% of your data is spam.
You have "trained" a model that ignores the content of the email and just predicts everything is not spam.

This model has good precision because it correctly predicts an email to be non spam 90% of the time.
However, it has poor recall because it will never correctly label spam emails.
Precision usually increases at the expense of recall and recall usually increases at the expense of precision.
People often optimize for F1 as a way to find a happy balance between precision and recall.

Evaluation Metrics for Numeric Outcomes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following evaluation metrics are used for numeric outcomes:

  - `Mean absolute error` is the average of the error between the prediction and the observed outcome.
  - `Root mean squared error` is a similar measure of error, except bad predictions are weighted more than close predictions.
  - `R2` is a measure of how closely observed outcomes fit the predictions based on variation explained by the model.
  - `Explained variance` is measure related to R2 of how much of the error between prediction and observed outcome is explained by the model

Lower values are better.

Predictions Dataset
^^^^^^^^^^^^^^^^^^^

By default, 20% of the input data source is used to evaluate the model.
The predictions generated during model evaluation are saved.
You can explore the predictions dataset by using SQL to get a better understanding of
how the model behaves and identify possible feature cleaning to improve the model.
For example, suppose you notice that house price predictions are farther off when the number of bedrooms is unknown.
You could clean up the data by finding data points with missing bedroom counts and fill in with a reasonable guess,
such as the average number of bedrooms. Then, train another model to see if it performs better.


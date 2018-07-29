.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

:hide-toc: true

.. _user-guide-mmds-modeling:

========
Modeling
========

After splitting and verifying your data, you can choose the modeling algorithm to train your model.
Depending on the type of your outcome, the set of available algorithms is different.

Algorithms
----------

For categorical outcomes, you can choose from several classification algorithms.
For numeric, you can choose from several regression algorithms.
Each algorithm has its own set of parameters that you can modify.

.. figure:: /_images/mmds-algorithm.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

If you don’t know which model to use or how to configure a model, that’s ok.
Training models requires lots of trial and error.
Even seasoned data scientists try different settings to see which settings give the best result.
Often times, the amount of data and the feature quality is more important than the parameters used to train your model.

Training
--------

After selecting an algorithm, you can start training your model in the experiment view.

The experiment view shows you how the models compare. If any model encounters a failure while training, you can view the logs.
You can also explore the predictions that each model made or evaluate the features that you used to train the model.
More information about common feature problems can be found in the section on :ref:`Feature Generation <user-guide-mmds-feature-gen>`.

.. figure:: /_images/mmds-model-list.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Scoring
-------

Once you are happy with a model, you can use the model to create a scoring pipeline in the Pipeline Studio view.

For illustrative purposes, the beginnings of a pipeline using your original data source as an input is pre-configured for you.
You should replace the source with the unlabeled data that you want to make predictions on.

You also need to configure your MLPredictor plugin by specifying a prediction field and adding the field to the output schema.
The type of prediction depends on the type of model being used.

  - For a classification model, the prediction type should be a string.
  - For a regression model, the prediction type should be a double.

In our real estate example, we would add a prediction field named `predicted_price` of type double.

Finally, you need to attach a sink where the pipeline will write the predicted data.

More information about this process can be found in the :ref:`Example Walk-Through <user-guide-mmds-example-scoring-pipeline>`.


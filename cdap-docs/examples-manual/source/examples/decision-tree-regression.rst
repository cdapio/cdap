.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform DecisionTreeRegression Application
    :copyright: Copyright © 2017 Cask Data, Inc.

.. _examples-decision-tree-regression:

========================
Decision Tree Regression
========================

A Cask Data Application Platform (CDAP) example demonstrating Spark2.


Overview
========
This example demonstrates a Spark2 application training a machine-learning model using the
`Decision Tree Regression <https://en.wikipedia.org/wiki/Decision_tree_learning>`__ method.

Labeled data in `libsvm format <http://www.csie.ntu.edu.tw/~cjlin/libsvm/>`__ is
uploaded to a CDAP Service by a RESTful call. This data is processed by the
``ModelTrainer`` Spark program, which divides the labeled data into a test set and a
training set. A model is trained using the Decision Tree Regression method, and metadata
about the model, such as the `root-mean-square error
<https://en.wikipedia.org/wiki/Root-mean-square_deviation>`__, are stored in an
ObjectMappedTable dataset.

Once the ``ModelTrainer`` program completes, you can list the models trained by
querying the ``models`` endpoint of the *ModelDataService*. You can fetch metadata about a
specific model using the ``models/{model-id}`` endpoint of the *ModelDataService*. It will
respond with metadata, such as how many data points were tested, how many data points were
correctly labeled, and the root-mean-square error.

Let's look at some of these components, and then run the application and see the results.

The *DecisionTreeRegression* Application
----------------------------------------
As in the other :ref:`examples <examples-index>`, the components of the application are
tied together by the class ``DecisionTreeRegressionApp``:

.. literalinclude:: /../../../cdap-examples/DecisionTreeRegression/src/main/java/co/cask/cdap/examples/dtree/DecisionTreeRegressionApp.java
   :language: java
   :lines: 31-55
   :append: . . .

The *trainingData* and *models* FileSet Data Storage
----------------------------------------------------
The labeled data is stored in a FileSet dataset, *trainingData*.
Trained models are stored in a FileSet dataset, *models*.

The *modelMeta* ObjectMappedTable Data Storage
----------------------------------------------
Metadata about trained models are stored in an ObjectMappedTable dataset, *modelMeta*.

The *ModelDataService* Service
------------------------------
This service has three endpoints:

- ``labels`` endpoint is used to upload labeled data for training and testing
- ``models`` endpoint is used to list the IDs of all models trained to date
- ``models/{model-id}`` endpoint is used to retrieve metadata about a specific model


.. Building and Starting
.. =====================
.. |example| replace:: DecisionTreeRegression
.. |example-italic| replace:: *DecisionTreeRegression*
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <DecisionTreeRegression>`

.. include:: _includes/_building-starting-running.txt


Running the Example
===================

Setting the Spark Version
-------------------------

This example uses **Spark2**, and the CDAP Sandbox must be configured to use the
Spark2 runtime instead of the default of *Spark1*. To do this, modify the
``conf/cdap-site.xml`` file of the CDAP Sandbox. The property
``app.program.spark.compat`` must be changed to ``spark2_2.11`` and CDAP restarted, if it
is currently running.

.. Starting the Service
.. --------------------
.. |example-service| replace:: ModelDataService
.. |example-service-italic| replace:: *ModelDataService*

.. include:: _includes/_starting-service.txt

Uploading Label Data
--------------------
Upload labeled data in ``libsvm`` format by running this command from the CDAP Local
Sandbox home directory, using the CDAP :ref:`Command Line Interface <cli>`:

.. tabbed-parsed-literal::

  $ cdap cli call service DecisionTreeRegression.ModelDataService PUT labels body:file examples/DecisionTreeRegression/src/test/resources/sample_libsvm_data.txt

Running the Spark Program
-------------------------
There are three ways to start the Spark program:

1. Go to the |example-italic| :cdap-ui-apps-programs:`application overview page, programs
   tab <DecisionTreeRegression>`, click |example-service-italic| to get to the service detail
   page, then click the *Start* button; or

#. Use the Command Line Interface:

   .. tabbed-parsed-literal::

    $ cdap cli start spark DecisionTreeRegression.ModelTrainer

#. Send a query via an HTTP request using the ``curl`` command:

   .. tabbed-parsed-literal::

    $ curl -w"\n" -X POST \
    "http://localhost:11015/v3/namespaces/default/apps/DecisionTreeRegression/spark/ModelTrainer/start"


Querying the Results
--------------------
Once the trainer has completed, you can retrieve the ID of the trained model. (If it has
not completed, the examples in this section will return no results, and can be retried
until they return results.)

To list the IDs of trained models using the ``ModelDataService``, you can:

- Use the Command Line Interface:

  .. tabbed-parsed-literal::

    $ cdap cli call service DecisionTreeRegression.ModelDataService GET models

    [ "92f9da09-71c3-45b0-aec5-2eb100cfbbac" ]

- Send a query via an HTTP request using the ``curl`` command. For example:

  .. tabbed-parsed-literal::

    $ curl -w"\n" -X GET "http://localhost:11015/v3/namespaces/default/apps/DecisionTreeRegression/services/ModelDataService/methods/models"

    [ "92f9da09-71c3-45b0-aec5-2eb100cfbbac" ]

To retreive metadata about a specific model using the ``ModelDataService``, you can:

- Use the Command Line Interface:

  .. tabbed-parsed-literal::

    $ cdap cli call service DecisionTreeRegression.ModelDataService GET models/92f9da09-71c3-45b0-aec5-2eb100cfbbac

- Send a query via an HTTP request using the ``curl`` command. For example:

  .. tabbed-parsed-literal::

    $ curl -w"\n" -X GET "http://localhost:11015/v3/namespaces/default/apps/DecisionTreeRegression/services/ModelDataService/methods/models/92f9da09-71c3-45b0-aec5-2eb100cfbbac"

    {
      "numFeatures": 692,
      "numPredictions": 37,
      "numPredictionsCorrect": 35,
      "numPredictionsWrong": 2,
      "rmse": 0.2324952774876386,
      "trainingPercentage": 0.7
    }

.. Stopping and Removing the Application
.. =====================================
.. |example-spark| replace:: ModelTrainer
.. |example-spark-italic| replace:: *ModelTrainer*
.. include:: _includes/_stopping-spark-service-removing-application.txt

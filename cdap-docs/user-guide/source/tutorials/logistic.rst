.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide: Getting Started

:hide-toc: true

===============================================================
Example: Building a Fraud Classification Machine Learning Model
===============================================================

Introduction
------------
In this tutorial, you will learn how to use CDAP to build and operationalize an enterprise-ready Machine Learning for predicting credit fraud using only CDAP’s point-and-click interfaces.

Scenario
---------
You are processing a large number of credit card transactions. Identifying which transactions are fraudulent is business critical.

- You want to train a Logistic Regression model using labeled data

- You want to be able to send events to the model in realtime, and see which are predicted to be fraudelent

Data
----
Click the link below to download a `.csv` file containing the data necessary to complete the tutorial.

:download:`fraud_train.csv </_include/tutorials/fraud_train.csv>`

Video Tutorial
--------------

..  youtube:: bt_qTj63c04

Step-by-Step Walkthrough
------------------------

Loading the Data
~~~~~~~~~~~~~~~~
Download the data linked in the Data section above. Open Data Preparation, and upload `fraud_train.csv` as a "File." This is the data we will use to train our model.

Cleaning and Exploring the Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
First, you need to parse the data:

- From the `body` column drop-down menu, select Parse > CSV, setting the first row as the header. 
- Next, from the `body` column drop-down, select Delete Column. You may also delete the `id` column, since we will not be using it in our regression. 

We have eight predictive variables, all of which have been normalized except for Amount:

- `normalized_total_spent_last_24_hours`: Total spent in the last 24 hours.
- `normalized_merchant_fraud_risk`: Merchant's track record of fraudulent transactions.
- `normalized_time_since_last_transaction`: Time since the last purchase.
- `normalized_average_transaction`: Average transaction size.
- `normalized_time_till_expiration`: Time until the card expires.
- `normalized_daily_average_transaction`: Average number of transactions per day.
- `normalized_change_in_merchant_sales`: Change over the previous day in merchant sales.
- `Amount`: Amount spent in the transaction.

The first rows of the data should like the data shown below.

.. figure:: /_images/tutorials/logistic/sample.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Overview of Model Building Procedure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The process of building the logistic regression model will be split into two phases: training and testing. In training, you will prepare the data and train the Logistic Regression plugin in a pipeline. For this portion, you will use the file "fraud_train.csv." 

For testing, you will use the trained model to predict the outcome of previously unseen transactions. 

Training: Capturing Interaction Effects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You will create a variable that combines two variables in order to capture interaction effects in the regression.

Specifically, you want to create synthetic variable called `24_hour_to_average_ratio` that the ratio of `normalized_total_spent_last_24_hours` to `normalized_average_transaction`. 

First, you need to set the type of `normalized_total_spent_last_24_hours` to Double. Currently, it is a String. To do so, enter the following directive into the prompt at the bottom of the screen:

.. figure:: /_images/tutorials/logistic/to_double.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Repeat this procedure for `normalized_average_transaction` so that its data type is Double.

Now, you can create the interaction variable. You will accomplish this by selecting the drop down menu for either column, and applying a `Custom Transform`. However, custom transforms replace the value of the current column. Therefore, we first want to copy `normalized_total_spent_last_24_hours`, rename it `twentyfour_hour_to_average_ratio`, and store the result of the column-wise division in this column.  

To copy `normalized_total_spent_last_24_hours`, choose its drop down menu and `Copy Column`. Apply. Rename the new column `twentyfour_hour_to_average_ratio`.

.. figure:: /_images/tutorials/logistic/copy.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Now, you can calculate your interaction variable. Choose the drop-down menu for `twentyfour_hour_to_average_ratio` and select `Custom Transform`. Apply the transform shown below.

.. figure:: /_images/tutorials/logistic/create_interaction.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

You will see that the results have been stored in the `twentyfour_hour_to_average_ratio` column. 

Finally, you need to set the type for all columns to Double. Use the `set-type` directive as above to accomplish this. Alternatively, you may paste in the following directives:

`set-type normalized_merchant_fraud_risk Double`
`set-type normalized_time_since_last_transaction Double`
`set-type normalized_days_till_expiration Double`
`set-type normalized_change_in_merchant_sales Double`
`set-type normalized_transaction_time Double`
`set-type Amount Double`
`set-type Class Double`

Training: Deploying the Model in a Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In the upper right hand corner, click "Create Pipeline." Choose Batch.

You will see the following configuration appear on your screen.

.. figure:: /_images/tutorials/logistic/start_pipeline.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Since you are training a Logistic Regression model, you now want to add a Logistic Regression plugin to the canvas. You will need to download this plugin, which is available on the Cask Market. 

Choose "Cask Market" in the global navigation bar. Find "Logistic Regression Analytics" in the application menu and deploy the plugin.

Now, save the pipeline as "LogisticRegressionTrainer".

.. figure:: /_images/tutorials/logistic/save_trainer.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Refresh the page, and under the "Sink" tab in the left side menu, you will find the "Logistic Regression Trainer". Add this plugin the canvas, and connect it to the "Wrangler" stage. You will see the configuration below:

.. figure:: /_images/tutorials/logistic/first_version_trainer.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Looking at the canvas, you will see that several stages have small yellow numbers displayed in the upper right hand corner. These values tell you how many fields need to be configured before the pipeline can be deployed and run.

Open the `File` stage and rename it (by changing the label and reference name) to "CreditCardSource." Similarly, rename "Wrangler" to "CreditTransform" and "LogisticRegressionTrainer" to "FraudTrainerSink." 

In the Trainer configuration, specify `Label Field` to be Class. This is the field that your Logistic Regression model will predict. Also, give `FileSet Name` the value "FraudModel," which is the fileset in which the trained model will be stored.

.. figure:: /_images/tutorials/logistic/trainer_config.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Your configured pipeline will appear as the pipeline below.

.. figure:: /_images/tutorials/logistic/configured.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

To test your pipeline configuration, click `Preview` in the upper right hander corner, then `Run`. Once the preview is complete you can open the LogisticRegressionTrainer and see the records that have flown through in the `Preview` tab.

.. figure:: /_images/tutorials/logistic/verify.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

You can now click "Deploy" and run the pipeline.

Testing: Evaluating Unknown Events
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Return to Data Preparation, where you were working with the "fraud_train.csv" file. Click "Create Pipeline" again.

Delete the File node. In its place, connect a "CDAP Stream" source to the "Wrangler" stage. You repeated this original procedure because the Wrangler stage has all the same transformations you want to apply to your test data.

.. figure:: /_images/tutorials/logistic/delete.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Name the "CDAP Stream" to "FraudStream" and specify the duration to be 10m.

Add a "LogisticRegressionClassifier" to the canvas, followed by a "Avro Time Partitioned Dataset" sink. Rename "Wrangler" to "CreditTransform", "LogisiticRegressionClassifier" to "FraudClassifier", and "Avro Time Partitioned Dataset" to "FraudSink."

In the "FraudClassifier" stage, set the `FileSet Name` to "LogisticRegressionModel" and `Prediction Field` to "Class".

.. figure:: /_images/tutorials/logistic/classifier_config.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

In the "FraudSink" stage, specify the `Dataset Name` to be "FraudSink".

You can now name your pipeline "FraudClassifier" and deploy it.

.. figure:: /_images/tutorials/logistic/deployed_pipe.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Open the stream's properties, and click "View Details." Click the upload arrow and send paste the events below into the text box that appears on screen. This will send the events to the newly created stream.

`9253,-6.18585747766,7.10298492227,-13.0304552639,8.01082339822,-7.88523736132,-3.9745499936,-12.2296077787,44.9`
`8175,-0.734303155038,0.435519332206,-0.530865546585,-0.47111960693,0.643214386605,0.713832369658,-1.23457207133,29.95`
`6015,-2.29698749922,4.0640433419,-5.95770634517,4.68000806392,-2.0809375996,-1.46327216002,-4.49084686932,104.0`
`434,-1.70678465109,0.291716504769,1.7070939159,0.551977355909,0.234141485112,-0.265204391462,0.0453993133097,38.19`
`3174,1.20124498017,-0.599557518154,0.604505150814,-0.807313717744,-0.43403239775,0.823049573638,-0.858524252306,6.41`

These events should be pasted into this box:

.. figure:: /_images/tutorials/logistic/stream.jpeg
	:figwidth: 100%
	:width: 500px
	:align: center
	:class: bordered-image

Return your pipeline and click "Run". Finally, you can inspect the "FraudSink" classification results by clicking "View Details" from the configuration and querying the database. Looking at the `Class` field, you will see that two the these events have a Class of "1", indicating that they were predicted to be fraudulent.

.. figure:: /_images/tutorials/logistic/fraud.jpeg
	:figwidth: 100%
	:width: 800px
	:align: center
	:class: bordered-image


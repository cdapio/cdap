===========================================================
Controlling the Lifecyle of a Reactor Application
===========================================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. rst2pdf: CutStart
.. Slide Presentation HTML Generation
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/

.. include:: ../_slide-fragments/continuuity_logo_copyright.rst

.. |br| raw:: html

   <br />
.. rst2pdf: CutStop

.. rst2pdf: config ../../../developer-guide/source/_templates/pdf-config
.. rst2pdf: stylesheets ../../../developer-guide/source/_templates/pdf-stylesheet
.. rst2pdf: build ../../pdf/
.. rst2pdf: .. |br|  unicode:: U+0020 .. space

----

Exercise Objectives
====================

In this exercise, you will:

- Control the example application's lifecycle
- Start and stop Flows and Procedures
- Deploy the example application
- Truncate an existing DataSet

----

Exercise Steps (1 of 2)
=======================

Stop the ``analysis`` Flow (or another Procedure, MapReduce job) associated with the application::

	curl -o /dev/null -sL -w "%{http_code}\\n" -X POST 
	  http://localhost:10000/v2/apps/SentimentAnalysisApp/flows/analysis/stop

Using a similar ``curl`` command, stop the ``sentiment-query`` procedure

Now deploy the app using a ``curl`` command, executed from the directory where you are building the application::

	curl -o /dev/null -sL -w "%{http_code}\\n" -H "X-Archive-Name: SentimentAnalysisApp" 
	  -X POST http://localhost:10000/v2/apps 
	  --data-binary @target/SentimentAnalysis-1.0-SNAPSHOT.jar

----

Exercise Steps (2 of 2)
=======================

Re-use the previous commands to start the ``analysis`` Flow and |br|
``sentiment-query`` Procedure

Use a DataSet command to truncate the two tables in the example; the command is of the form::

	POST <base-url>/datasets/<dataset-name>/truncate

----

Exercise Summary
===================

You should now be able to:

- Control an application's lifecycle using external commands
- Start and stop Flows and Procedures
- Deploy applications
- Truncate an existing DataSet

----

Exercise Completed
==================

`Chapter Index <return.html#e12>`__


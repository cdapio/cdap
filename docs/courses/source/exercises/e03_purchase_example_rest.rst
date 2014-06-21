===========================================================
Continuuity Reactor Purchase Example Using REST
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

- Run the Continuuity Reactor *Purchase* Example, using shell scripts
- Run an application that uses scheduled MapReduce Workflows to read from one 
  ``ObjectStore`` DataSet and write to another
- Use shell commands to install Applications and interact with Reactor

----

Purchase Example and Steps
==========================

- Install the Purchase Application using the shell scripts in the example in a running Reactor
- Send sentences of the form "Tom bought 5 apples for $10" to the purchaseStream using ``curl``
- The PurchaseFlow reads the purchaseStream and converts every input String into a
  Purchase object and stores the object in the purchases DataSet
- When scheduled by the PurchaseHistoryWorkflow, the PurchaseHistoryBuilder MapReduce Job
  reads the purchases DataSet, creates a purchase history,
  and stores the purchase history in the history DataSet every morning at 4:00 A.M.
- Manually (in the Process screen in the Reactor Dashboard) execute 
  the PurchaseHistoryBuilder MapReduce job to store customers' purchase history
  in the history DataSet.
- Execute the PurchaseQuery procedure using ``curl`` to query the history DataSet and discover
  the purchase history of each customer

----

Purchase Example Hints
==========================

The Purchase Application is located in ``/examples/Purchase``

Shell script for installing it is located in ``/examples/Purchase/bin``

Send sentences using

.. sourcecode:: shell-session

	curl -X POST -d "Tom bought 5 apples for $10" \
	  http://localhost:10000/v2/streams/purchaseStream

Execute the PurchaseQuery procedure using

.. sourcecode:: shell-session

	curl -v -d '{"customer": "Tom"}' -X POST \
	  '<base-url>/PurchaseHistory/procedures/PurchaseQuery/methods/history'

where ``<base-url>`` is

.. sourcecode:: shell-session

	http://localhost:10000/v2/apps

----

Exercise Summary
===================

You are now able to:

- Install pre-compiled Applications using a shell script
- Interact with the Reactor with curl
- Send events to a stream
- Receiving data from queries

----

Exercise Completed
==================

`Chapter Index <return.html#e03>`__


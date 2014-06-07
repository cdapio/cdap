===========================================================
Continuuity Reactor Purchase Example Using Dashboard
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

- Run the Continuuity Reactor *Purchase* Example, using drag 'n drop
- This application uses scheduled MapReduce Workflows to read from one 
  ObjectStore DataSet and write to another
- Learn how to use the Dashboard to enter events to a Stream
- Learn how to use the Dashboard to run MapReduce Workflows
- Learn how to use the Dashboard to run Procedures and query results

----

Purchase Example and Steps
==========================

- Install the Purchase Application by dragging the application jar to the Dashboard of a running Reactor
- Send sentences of the form "Tom bought 5 apples for $10" to the purchaseStream using
  the Dashboard
- The PurchaseFlow reads the purchaseStream and converts every input String into a
  Purchase object and stores the object in the purchases DataSet
- When scheduled by the PurchaseHistoryWorkflow, the PurchaseHistoryBuilder MapReduce Job
  reads the purchases DataSet, creates a purchase history,
  and stores the purchase history in the history DataSet every morning at 4:00 A.M.
- Manually (in the Process screen in the Reactor Dashboard) execute 
  the PurchaseHistoryBuilder MapReduce job to store customers' purchase history
  in the history DataSet.
- Execute the PurchaseQuery procedure to query the history DataSet and discover
  the purchase history of each customer

----

Purchase Example Hints
==========================

- The Purchase Application is located in ``/examples/Purchase``

- The method is ``history`` ; the parameters are entered as a JSON string such as
  ``{ "customers" : "Tom" }``

-----

Exercise Summary
===================

You are now able to:

- Install pre-compiled Applications
- Use the Dashboard to enter events to a Stream
- Use the Dashboard to run MapReduce Workflows
- Use the Dashboard to query Reactor using a Procedure

----

Exercise Completed
==================

`Chapter Index <return.html#e02>`__


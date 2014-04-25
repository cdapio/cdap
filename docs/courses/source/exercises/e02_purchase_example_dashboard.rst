===========================================================
Continuuity Reactor Purchase Example using Dashboard
===========================================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. Slide Presentation HTML Generation
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/

.. include:: ../_slide-fragments/continuuity_logo.rst

.. |br_e02| raw:: html

   <br />

.. |br2_e02| raw:: html

   <br /><br />

----

Exercise Objectives
====================

In this exercise, you will:

- Run the Continuuity Reactor *Purchase* Example
- An application that uses scheduled MapReduce Workflows to read from one 
  ObjectStore DataSet and write to another
- ??

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

- The method is ``history`` ; the parameters are entered as a JSON string such as
  ``{ "customers" : "Tom" }``

-----

Exercise Summary
===================

You are now able to:

- Install pre-compiled Applications
- Use the Dashboard to...
- ?

----

Exercise Completed
==================

`Chapter Index <return.html#e02>`__


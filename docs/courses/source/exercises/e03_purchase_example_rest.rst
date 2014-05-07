===========================================================
Continuuity Reactor Purchase Example using REST
===========================================================

.. .. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. Slide Presentation HTML Generation
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/

.. include:: ../_slide-fragments/continuuity_logo.rst

.. |br_e03| raw:: html

   <br />

.. |br2_e03| raw:: html

   <br /><br />

----

Exercise Objectives
====================

In this exercise, you will:

- Run the Continuuity Reactor *Purchase* Example
- An application that uses scheduled MapReduce Workflows to read from one 
  ObjectStore DataSet and write to another
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

Send sentences using

.. sourcecode:: shell-session

	curl -X POST -d "Tom bought 5 apples for $10" \
	  http://localhost:10000/v2/streams/purchaseStream

Execute the PurchaseQuery procedure using

.. sourcecode:: shell-session

	curl -v -d '{"customer": "Tom"}' -X POST \
	  'http://localhost:10000/v2/apps/PurchaseHistory/procedures/PurchaseQuery/methods/history'

----

Exercise Summary
===================

You are now able to:

- Install pre-compiled Applications using shell commands
- Interact with the Reactor with shell commands, both sending and receiving

----

Exercise Completed
==================

`Chapter Index <return.html#e03>`__


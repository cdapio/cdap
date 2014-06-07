============================================
Procedure REST API
============================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. rst2pdf: CutStart
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

Module Objectives
=================

In this module, you will cover:

- The Procedure REST API
- Calling Procedures from external systems
- Interpreting the results

----

Procedure REST API
==================

Used to send queries to and receive results from the methods of an Application’s Procedures


To call a method in an Application's Procedure:

- Send the method name as part of the request URL
- Send the arguments as a JSON string in the body of the request

----

Executing Procedures
====================

The request is an HTTP POST:

``POST <base-url>/apps/<app-id>/procedures/`` |br| ``   <procedure-id>/methods/<method-id>``

Parameter
  ``<app-id>``
Description
  Name of the Application being called

Parameter
  ``<procedure-id>``
Description
  Name of the Procedure being called

Parameter
  ``<method-id>``
Description
  Name of the method being called

HTTP Responses

Status Code : Description
  ``200 OK`` : The event successfully called the method, and the body contains the results |br|
  ``400 Bad Request`` : The Application, Procedure and method exist, but the arguments are not as expected |br|
  ``404 Not Found`` : The Application, Procedure, or method does not exist

----

Procedure Example
=================

``POST <base-url>/apps/WordCount/procedures/`` |br| ``   RetrieveCounts/methods/getCount``

Calls the ``getCount()`` method of the *RetrieveCounts* Procedure in the *WordCount* Application
with the arguments as a JSON string in the body:

.. sourcecode:: json

       {"word":"a"}

The response will have the results—if any—in the body

----

Module Summary
==============

You should now be able to:

- Call Procedures from external systems
- Interpret results from Procedure calls

----

Module Completed
================

`Chapter Index <return.html#m21>`__
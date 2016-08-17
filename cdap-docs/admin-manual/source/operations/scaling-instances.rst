.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _scaling-instances:

=================
Scaling Instances
=================

You can scale CDAP components (instances of flowlets, services, and workers) using:

- the :ref:`Scaling <http-restful-api-lifecycle-scale>` methods of the 
  :ref:`Lifecycle HTTP RESTful API <http-restful-api-lifecycle>`;
- the :ref:`Get/Set Commands <cli-available-commands>` of the 
  :ref:`Command Line Interface <cli>`; or
- the :ref:`ProgramClient API <program-client>` of the 
  :ref:`Java Client API <java-client-api>`.

The examples given below use the :ref:`Lifecycle HTTP RESTful API
<http-restful-api-lifecycle-scale>`.

.. include:: /../../reference-manual/source/http-restful-api/lifecycle.rst
    :start-after: .. _rest-scaling-flowlets:
    :end-before: .. _rest-program-runs:

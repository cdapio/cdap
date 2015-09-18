.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. :hide-toc: true

.. _apptemplates-etl-templates:

========================
ETL Template and Plugins 
========================

.. rubric:: Introduction

Details of templates and the required properties for sources, transformations, and sinks
can be explored using RESTful APIs.

If you are creating a custom plugin to add to either the existing templates or your own
template, its name needs to not collide with existing names.

Shipped with CDAP as part of the *ETL Batch* and *ETL Realtime* application templates, the
plugins listed below are available for creating ETL adapters.


.. toctree::
   :maxdepth: 3
   
    Sources <sources/index>
    Transformations <transformations/index>
    Validators <validators/index.rst>
    Sinks <sinks/index>
    
.. rubric:: Exploring Application Template Details

Details on the available application templates can be obtained using the
:ref:`Application Template and Adapters HTTP RESTful API <http-restful-api-artifact>`.

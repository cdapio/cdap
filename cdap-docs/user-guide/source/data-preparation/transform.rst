.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-transform:

==================
Wrangler Transform
==================

This plugin applies data transformation directives on your data to transform the records.
The directives are generated either by an interactive user interface or manual entered
into the plugin.

Plugin Configuration
====================
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Configuration
     - Required
     - Default
     - Description
   * - **Name of the field**
     - *No*
     - ``*``
     - Specifies the name of the input field to be used to wrangle or transform. If the
       value is ``*``, then all the fields are available for wrangling.
   * - **Precondition Filter**
     - *No*
     - ``false``
     - A precondition filter that is applied before a record is wrangled. For example, use to remove a header record.
   * - **Transformation Directives**
     - *Yes*
     - n/a
     - Specifies a series of wrangling directives that are applied on the input record.
   * - **Max Error Events**
     - *No*
     - ``5``
     - Maximum number of errors tolerated before failing the processing of the pipeline.

Directives
===========
There are numerous directives and variations supported by the system, as documented under
:ref:`Directives <directives>`.

Usage Notes
===========
- The input record fields are made available to the wrangling directives when ``*`` is used as
  the field to be wrangled and they are in the record in the same order as they appear.

- If wrangling doesn't operate on all the input record fields or if a field is not
  configured as part of the output schema and you are using the SET COLUMNS directive, you may
  see inconsistent behavior. To avoid this, use the DROP directive to drop the fields that are
  not used in the wrangler.

- A precondition filter is useful when you want to apply filtering on a record before the
  records are delivered for wrangling. To filter a record, specify a condition that will
  result in the boolean state of ``true``. For example, to filter out all records that are
  header records from CSV files and the header record is at the start of the file (first
  record), use this filter::

    offset == 0

  This will filter out records that have an ``offset`` of zero.

- This plugin uses the ``emitError`` capability to emit records that fail parsing into a
  separate error stream, allowing the aggregation of all wrangling errors. Note that if
  ``Max Error Events`` is set, then the pipeline will fail when that error threshold is
  reached.

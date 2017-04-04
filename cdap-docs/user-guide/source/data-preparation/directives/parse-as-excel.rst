.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

==============
Parse as Excel
==============

The PARSE-AS-EXCEL is a directive for parsing excel file - XLS, XLSX.

Syntax
------

::

    parse-as-excel <column> <sheet number> | <sheet name>

The ``<column>`` specifies the column in the record that contains excel.
The ``<sheet number>`` or ``<sheet name>`` specifies the sheet within
the excel file that needs to be parsed.

.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

========================
Sources: Batch: Database 
========================

.. rubric:: Description: Batch source for a Database

**Query:** The SELECT query to use to import data from the specified table. You can
specify an arbitrary number of columns to import, or import all columns using *. You can
also specify a number of WHERE clauses or ORDER BY clauses. However, LIMIT and OFFSET
clauses should not be used in this query.
    
**Count Query:** The SELECT query to use to get the count of records to import from the
specified table. Examples::

  SELECT COUNT(*) from <my_table> where <my_column> 1
  SELECT COUNT(my_column) from my_table

*Note:* Please include the same WHERE clauses in this query as the ones used in the import
query to reflect an accurate number of records to import.

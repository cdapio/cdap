.. :Author: Continuuity, Inc.
   :Description: Codename Tengo

=====================================
Codename Tengo
=====================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. rst2pdf: .. contents::
.. rst2pdf: config _templates/pdf-config
.. rst2pdf: stylesheets _templates/pdf-stylesheet
.. rst2pdf: build ../build-pdf/

Installation
==================
Note about maven dependency to add?

How to start
==================
TEST

Client APIs
==================
``TransactionAwareHTable`` implements ``HTableInterface``, thus providing the same APIs that a standard ``HTable``
provides. Only certain operations are supported transactionally. They are:

==============================================================
   Methods
==============================================================
exists(Get get)
exists(List<Get> gets)
get(Get get)
get(List<Get> gets)
getScanner(byte[] family)
getScanner(byte[] family, byte[] qualifier)
put(Put put)
put(List<Put> puts)
delete(Delete delete)
delete(List<Delete> deletes)
==============================================================

==========================================
   Methods
==========================================
getRowOrBefore(byte[] row, byte[], family)
checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)
mutateRow(RowMutations rm)
append(Append append)
increment(Increment increment)
incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)
incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
==========================================

Usage
==================
To use a ``TransactionalAwareHTable``



Example
==================
Note about constructor : Want a better/cleaner way to instantiate an instance of secondary table.
.. :Author: Continuuity, Inc.
   :Description: Codename Tengo

==============
Codename Tengo
==============

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. rst2pdf: .. contents::
.. rst2pdf: config _templates/pdf-config
.. rst2pdf: stylesheets _templates/pdf-stylesheet
.. rst2pdf: build ../build-pdf/

Introduction
============
Codename Tengo provides transactional operations on HBase. That is, operations may be committed as part of a single
transaction and upon a failure every operation in the transaction the data is rolled back to the state that existed
prior to the beginning of the transaction. 

Installation
============
// TODO : Note about maven dependency to add?

Client APIs
===========
``TransactionAwareHTable`` implements ``HTableInterface``, thus providing the same APIs that a standard ``HTable``
provides. Only certain operations are supported transactionally. They are:

.. csv-table::
  :header: Methods
  :widths: 100
  :delim: 0x9

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

Other operations are not supported transactionally and would throw an ``UnsupportedOperationException`` if invoked.
To allow these non-transactional operations, call ``setAllowNonTransactional(true)``. This allows you to use
the following methods non-transactionally:

.. csv-table::
  :header: Methods
  :widths: 100
  :delim: 0x9

    getRowOrBefore(byte[] row, byte[], family)
    checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
    checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)
    mutateRow(RowMutations rm)
    append(Append append)
    increment(Increment increment)
    incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
    incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)
    incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)

Usage
=====
To use a ``TransactionalAwareHTable``, you need an instance of ``TransactionContext`` that will automatically
invoke ``rollback`` on failed transactions. ::

  TransactionContext context = new TransactionContext(client, transactionAwareHTable);
  try {
    context.start();
    transactionAwareHTable.put(new Put(Bytes.toBytes("row"));
    // ...
    context.finish();
  } catch (TransactionFailureException e) {
    context.abort();
  }

Example
=======
To demonstrate how you might use ``TransactionAwareHTable``\s, below is a basic implementation of a
``SecondaryIndexTable``. This class encapsulates the usage of a ``TransactionContext`` and provides a simple interface
to a user.

// TODO : Add example here after review.
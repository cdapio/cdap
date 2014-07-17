.. :Author: Continuuity, Inc.
   :Description: Tephra

==============
Tephra
==============

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. rst2pdf: .. contents::
.. rst2pdf: config _templates/pdf-config
.. rst2pdf: stylesheets _templates/pdf-stylesheet
.. rst2pdf: build ../build-pdf/

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

    batch(List<? extends Row> actions, Object[] results)
    batch(List<? extends Row> actions)
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
to a user. ::

  /**
   * A Transactional SecondaryIndexTable.
   */
  public class SecondaryIndexTable {
    private byte[] secondaryIndex;
    private byte[] secondaryIndexFamily;
    private TransactionAwareHTable transactionAwareHTable;
    private TransactionAwareHTable secondaryIndexTable;
    private TransactionContext transactionContext;
    private static final byte[] DELIMITER  = new byte[] {0};

    public SecondaryIndexTable(TransactionServiceClient transactionServiceClient, HTable hTable,
                               HTable secondaryTable, byte[] secondaryIndex) {
      this.secondaryIndex = secondaryIndex;
      this.secondaryIndexFamily = Bytes.toBytes("indexFamily");
      this.transactionAwareHTable = new TransactionAwareHTable(hTable);
      this.secondaryIndexTable = new TransactionAwareHTable(secondaryTable);
      this.transactionContext = new TransactionContext(transactionServiceClient, transactionAwareHTable,
                                                  secondaryIndexTable);
    }

    public Result get(Get get) throws IOException {
      return get(Collections.singletonList(get))[0];
    }

    public Result[] get(List<Get> gets) throws IOException {
      try {
        transactionContext.start();
        Result[] result = transactionAwareHTable.get(gets);
        transactionContext.finish();
        return result;
      } catch (Exception e) {
        try {
          transactionContext.abort();
        } catch (TransactionFailureException e1) {
          throw new IOException("Could not rollback transaction", e1);
        }
      }
      return null;
    }

    public Result[] getByIndex(byte[] value) throws IOException {
      try {
        transactionContext.start();
        Scan scan = new Scan(value, Bytes.add(value, new byte[0]));
        ResultScanner indexScanner = secondaryIndexTable.getScanner(scan);

        ArrayList<Get> gets = new ArrayList<Get>();
        for (Result result : indexScanner) {
          for (Cell cell : result.listCells()) {
            gets.add(new Get(cell.getValue()));
          }
        }
        Result[] results = transactionAwareHTable.get(gets);
        transactionContext.finish();
        return results;
      } catch (Exception e) {
        try {
          transactionContext.abort();
        } catch (TransactionFailureException e1) {
          throw new IOException("Could not rollback transaction", e1);
        }
      }
      return null;
    }

    public void put(Put put) throws IOException {
      put(Collections.singletonList(put));
    }


    public void put(List<Put> puts) throws IOException {
      try {
        transactionContext.start();
        ArrayList<Put> secondaryIndexPuts = new ArrayList<Put>();
        for (Put put : puts) {
          Put secondaryIndexPut = new Put(put.getRow());
          Set<Map.Entry<byte[], List<KeyValue>>> familyMap = put.getFamilyMap().entrySet();
          for (Map.Entry<byte [], List<KeyValue>> family : familyMap) {
            for (KeyValue value : family.getValue()) {
              if (value.getQualifier().equals(secondaryIndex)) {
                byte[] secondaryQualifier = Bytes.add(value.getQualifier(), DELIMITER,
                                                      Bytes.add(value.getValue(), DELIMITER,
                                                                value.getRow()));
                secondaryIndexPut.add(secondaryIndexFamily, secondaryQualifier, value.getValue());
              }
            }
          }
          secondaryIndexPuts.add(secondaryIndexPut);
        }
        transactionAwareHTable.put(puts);
        secondaryIndexTable.put(secondaryIndexPuts);
        transactionContext.finish();
      } catch (Exception e) {
        try {
          transactionContext.abort();
        } catch (TransactionFailureException e1) {
          throw new IOException("Could not rollback transaction", e1);
        }
      }
    }
  }

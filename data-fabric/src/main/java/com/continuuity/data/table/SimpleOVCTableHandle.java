package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Defines common methods for Simple OVC table handle.
 */
public abstract class SimpleOVCTableHandle extends AbstractOVCTableHandle {

  protected final ConcurrentSkipListMap<byte[], OrderedVersionedColumnarTable>
      openTables =
        new ConcurrentSkipListMap<byte[], OrderedVersionedColumnarTable>(
            Bytes.BYTES_COMPARATOR);

  @Override
  public OrderedVersionedColumnarTable getTable(byte[] tableName)
      throws OperationException {
    OrderedVersionedColumnarTable table = this.openTables.get(tableName);

    // we currently have an open table for this name
    if (table != null) {
      return table;
    }

    // the table is not open, but it may exist in the data fabric
    table = openTable(tableName);

    // table could not be opened, try to create it
    if (table == null) {
      table = createNewTable(tableName);
    }

    // some other thread may have created/found and added it already
    OrderedVersionedColumnarTable existing =
        this.openTables.putIfAbsent(tableName, table);

    return existing != null ? existing : table;
  }

  /**
   * attempts to create the table. If the table already exists, attempt
   * to open it instead.
   * @param tableName the name of the table
   * @return the new table
   * @throws OperationException if an operation fails
   */
  protected abstract OrderedVersionedColumnarTable createNewTable(
      byte [] tableName) throws OperationException;

  /**
   * attempts to open an existing table.
   * @param tableName the name of the table
   * @return the table if successful, null otherwise
   * @throws OperationException
   */
  protected abstract OrderedVersionedColumnarTable openTable(
      byte [] tableName) throws OperationException;

}

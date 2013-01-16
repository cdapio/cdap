package com.continuuity.data.dataset;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.StatusCode;

import java.util.Arrays;
import java.util.Map;

public class IndexedTable extends DataSet {

  private Table table, index;
  private byte[] column;

  public IndexedTable(String name) {
    super(name);
    this.table = new Table("t_" + name);
    this.index = new Table("i_" + name);
  }

  public IndexedTable(String name, byte[] columnToIndex) {
    super(name);
    this.column = columnToIndex;
    this.table = new Table("t_" + name);
    this.index = new Table("i_" + name);
  }

  @Override
  void initialize(DataSetMeta meta) throws OperationException {
    this.column = meta.getProperty("column").getBytes();
    this.table.initialize(meta.getMetaFor(this.table));
    this.index.initialize(meta.getMetaFor(this.index));
  }

  @Override
  DataSetMeta.Builder configure() {
    return new DataSetMeta.Builder(this).
        property("column", new String(this.column)).
        dataset(this.table.configure()).
        dataset(this.index.configure());
  }

  static final byte[] EXISTS = { 'x' };

  public OperationResult<Map<byte[], byte[]>> read(Table.Read read)
      throws OperationException {
    return this.table.read(read);
  }

  public OperationResult<Map<byte[], byte[]>> readBy(Table.Read read)
      throws OperationException {
    // read the entire row of the index for the given key
    Table.Read idxRead = new Table.Read(read.row, null, null);
    OperationResult<Map<byte[], byte[]>> result = this.index.read(idxRead);

    // if the index has no match, return nothing
    if (result.isEmpty()) {
      return result;
    }
    // iterate over all columns in the index result:
    // each of them represents a row key in the main table
    for (byte[] column : result.getValue().keySet()) {
      if (Arrays.equals(EXISTS, result.getValue().get(column))) {
        // construct a new read with this column as the row key
        Table.Read tableRead = read.columns == null
            ? new Table.Read(column, read.startCol, read.stopCol)
            : new Table.Read(column, read.columns);
        // issue that read against the main table
        OperationResult<Map<byte[], byte[]>> tableResult =
            this.table.read(tableRead);
        // if this yields something, return it
        if (!tableResult.isEmpty()) {
          return tableResult;
        }
      }
    }
    // nothing found - return empty result
    return new OperationResult<Map<byte[], byte[]>>(StatusCode.ENTRY_NOT_FOUND);
  }

  public void write(Table.Write write) throws OperationException {
    // first read the existing row to find its current value of the index col
    Table.Read firstRead = new Table.Read(write.row, this.column);
    OperationResult<Map<byte[], byte[]>> firstResult =
        this.table.read(firstRead);
    byte[] oldSecondaryKey = null;
    if (!firstResult.isEmpty()) {
      oldSecondaryKey = firstResult.getValue().get(this.column);
    }

    // find out whether the write contains a new value for the index column
    byte[] newSecondaryKey = null;
    for (int i = 0; i < write.columns.length; i++) {
      if (Arrays.equals(this.column, write.columns[i])) {
        newSecondaryKey = write.values[i];
        break;
      }
    }

    boolean keyMatches = Arrays.equals(oldSecondaryKey, newSecondaryKey);
    // if there is an existing row with a value for the index column,
    // and that value is different from the new value to be written,
    // then we must remove that the row key from the index for that value;
    Table.Delete idxDelete = null;
    if (oldSecondaryKey != null && !keyMatches) {
      idxDelete = new Table.Delete(oldSecondaryKey, write.row);
    }

    // and we only need to write to index if the row is new or gets a new
    // value for the index column;
    Table.Write idxWrite = null;
    if (newSecondaryKey != null && !keyMatches) {
      idxWrite = new Table.Write(newSecondaryKey, write.row, EXISTS);
    }

    // stage all operations against both tables
    this.table.stage(write);
    if (idxDelete != null) {
      this.index.stage(idxDelete);
    }
    if (idxWrite != null) {
      this.index.stage(idxWrite);
    }
  }

  public void delete(Table.Delete delete) throws OperationException {
    // first read the existing row to find its current value of the index col
    Table.Read firstRead = new Table.Read(delete.row, this.column);
    OperationResult<Map<byte[], byte[]>> firstResult =
        this.table.read(firstRead);
    byte[] oldSecondaryKey = null;
    if (!firstResult.isEmpty()) {
      oldSecondaryKey = firstResult.getValue().get(this.column);
    }

    // if there is an existing row with a value for the index column,
    // then we must remove that the row key from the index for that value;
    Table.Delete idxDelete = null;
    if (oldSecondaryKey != null) {
      idxDelete = new Table.Delete(oldSecondaryKey, delete.row);
    }

    // stage all operations against both tables
    this.table.stage(delete);
    if (idxDelete != null) {
      this.index.stage(idxDelete);
    }
  }

  public void swap(Table.Swap swap) throws OperationException {
    // if the swap is on a column other than the column key, then
    // the index is not affected - just execute the swap.
    // also, if the swap is on the index column, but the old value
    // is the same as the new value, then the index is not affected either.
    if (!Arrays.equals(this.column, swap.column) ||
        Arrays.equals(swap.expected, swap.value)) {
      this.table.stage(swap);
      return;
    }

    // the swap is on the index column. it will only succeed if the current
    // value matches the expected value of the swap. if that value is not null,
    // then we must remove the row key from the index for that value.
    Table.Delete idxDelete = null;
    if (swap.expected != null) {
      idxDelete = new Table.Delete(swap.expected, swap.row);
    }

    // if the new value is not null, then we must add the rowkey to the index
    // for that value.
    Table.Write idxWrite = null;
    if (swap.value != null) {
      idxWrite = new Table.Write(swap.value, swap.row, EXISTS);
    }

    // stage all operations against both tables
    this.table.stage(swap);
    if (idxDelete != null) {
      this.index.stage(idxDelete);
    }
    if (idxWrite != null) {
      this.index.stage(idxWrite);
    }
  }

}

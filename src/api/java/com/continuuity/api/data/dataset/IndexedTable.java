package com.continuuity.api.data.dataset;

import com.continuuity.api.data.*;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.*;
import com.continuuity.api.data.dataset.table.Write;

import java.util.Arrays;
import java.util.Map;


public class IndexedTable extends DataSet {

  private String tableName, indexName;
  private Table table, index;
  private byte[] column;

  private void init(String name, byte[] column) {
    this.tableName = "t_" + name;
    this.indexName = "i_" + name;
    this.column = column;
  }

  @SuppressWarnings("unused")
  public IndexedTable(DataSetSpecification spec) throws OperationException {
    super(spec);
    this.init(this.getName(), spec.getProperty("column").getBytes());
    this.table = new Table(spec.getSpecificationFor(this.tableName));
    this.index = new Table(spec.getSpecificationFor(this.indexName));
  }

  public IndexedTable(String name, byte[] columnToIndex) {
    super(name);
    this.init(name, columnToIndex);
    this.table = new Table(this.tableName);
    this.index = new Table(this.indexName);
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this).
        property("column", new String(this.column)).
        dataset(this.table.configure()).
        dataset(this.index.configure()).
        create();
  }

  static final byte[] EXISTS = { 'x' };

  public OperationResult<Map<byte[], byte[]>> read(Read read)
      throws OperationException {
    return this.table.read(read);
  }

  public OperationResult<Map<byte[], byte[]>> readBy(Read read)
      throws OperationException {
    // read the entire row of the index for the given key
    Read idxRead = new Read(read.getRow(), null, null);
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
        Read tableRead = read.getColumns() == null
            ? new Read(column, read.getStartCol(), read.getStopCol())
            : new Read(column, read.getColumns());
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

  public void write(Write write) throws OperationException {
    // first read the existing row to find its current value of the index col
    Read firstRead = new Read(write.getRow(), this.column);
    OperationResult<Map<byte[], byte[]>> firstResult =
        this.table.read(firstRead);
    byte[] oldSecondaryKey = null;
    if (!firstResult.isEmpty()) {
      oldSecondaryKey = firstResult.getValue().get(this.column);
    }

    // find out whether the write contains a new value for the index column
    byte[] newSecondaryKey = null;
    for (int i = 0; i < write.getColumns().length; i++) {
      if (Arrays.equals(this.column, write.getColumns()[i])) {
        newSecondaryKey = write.getValues()[i];
        break;
      }
    }

    boolean keyMatches = Arrays.equals(oldSecondaryKey, newSecondaryKey);
    // if there is an existing row with a value for the index column,
    // and that value is different from the new value to be written,
    // then we must remove that the row key from the index for that value;
    Delete idxDelete = null;
    if (oldSecondaryKey != null && !keyMatches) {
      idxDelete = new Delete(oldSecondaryKey, write.getRow());
    }

    // and we only need to write to index if the row is new or gets a new
    // value for the index column;
    Write idxWrite = null;
    if (newSecondaryKey != null && !keyMatches) {
      idxWrite = new Write(newSecondaryKey, write.getRow(), EXISTS);
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

  public void delete(Delete delete) throws OperationException {
    // first read the existing row to find its current value of the index col
    Read firstRead = new Read(delete.getRow(), this.column);
    OperationResult<Map<byte[], byte[]>> firstResult =
        this.table.read(firstRead);
    byte[] oldSecondaryKey = null;
    if (!firstResult.isEmpty()) {
      oldSecondaryKey = firstResult.getValue().get(this.column);
    }

    // if there is an existing row with a value for the index column,
    // then we must remove that the row key from the index for that value;
    Delete idxDelete = null;
    if (oldSecondaryKey != null) {
      idxDelete = new Delete(oldSecondaryKey, delete.getRow());
    }

    // stage all operations against both tables
    this.table.stage(delete);
    if (idxDelete != null) {
      this.index.stage(idxDelete);
    }
  }

  public void swap(Swap swap) throws OperationException {
    // if the swap is on a column other than the column key, then
    // the index is not affected - just execute the swap.
    // also, if the swap is on the index column, but the old value
    // is the same as the new value, then the index is not affected either.
    if (!Arrays.equals(this.column, swap.getColumn()) ||
        Arrays.equals(swap.getExpected(), swap.getValue())) {
      this.table.stage(swap);
      return;
    }

    // the swap is on the index column. it will only succeed if the current
    // value matches the expected value of the swap. if that value is not null,
    // then we must remove the row key from the index for that value.
    Delete idxDelete = null;
    if (swap.getExpected() != null) {
      idxDelete = new Delete(swap.getExpected(), swap.getRow());
    }

    // if the new value is not null, then we must add the rowkey to the index
    // for that value.
    Write idxWrite = null;
    if (swap.getValue() != null) {
      idxWrite = new Write(swap.getValue(), swap.getRow(), EXISTS);
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

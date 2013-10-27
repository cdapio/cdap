package com.continuuity.metrics.data;

import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class LevelDBFuzzyRowFilter extends FilterBase {
  private FuzzyRowFilter filter;

  public LevelDBFuzzyRowFilter(FuzzyRowFilter f) {
    this.filter = f;
  }

  @Override
  public void reset() {
    try {
      filter.reset();
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    try {
      return filter.filterRowKey(buffer, offset, length);
    } catch (IOException ioe) {
      Throwables.propagate(ioe);
    }
    return false;
  }

  @Override
  public boolean filterAllRemaining() {
    return filter.filterAllRemaining();
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) {
    return filter.filterKeyValue(v);
  }

  @Override
  public Cell transformCell(Cell v) {
    try {
      return filter.transformCell(v);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void filterRowCells(List<Cell> cells) {
    try {
      filter.filterRow();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean hasFilterRow() {
    return filter.hasFilterRow();
  }

  @Override
  public boolean filterRow() {
    try {
      return filter.filterRow();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Cell getNextCellHint(Cell currentCell) {
    return filter.getNextCellHint(currentCell);
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    KeyValue output = null;
    try {
      KeyValue next = filter.getNextKeyHint(currentKV);
      KeyValue.Type type = KeyValue.Type.codeToType(next.getType());
      // using the currentKV's family, qualifier, timestamp, and value, otherwise they will
      // be empty byte arrays which will cause level db's KeyValue implementation to represent
      // the actual key it uses with fewer bytes, which will mess with the seeking.
      output = new KeyValue(next.getRow(), currentKV.getFamily(), currentKV.getQualifier(),
                            currentKV.getTimestamp(), type, currentKV.getValue());
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
    return output;
  }

  public static LevelDBFuzzyRowFilter parseFrom(byte[] serialized) throws DeserializationException {
    FuzzyRowFilter wrappedFilter = FuzzyRowFilter.parseFrom(serialized);
    return new LevelDBFuzzyRowFilter(wrappedFilter);
  }

  @Override
  public byte[] toByteArray() {
    return filter.toByteArray();
  }
}

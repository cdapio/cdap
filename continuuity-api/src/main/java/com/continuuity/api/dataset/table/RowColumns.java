package com.continuuity.api.dataset.table;

import com.continuuity.api.common.Bytes;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * Base class for defining a row with set of columns.
 * @param <T> implementor type - to support handy builder-like methods.
 */
public abstract class RowColumns<T> {
  private final byte[] row;

  private final List<byte[]> columns;

  public byte[] getRow() {
    return row;
  }

  public List<byte[]> getColumns() {
    return columns;
  }

  // key & column as byte[]

  public RowColumns(byte[] row) {
    this.row = row;
    this.columns = Lists.newArrayList();
  }

  public RowColumns(byte[] row, byte[]... columns) {
    this(row);
    add(columns);
  }

  @SuppressWarnings("unchecked")
  public T add(byte[]... columns) {
    Collections.addAll(this.columns, columns);
    return (T) this;
  }

  // key and column as String

  public RowColumns(String row) {
    this(Bytes.toBytes(row));
  }

  public RowColumns(String row, String... columns) {
    this(row);
    add(columns);
  }

  @SuppressWarnings("unchecked")
  public T add(String... columns) {
    for (String col : columns) {
      this.columns.add(Bytes.toBytes(col));
    }
    return (T) this;
  }
}

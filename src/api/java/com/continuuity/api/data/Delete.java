package com.continuuity.api.data;


public class Delete implements WriteOperation {

  private final byte [] key;
  private final byte [][] columns;

  public Delete(final byte [] key) {
    this(key, KV_COL_ARR);
  }

  public Delete(final byte [] key, final byte [] column) {
    this(key, new byte [][] { column });
  }

  public Delete(final byte [] key, final byte [][] columns) {
    this.key = key;
    this.columns = columns;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }

  public byte [][] getColumns() {
    return this.columns;
  }

  @Override
  public int getPriority() {
    return 1;
  }
}

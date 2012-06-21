package com.continuuity.api.data;


public class ColumnWrite implements WriteOperation {
  private final byte [] key;
  private final byte [][] columns;
  private final byte [][] values;

  public ColumnWrite(final byte [] key, final byte [] column,
      final byte [] value) {
    this(key, new byte [][] { column }, new byte [][] { value } );
  }

  public ColumnWrite(final byte [] key, final byte [][] columns,
      final byte [][] values) {
    this.key = key;
    this.columns = columns;
    this.values = values;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }

  public byte [][] getColumns() {
    return this.columns;
  }

  public byte [][] getValues() {
    return this.values;
  }

  @Override
  public int getPriority() {
    return 1;
  }
}

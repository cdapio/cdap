package com.continuuity.data.operation;

import com.google.common.base.Preconditions;

/**
 * An operations to spit a table into ranges of row keys
 */
public class GetSplits extends ReadOperation {

  // the table name
  private final String table;

  // the start and the end of the input range
  private final byte[] start, stop;

  // the columns to read
  private final byte[][] columns;

  // the number of splits requested
  private final int numSplits;

  /**
   * Get splits for all columns of the entire table
   */
  public GetSplits(String table) {
    this(table, -1);
  }

  /**
   * Get splits for an entire table, reading only one specified column.
   * @param table the name of the table
   * @param column the column to read
   */
  public GetSplits(String table, byte[] column) {
    this(table, -1, column);
  }

  /**
   * Get splits for an entire table, reading only the specified columns.
   * @param table the name of the table
   * @param columns the columns to read
   */
  public GetSplits(String table, byte[][] columns) {
    this(table, -1, columns);
  }

  /**
   * Get splits for a range of rows, reading all columns.
   * @param table the name of the table
   * @param start the start row
   * @param stop the (exclusive) end row
   */
  public GetSplits(String table, byte[] start, byte[] stop) {
    this(table, -1, start, stop);
  }

  /**
   * Get a specific number of splits for a range of rows, reading only one specified column.
   * @param table the name of the table
   * @param start the start row
   * @param stop the (exclusive) end row
   * @param column the column to read
   */
  public GetSplits(String table, byte[] start, byte[] stop, byte[] column) {
    this(table, start, stop, new byte[][] { column });
  }

  /**
   * Get a specific number of splits for a range of rows, reading only the specified columns.
   * @param table the name of the table
   * @param start the start row
   * @param stop the (exclusive) end row
   * @param columns the columns to read
   */
  public GetSplits(String table, byte[] start, byte[] stop, byte[][] columns) {
    this(table, -1, start, stop, columns);
  }

  /**
   * Get a specific number of splits for an entire table, reading all columns.
   * @param table the name of the table
   * @param numSplits the desired number of plits
   */
  public GetSplits(String table, int numSplits) {
    this(table, numSplits, null, null, (byte[][]) null);
  }

  /**
   * Get a specific number of splits for an entire table, reading only one specified column.
   * @param table the name of the table
   * @param numSplits the desired number of plits
   * @param column the column to read
   */
  public GetSplits(String table, int numSplits, byte[] column) {
    this(table, numSplits, new byte[][] { column });
  }

  /**
   * Get a specific number of splits for an entire table, reading only the specified columns.
   * @param table the name of the table
   * @param numSplits the desired number of plits
   * @param columns the columns to read
   */
  public GetSplits(String table, int numSplits, byte[][] columns) {
    this(table, numSplits, null, null, columns);
  }

  /**
   * Get a specific number of splits for a range of rows, reading all columns.
   * @param table the name of the table
   * @param numSplits the desired number of plits
   * @param start the start row
   * @param stop the (exclusive) end row
   */
  public GetSplits(String table, int numSplits, byte[] start, byte[] stop) {
    this(table, numSplits, start, stop, (byte[][]) null);
  }

  /**
   * Get a specific number of splits for a range of rows, reading only one specified column.
   * @param table the name of the table
   * @param numSplits the desired number of plits
   * @param start the start row
   * @param stop the (exclusive) end row
   * @param column the column to read
   */
  public GetSplits(String table, int numSplits, byte[] start, byte[] stop, byte[] column) {
    this(table, numSplits, start, stop, new byte[][] { column });
  }

  /**
   * Get a specific number of splits for a range of rows, reading only the specified columns.
   * @param table the name of the table
   * @param numSplits the desired number of plits
   * @param start the start row
   * @param stop the (exclusive) end row
   * @param columns the columns to read
   */
  public GetSplits(String table, int numSplits, byte[] start, byte[] stop, byte[][] columns) {
    Preconditions.checkArgument(columns == null || columns.length > 0, "columns must not have size 0");
    this.numSplits = numSplits;
    this.columns = columns;
    this.stop = stop;
    this.start = start;
    this.table = table;
  }

  /**
   * Constructor for deserializing.
   * @param id the id of the operation
   * @param table the name of the table
   * @param numSplits the desired number of plits
   * @param start the start row
   * @param stop the (exclusive) end row
   * @param columns the columns to read
   */
  public GetSplits(long id, String table, int numSplits, byte[] start, byte[] stop, byte[][] columns) {
    super(id);
    this.numSplits = numSplits;
    this.columns = columns;
    this.stop = stop;
    this.start = start;
    this.table = table;
  }

  /**
   * @return the name of the table
   */
  public String getTable() {
    return table;
  }

  /**
   * @return the start row for the splits
   */
  public byte[] getStart() {
    return start;
  }

  /**
   * @return the stop row for the splits
   */
  public byte[] getStop() {
    return stop;
  }

  /**
   * @return the columns to read
   */
  public byte[][] getColumns() {
    return columns;
  }

  /**
   * @return the number of requested splits
   */
  public int getNumSplits() {
    return numSplits;
  }
}

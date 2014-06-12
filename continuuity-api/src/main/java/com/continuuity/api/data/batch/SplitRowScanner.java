/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.data.batch;

/**
 * Defines a reader of a dataset {@link com.continuuity.api.data.batch.Split}.
 * @param <ROW> the type of objects that represents a single row
 */
public abstract class SplitRowScanner<ROW> {

  /**
   * Called once at initialization.
   * @param split The split that defines the range of rows to read.
   * @throws InterruptedException
   */
  public abstract void initialize(Split split) throws InterruptedException;

  /**
   * Read the next row.
   * @return true if a row was read.
   * @throws InterruptedException
   */
  public abstract boolean nextRow() throws InterruptedException;

  /**
   * Get the current row.
   * @return The current row, or null if there is no current row.
   * @throws InterruptedException
   */
  public abstract ROW getCurrentRow() throws InterruptedException;

  /**
   * The current progress of the record reader through its data.
   * By default progress is not reported in the middle of split reading.
   * @return A number between 0.0 and 1.0 that is the fraction of the data that has been read.
   * @throws InterruptedException
   */
  public float getProgress() throws InterruptedException {
    return 0;
  }

  /**
   * Close the record reader.
   */
  public abstract void close();
}

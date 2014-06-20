package com.continuuity.api.data.batch;

/**
 * Utility methods for row scanners.
 */
public class Scannables {

  /**
   * Provides a way to convert a key and a value - as provided by a split reader - in to a single row object.
   * @param <KEY> the key type
   * @param <VALUE> the value type
   * @param <ROW> the type representing a row
   */
  public interface RowMaker<KEY, VALUE, ROW> {

    /**
     * Convert a single key/value pair into a row.
     * @param key the key
     * @param value the value
     * @return the row
     */
    ROW makeRow(KEY key, VALUE value);
  }

  /**
   * Given a split reader and a way to convert its key/value pairs into rows, return a split row scanner that
   * delegates all operations to the underlying split reader.
   */
  public static <KEY, VALUE, ROW>
  SplitRowScanner<ROW> splitRowScanner(final SplitReader<KEY, VALUE> splitReader,
                                       final RowMaker<KEY, VALUE, ROW> rowMaker) {
    return new SplitRowScanner<ROW>() {
      @Override
      public void initialize(Split split) throws InterruptedException {
        splitReader.initialize(split);
      }

      @Override
      public boolean nextRow() throws InterruptedException {
        return splitReader.nextKeyValue();
      }

      @Override
      public ROW getCurrentRow() throws InterruptedException {
        return rowMaker.makeRow(splitReader.getCurrentKey(), splitReader.getCurrentValue());
      }

      @Override
      public void close() {
        splitReader.close();
      }

      @Override
      public float getProgress() throws InterruptedException {
        return splitReader.getProgress();
      }
    };
  }

  /**
   * Given a split reader and a way to convert its key/value pairs into rows, return a split row scanner that
   * delegates all operations to the underlying split reader.
   */
  public static <KEY, VALUE>
  SplitRowScanner<VALUE> valueRowScanner(final SplitReader<KEY, VALUE> splitReader) {
    return new SplitRowScanner<VALUE>() {
      @Override
      public void initialize(Split split) throws InterruptedException {
        splitReader.initialize(split);
      }

      @Override
      public boolean nextRow() throws InterruptedException {
        return splitReader.nextKeyValue();
      }

      @Override
      public VALUE getCurrentRow() throws InterruptedException {
        return splitReader.getCurrentValue();
      }

      @Override
      public void close() {
        splitReader.close();
      }

      @Override
      public float getProgress() throws InterruptedException {
        return splitReader.getProgress();
      }
    };
  }
}

package com.continuuity.api.data.batch;

import com.continuuity.api.annotation.Beta;

/**
 * Utility methods for record scanners.
 */
@Beta
public class Scannables {

  /**
   * Provides a way to convert a key and a value - as provided by a split reader - in to a single record object.
   * @param <KEY> the key type
   * @param <VALUE> the value type
   * @param <RECORD> the type representing a record
   */
  public interface RecordMaker<KEY, VALUE, RECORD> {

    /**
     * Convert a single key/value pair into a record.
     * @param key the key
     * @param value the value
     * @return the record
     */
    RECORD makeRecord(KEY key, VALUE value);
  }

  /**
   * Given a split reader and a way to convert its key/value pairs into records, return a split record scanner that
   * delegates all operations to the underlying split reader.
   */
  public static <KEY, VALUE, RECORD>
  RecordScanner<RECORD> splitRecordScanner(final SplitReader<KEY, VALUE> splitReader,
                                           final RecordMaker<KEY, VALUE, RECORD> recordMaker) {
    return new RecordScanner<RECORD>() {
      @Override
      public void initialize(Split split) throws InterruptedException {
        splitReader.initialize(split);
      }

      @Override
      public boolean nextRecord() throws InterruptedException {
        return splitReader.nextKeyValue();
      }

      @Override
      public RECORD getCurrentRecord() throws InterruptedException {
        return recordMaker.makeRecord(splitReader.getCurrentKey(), splitReader.getCurrentValue());
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
   * Given a split reader and a way to convert its key/value pairs into records, return a record scanner that
   * delegates all operations to the underlying split reader.
   */
  public static <KEY, VALUE>
  RecordScanner<VALUE> valueRecordScanner(final SplitReader<KEY, VALUE> splitReader) {
    return new RecordScanner<VALUE>() {
      @Override
      public void initialize(Split split) throws InterruptedException {
        splitReader.initialize(split);
      }

      @Override
      public boolean nextRecord() throws InterruptedException {
        return splitReader.nextKeyValue();
      }

      @Override
      public VALUE getCurrentRecord() throws InterruptedException {
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

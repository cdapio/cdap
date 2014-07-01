package com.continuuity.api.data.batch;

import com.continuuity.api.annotation.Beta;

import java.io.Closeable;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Represents data sets that can be processed in batches, as series of records (as apposed to key/value pairs). See
 * {@link BatchReadable}.
 * @param <RECORD> the type of objects that represents a single record
 */
@Beta
public interface RecordScannable<RECORD> extends Closeable {

  /**
   * This method is needed because Java does not remember the RECORD type parameter at runtime.
   * @return the schema type, that is RECORD.
   */
  Type getRecordType();

  /**
   * Returns all splits of the dataset.
   * <p>
   *   For feeding the whole dataset into a batch job.
   * </p>
   * @return A list of {@link Split}s.
   */
  List<Split> getSplits();

  /**
   * Creates a reader for the split of a dataset.
   * @param split The split to create a reader for.
   * @return The instance of a {@link RecordScanner}.
   */
  RecordScanner<RECORD> createSplitRecordScanner(Split split);
}

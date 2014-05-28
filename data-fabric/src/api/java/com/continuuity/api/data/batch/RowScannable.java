package com.continuuity.api.data.batch;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Represents data sets that can be processed in batches, as series of rows (as apposed to key/value pairs). See
 * {@link BatchReadable}.
 * @param <ROW> the type of objects that represents a single row
 */
public interface RowScannable<ROW> {

  /**
   * This method is needed because Java does not remember the ROW type parameter at runtime.
   * @returns the schema type, that is ROW.
   */
  Type getRowType();

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
   * @return The instance of a {@link SplitReader}.
   */
  SplitRowScanner<ROW> createSplitScanner(Split split);
}

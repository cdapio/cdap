/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

import com.continuuity.api.annotation.Beta;

import java.util.List;

/**
 * Interface for datasets that can be input to a batch job.
 * <p>
 *   In order to feed a dataset into a batch job, the dataset must be splittable into chunks so that it's possible 
 *   to process every part of the dataset in parallel. Every chunk must be readable as a collection of {key,value}
 *   records.
 * </p>
 * @param <KEY> The key type.
 * @param <VALUE> The value type.
 */
@Beta
public interface BatchReadable<KEY, VALUE> {
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
  SplitReader<KEY, VALUE> createSplitReader(Split split);
}

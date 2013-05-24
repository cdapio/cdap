/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.OperationException;

import java.util.List;

/**
 * Defines interface for dataset that can be input to a batch job.
 * <p>
 *   To feed dataset into batch job it should be splittable in chunks, so that it is possible to process every part in
 *   parallel. Every chunk should be readable as a collection of {key,value} records.
 * </p>
 * @param <KEY> the key type
 * @param <VALUE> the value type
 */
@Beta
public interface BatchReadable<KEY, VALUE> {
  /**
   * Returns all splits of the dataset.
   * <p>
   *   Used to feed whole dataset into batch job.
   * </p>
   * @return list of {@link Split}
   */
  List<Split> getSplits() throws OperationException;

  /**
   * Creates reader for the split of dataset.
   * @param split split to create reader for.
   * @return instance of a {@link SplitReader}
   */
  SplitReader<KEY, VALUE> createSplitReader(Split split);
}

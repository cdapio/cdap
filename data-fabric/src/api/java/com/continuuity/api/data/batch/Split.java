/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

/**
 * Defines split of the dataset.
 * <b>
 *   Typically a dataset is spit into multiple chunks that are fed into a batch job.
 * </b>
 */
public abstract class Split {
  /**
   * By default assume that the size of each split is roughly the same.
   *
   * @return Optional split length. Used only for tracking split consuming completion percentile.
   */
  public long getLength() {
    return 0;
  }
}

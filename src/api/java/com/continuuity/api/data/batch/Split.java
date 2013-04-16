/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

/**
 * Defines split of the dataset.
 * <b>
 *   Usually dataset is spit in multiple chunks which are fed into batch job.
 * </b>
 */
public abstract class Split {
  /**
   * @return optional split length. Used only for tracking split consuming completion percentile.
   */
  public long getLength() {
    // by default assume that the size of each split is roughly the same
    return 0;
  }
}

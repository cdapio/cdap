/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.pipeline;

/**
 * <p>A Stage represents a set of tasks that can be performed on objects,
 * and methods used to communicate with other stages in a {@link Pipeline}.</p>
 */
public interface Stage {

  /**
   * Implementation of this method should atomically process a single data
   * object and transfer any objects generated to downstream for processing.
   *
   * @param ctx
   */
  void process(Context ctx) throws Exception;
}

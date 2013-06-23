/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.pipeline;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * This class represents a processing system consisting of a number of stages.
 * Each {@link Stage} takes in data processes it and forwards it to the next {@link Stage}
 * <p/>
 * This class also allows all stages in the {@link Pipeline} to be managed collectively
 * with methods to run and get results of processing.
 */
public interface Pipeline<T> {
  /**
   * Adds a {@link Stage} to the end of this pipeline.
   *
   * @param stage to be added to this pipeline.
   */
  void addLast(Stage stage);

  /**
   * Runs this pipeline passing in the parameter to run with.
   *
   * @param o argument to run the pipeline.
   */
  ListenableFuture<T> execute(Object o);
}

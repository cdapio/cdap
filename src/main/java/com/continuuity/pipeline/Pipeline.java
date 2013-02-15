/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.pipeline;

/**
 * This class represents a processing system consisting of a number of stages.
 * Each {@link Stage} takes in data processes it and forwards it to the next {@link Stage}
 *
 * This class also allows all stages in the {@link Pipeline} to be managed collectively
 * with methods to run and get results of processing.
 */
public interface Pipeline {
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
  void execute(Object o) throws Exception;

  /**
   * @return Result of processing the pipeline.
   */
  Object getResult();
}

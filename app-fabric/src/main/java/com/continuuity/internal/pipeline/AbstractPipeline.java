/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.pipeline;

import com.continuuity.pipeline.Pipeline;
import com.continuuity.pipeline.Stage;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * Abstract implementation of {@link Pipeline}.
 *
 * @param <T> Type of object produced by this pipeline.
 */
public abstract class AbstractPipeline<T> implements Pipeline<T> {
  /**
   * List of stages in the pipeline.
   */
  private List<Stage> stages = Lists.newLinkedList();

  /**
   * Adds a {@link Stage} to the {@link Pipeline}.
   *
   * @param stage to be added to this pipeline.
   */
  @Override
  public void addLast(Stage stage) {
    stages.add(stage);
  }

  /**
   * @return list of Stages.
   */
  protected List<Stage> getStages() {
    return Collections.unmodifiableList(stages);
  }
}

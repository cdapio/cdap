/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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

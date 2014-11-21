/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.pipeline;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * This class represents a processing system consisting of a number of stages.
 * Each {@link Stage} takes in data processes it and forwards it to the next {@link Stage}
 * <p/>
 * This class also allows all stages in the {@link Pipeline} to be managed collectively
 * with methods to run and get results of processing.
 *
 * @param <T> Type of object produced by this Pipeline.
 */
public interface Pipeline<T> {
  /**
   * Adds a {@link Stage} to the end of this pipeline.
   *
   * @param stage to be added to this pipeline.
   */
  void addLast(Stage stage);

  /**
   * Sets a {@link Stage} that always be executed when the pipeline execution completed.
   * The context object of the last stage being executed will be provided to this final stage.
   */
  void setFinally(Stage stage);

  /**
   * Runs this pipeline passing in the parameter to run with.
   *
   * @param o argument to run the pipeline.
   */
  ListenableFuture<T> execute(Object o);
}

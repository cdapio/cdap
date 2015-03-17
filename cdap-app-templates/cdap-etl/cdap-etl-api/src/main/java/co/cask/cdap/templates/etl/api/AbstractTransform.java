/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.api;

/**
 * Transform Stage
 * @param <I> input
 * @param <O> output
 */
public abstract class AbstractTransform<I, O> implements StageLifecycle {

  private TransformContext context;

  /**
   * Configure the Transform stage.
   * @param configurer {@link StageConfigurer}
   */
  public void configure(StageConfigurer configurer) {
    configurer.setName(this.getClass().getSimpleName());
  }

  /**
   * Initialize the Transform Stage.
   * @param context {@link TransformContext}
   */
  public void initialize(TransformContext context) {
    this.context = context;
  }

  /**
   * Process I input and emit O output using {@link Emitter}
   * @param input input data
   * @param output {@link Emitter} emit output
   */
  public abstract void transform(I input, Emitter<O> output);

  @Override
  public void destroy() {
    //no-op
  }

  protected TransformContext getContext() {
    return context;
  }
}

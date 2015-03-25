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

import co.cask.cdap.api.ProgramLifecycle;

/**
 * Transform Stage.
 *
 * @param <KEY_IN> Type of KeyInput object
 * @param <VALUE_IN> Type of ValueInput object
 * @param <KEY_OUT> Type of KeyOutput object
 * @param <VALUE_OUT> Type of ValueOutput object
 */
public abstract class Transform<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> implements ProgramLifecycle<TransformContext> {

  private TransformContext context;

  /**
   * Configure the Transform stage. Used to provide information about the Transform.
   *
   * @param configurer {@link StageConfigurer}
   */
  public void configure(StageConfigurer configurer) {
    // no-op
  }

  /**
   * Initialize the Transform Stage. Called during the runtime with context of the Transform.
   *
   * @param context {@link TransformContext}
   */
  public void initialize(TransformContext context) {
    this.context = context;
  }

  /**
   * Process input Key and Value and emit output using {@link Emitter}.
   *
   * @param inputKey input key
   * @param inputValue input value
   * @param emitter {@link Emitter} to emit data to the next stage
   * @throws Exception
   */
  public void transform(final KEY_IN inputKey, VALUE_IN inputValue, final Emitter<KEY_OUT, VALUE_OUT> emitter)
    throws Exception {
    transform(inputValue, new ValueEmitter<VALUE_OUT>() {
      @Override
      public void emit(VALUE_OUT value) {
        emitter.emit((KEY_OUT) inputKey, value);
      }

      @Override
      public void emit(Void key, VALUE_OUT value) {
        emitter.emit(null, value);
      }
    });
  }

  /**
   * Process input and emit output using {@link ValueEmitter}
   *
   * @param input input
   * @param emitter {@link ValueEmitter} to emit data to the next stage
   * @throws Exception
   */
  public void transform(VALUE_IN input, ValueEmitter<VALUE_OUT> emitter) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void destroy() {
    //no-op
  }

  protected TransformContext getContext() {
    return context;
  }
}

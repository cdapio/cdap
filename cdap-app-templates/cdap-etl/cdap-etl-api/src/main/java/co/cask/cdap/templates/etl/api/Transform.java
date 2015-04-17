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
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;

/**
 * Transform Stage.
 *
 * @param <IN> Type of input object
 * @param <OUT> Type of output object
 */
public abstract class Transform<IN, OUT> implements ProgramLifecycle<TransformContext> {

  private final Type inputType = new TypeToken<IN>(getClass()) { }.getType();
  private final Type outputType = new TypeToken<OUT>(getClass()) { }.getType();

  /**
   * Get the Type of {@link IN}.
   *
   * @return {@link Type}
   */
  public final Type getInputType() {
    return inputType;
  }

  /**
   * Get the Type of {@link OUT}.
   *
   * @return {@link Type}
   */
  public final Type getOutputType() {
    return outputType;
  }

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
   * Initialize the Transform Stage. Transforms are initialized once when the program starts up. Initialization
   * is guaranteed to occur before any calls to {@link #transform(Object, Emitter)} are made. Transforms are also
   * initialized once during pipeline creation, so property validation should be done here in order to fail creation
   * of pipelines with invalid config.
   *
   * @param context {@link TransformContext}
   */
  public void initialize(TransformContext context) {
    this.context = context;
  }

  /**
   * Process input and emit output using {@link Emitter}.
   *
   * @param input the input to transform
   * @param emitter {@link Emitter} to emit data to the next stage
   * @throws Exception
   */
  public abstract void transform(IN input, Emitter<OUT> emitter) throws Exception;


  @Override
  public void destroy() {
    //no-op
  }

  protected TransformContext getContext() {
    return context;
  }
}

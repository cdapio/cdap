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
 * Transform Stage.
 *
 * @param <IN> Type of input object
 * @param <OUT> Type of output object
 */
public abstract class TransformStage<IN, OUT> implements Transform<IN, OUT> {

  private StageContext context;

  /**
   * Initialize the Transform Stage. Transforms are initialized once when the program starts up and
   * is guaranteed to occur before any calls to {@link #transform(Object, Emitter)} are made.
   *
   * @param context {@link StageContext}
   */
  public void initialize(StageContext context) {
    this.context = context;
  }

  public void destroy() {
    //no-op
  }

  protected StageContext getContext() {
    return context;
  }
}

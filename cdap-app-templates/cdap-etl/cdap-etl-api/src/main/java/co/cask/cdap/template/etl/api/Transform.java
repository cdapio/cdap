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

package co.cask.cdap.template.etl.api;

import co.cask.cdap.api.annotation.Beta;

/**
 * Transform Stage.
 *
 * @param <IN> Type of input object
 * @param <OUT> Type of output object
 */
@Beta
public abstract class Transform<IN, OUT> implements StageLifecycle<TransformContext>, Transformation<IN, OUT> {

  private TransformContext context;

  /**
   * Initialize the Transform Stage.
   *
   * @param context {@link TransformContext}
   * @throws Exception if there is any error during initialization
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    this.context = context;
  }

  /**
   * Destroy the Transform Stage.
   */
  @Override
  public void destroy() {
    //no-op
  }

  protected TransformContext getContext() {
    return context;
  }
}

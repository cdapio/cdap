/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

/**
 * A Transform that can split its input among multiple output ports.
 *
 * @param <T> type of input record
 * @param <E> type of error records emitted. Usually the same as the input record type
 */
public abstract class SplitterTransform<T, E>
  implements MultiOutputTransformation<T, E>, MultiOutputPipelineConfigurable, StageLifecycle<TransformContext> {

  public static final String PLUGIN_TYPE = "splittertransform";

  @Override
  public void configurePipeline(MultiOutputPipelineConfigurer multiOutputPipelineConfigurer) {
    // no-op
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    // no-op
  }

  @Override
  public void destroy() {
    // no-op
  }
}

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

/**
 * A factory interface for creating pipelines. This class allows to
 * implement different {@link co.cask.cdap.pipeline.Pipeline} based on external constraints.
 */
public interface PipelineFactory {
  /**
   * @return A {@link co.cask.cdap.pipeline.Pipeline} created by the factory.
   */
  <T> Pipeline<T> getPipeline();
}

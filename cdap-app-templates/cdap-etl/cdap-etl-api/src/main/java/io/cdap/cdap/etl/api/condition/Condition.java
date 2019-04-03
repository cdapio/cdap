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

package co.cask.cdap.etl.api.condition;

import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurer;

/**
 * Represents condition to be executed in the pipeline.
 */
public abstract class Condition implements PipelineConfigurable {
  public static final String PLUGIN_TYPE = "condition";

  /**
   * Returns the result of execution of the condition. If {@code true} is returned, stages on the
   * true branch will get executed, otherwise staged on the false branch will get executed.
   * @param context the context which is used for evaluating the conditions
   * @return boolean value based on which either the true branch or false branch will get executed
   * @throws Exception if any error occurred while evaluating the condition
   */
  public abstract boolean apply(ConditionContext context) throws Exception;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    //no-op
  }
}

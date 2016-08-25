/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.api.action;

import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurer;

/**
 * Represents custom logic to be executed in the pipeline.
 */
public abstract class Action implements PipelineConfigurable {
  public static final String PLUGIN_TYPE = "action";

  /**
   * Implement this method to execute the code as a part of action run.
   * @param context the action context, containing information about the pipeline run
   * @throws Exception when there is failure in method execution
   */
  public abstract void run(ActionContext context) throws Exception;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    //no-op
  }
}

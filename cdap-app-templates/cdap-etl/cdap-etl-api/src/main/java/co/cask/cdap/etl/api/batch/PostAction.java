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

package co.cask.cdap.etl.api.batch;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.PipelineConfigurer;

/**
 * Defines some logic that runs at the end of a workflow run. The action is run regardless of whether the pipeline
 * successfully completed or not.
 */
@Beta
public abstract class PostAction implements PipelineConfigurable {
  public static final String PLUGIN_TYPE = "postaction";

  /**
   * Configure the pipeline. This is run once when the pipeline is being published.
   * This is where you perform any static logic, like creating required datasets, registering required plugins,
   * and things of that nature.
   *
   * @param pipelineConfigurer the configurer used to add required datasets and streams
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // no-op
  }

  /**
   * Run the action.
   *
   * @param context the action context, containing information about the pipeline run
   * @throws Exception if there was an error running the ending action
   */
  public abstract void run(BatchActionContext context) throws Exception;
}

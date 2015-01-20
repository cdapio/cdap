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

package co.cask.cdap.conversion.app;

import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowConfigurer;

/**
 * Workflow that periodically reads data from a stream and writes it into a time partitioned fileset.
 */
public class StreamConversionWorkflow implements Workflow {
  @Override
  public void configure(WorkflowConfigurer configurer) {
    configurer.setName("StreamConversionWorkflow");
    configurer.setDescription("Periodically reads stream data and writes it into a FileSet");
    configurer.addMapReduce("StreamConversionMapReduce");
  }
}

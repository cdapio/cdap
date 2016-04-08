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

package co.cask.cdap.datapipeline;

import co.cask.cdap.api.workflow.WorkflowConfigurer;

/**
 * Adds programs on the workflow trunk.
 */
public class TrunkProgramAdder implements WorkflowProgramAdder {
  private final WorkflowConfigurer configurer;

  public TrunkProgramAdder(WorkflowConfigurer configurer) {
    this.configurer = configurer;
  }

  @Override
  public void addMapReduce(String name) {
    configurer.addMapReduce(name);
  }

  @Override
  public void addSpark(String name) {
    configurer.addSpark(name);
  }
}

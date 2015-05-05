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

package co.cask.cdap.examples.streamconversion;

import co.cask.cdap.api.workflow.AbstractWorkflow;

/**
 * A workflow that only runs the stream conversion MapReduce.
 */
public class StreamConversionWorkflow extends AbstractWorkflow {

  @Override
  protected void configure() {
    setName("StreamConversionWorkflow");
    setDescription("Runs the stream conversion MapReduce.");
    addMapReduce("StreamConversionMapReduce");
  }
}

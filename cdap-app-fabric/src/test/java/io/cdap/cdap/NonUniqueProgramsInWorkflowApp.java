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

package co.cask.cdap;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.workflow.AbstractWorkflow;

/**
 * App to test the non unique program names in the Workflow.
 */
public class NonUniqueProgramsInWorkflowApp extends AbstractApplication {

  @Override
  public void configure() {
    addMapReduce(new NoOpMR());
    addWorkflow(new NonUniqueProgramsInWorkflow());
  }

  /**
   * No operation MapReduce program.
   */
  public static class NoOpMR extends AbstractMapReduce {
  }

  /**
   * Workflow containing same MapReduce program multiple times.
   */
  public static class NonUniqueProgramsInWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      addMapReduce("NoOpMR");
      addMapReduce("NoOpMR");
    }
  }
}

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
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.workflow.AbstractWorkflow;

/**
 * Application to test the deletion of the Schedules after unrecoverable reset
 */
public class AppForUnrecoverableResetTest extends AbstractApplication {
  @Override
  public void configure() {
    setName("AppForUnrecoverableResetTest");
    setDescription("Application to test the deletion of the Schedules after unrecoverable reset");
    addWorkflow(new DummyWorkflow());
    addMapReduce(new DummyMR());
    schedule(buildSchedule("Every5HourSchedule", ProgramType.WORKFLOW, "DummyWorkflow")
               .setDescription("Every 5 hour schedule")
               .triggerByTime("0 */5 * * *"));
  }

  /**
   * Dummy workflow
   */
  class DummyWorkflow extends AbstractWorkflow {

    @Override
    protected void configure() {
      setName("DummyWorkflow");
      addMapReduce("DummyMR");
    }
  }

  /**
   * Dummy MapReduce which does nothing
   */
  static class DummyMR extends AbstractMapReduce {
  }
}

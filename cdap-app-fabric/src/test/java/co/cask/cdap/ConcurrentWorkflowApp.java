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
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import com.google.common.collect.Maps;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ConcurrentWorkflowApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("ConcurrentWorkflowApp");
    setDescription("Application with concurrently running Workflow instances");
    addWorkflow(new ConcurrentWorkflow());

    // Schedule Workflow
    Map<String, String> schedule1Properties = Maps.newHashMap();
    schedule1Properties.put("schedule.name", "concurrentWorkflowSchedule1");
    scheduleWorkflow(Schedules.createTimeSchedule("concurrentWorkflowSchedule1", "", "* * * * *"),
                     "ConcurrentWorkflow", schedule1Properties);

    Map<String, String> schedule2Properties = Maps.newHashMap();
    schedule2Properties.put("schedule.name", "concurrentWorkflowSchedule2");
    scheduleWorkflow(Schedules.createTimeSchedule("concurrentWorkflowSchedule2", "", "* * * * *"),
                     "ConcurrentWorkflow", schedule2Properties);
  }

  /**
   *
   */
  private static class ConcurrentWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("ConcurrentWorkflow");
      setDescription("Workflow configured to run concurrently.");
      addAction(new SimpleAction());
    }
  }

  static final class SimpleAction extends AbstractWorkflowAction {
    @Override
    public void run() {
      try {
        String scheduleName = getContext().getRuntimeArguments().get("schedule.name");

        File file = new File(getContext().getRuntimeArguments().get(scheduleName + ".file"));
        System.out.println("Creating file - " + getContext().getRuntimeArguments().get(scheduleName + ".file"));
        file.createNewFile();
        File doneFile = new File(getContext().getRuntimeArguments().get("done.file"));

        while (!doneFile.exists()) {
          TimeUnit.MILLISECONDS.sleep(50);
        }
      } catch (Exception e) {
        // no-op
      }
    }
  }
}

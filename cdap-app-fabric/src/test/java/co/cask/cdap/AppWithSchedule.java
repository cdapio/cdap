/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Application with workflow scheduling.
 */
public class AppWithSchedule extends AbstractApplication<AppWithSchedule.AppConfig> {

  public static final String NAME = "AppWithSchedule";
  public static final String STREAM = "SampleStream";
  public static final String WORKFLOW_NAME = "SampleWorkflow";
  public static final String SCHEDULE = "SampleSchedule";
  public static final String SCHEDULE_2 = "SampleSchedule2";
  public static final String MAPREDUCE = "SampleMR";

  @Override
  public void configure() {
    try {
      setName(NAME);
      setDescription("Sample application");
      ObjectStores.createObjectStore(getConfigurer(), "input", String.class);
      ObjectStores.createObjectStore(getConfigurer(), "output", String.class);
      addStream(STREAM);
      AppConfig config = getConfig();
      // if add workflow is false, we want to add a flow, so the app will have at least one program, for testing deploy
      if (!config.addWorkflow) {
        addFlow(new AllProgramsApp.NoOpFlow());
      }

      if (config.addWorkflow) {
        addMapReduce(new SampleMR());
        addWorkflow(new SampleWorkflow());
      }

      Map<String, String> scheduleProperties = Maps.newHashMap();
      scheduleProperties.put("oneKey", "oneValue");
      scheduleProperties.put("anotherKey", "anotherValue");
      scheduleProperties.put("someKey", "someValue");

      if (config.addWorkflow && config.addSchedule1) {
        schedule(
          buildSchedule(SCHEDULE, ProgramType.WORKFLOW, WORKFLOW_NAME)
          .setDescription("Sample schedule")
          .setProperties(scheduleProperties)
          .triggerByTime("0/15 * * * * ?"));
      }
      if (config.addWorkflow && config.addSchedule2) {
        scheduleWorkflow(Schedules.builder(SCHEDULE_2)
                           .setDescription("Sample schedule")
                           .createTimeSchedule("0/30 * * * * ?"),
                         WORKFLOW_NAME,
                         scheduleProperties);
      }
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }



  /**
   * Application Config Class to control schedule creation
   */
  public static class AppConfig extends Config {
    private final boolean addWorkflow;
    private final boolean addSchedule1;
    private final boolean addSchedule2;

    public AppConfig() {
      this.addSchedule1 = true;
      this.addSchedule2 = false;
      this.addWorkflow = true;
    }

    public AppConfig(boolean addWorkflow, boolean addSchedule1, boolean addSchedule2) {
      this.addWorkflow = addWorkflow;
      this.addSchedule1 = addSchedule1;
      this.addSchedule2 = addSchedule2;
    }
  }

  /**
   * Sample workflow. Schedules a dummy MR job.
   */
  public static class SampleWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
        setName("SampleWorkflow");
        setDescription("SampleWorkflow description");
        addAction(new DummyAction());
    }
  }

  /**
   * DummyAction
   */
  public static class DummyAction extends AbstractCustomAction {
    private static final Logger LOG = LoggerFactory.getLogger(DummyAction.class);
    @Override
    public void run() {
      Preconditions.checkArgument(getContext().getRuntimeArguments().get("oneKey").equals("oneValue"));
      Preconditions.checkArgument(getContext().getRuntimeArguments().get("anotherKey").equals("anotherValue"));
      Preconditions.checkArgument(getContext().getRuntimeArguments().get("someKey").equals("someWorkflowValue"));
      Preconditions.checkArgument(getContext().getRuntimeArguments().get("workflowKey").equals("workflowValue"));
      LOG.info("Ran dummy action");
    }
  }

  private class SampleMR extends AbstractMapReduce {
    @Override
    public void configure() { }
  }
}

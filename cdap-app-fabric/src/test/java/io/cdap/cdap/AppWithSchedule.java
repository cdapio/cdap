/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.lib.ObjectStores;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.worker.AbstractWorker;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Application with workflow scheduling.
 */
public class AppWithSchedule extends AbstractApplication<AppWithSchedule.AppConfig> {

  public static final String NAME = "AppWithSchedule";
  public static final String DESC = "Sample application with schedule";
  public static final String WORKFLOW_NAME = "SampleWorkflow";
  public static final String SCHEDULE = "SampleSchedule";
  public static final String SCHEDULE_2 = "SampleSchedule2";
  public static final String MAPREDUCE = "SampleMR";
  public static final String SPARK = "SampleSpark";
  public static final String WORKER = "DummyWorker";

  @Override
  public void configure() {
    try {
      setName(NAME);
      setDescription(DESC);
      ObjectStores.createObjectStore(getConfigurer(), "input", String.class);
      ObjectStores.createObjectStore(getConfigurer(), "output", String.class);
      AppConfig config = getConfig();
      // if add workflow is false, we want to add a flow, so the app will have at least one program, for testing deploy
      if (!config.addWorkflow) {
        addWorker(new DummyWorker());
      }

      if (config.addWorkflow) {
        addMapReduce(new SampleMR());
        addSpark(new SampleSpark());
        addWorkflow(new SampleWorkflow());
        addWorker(new DummyWorker());
      }

      Map<String, String> scheduleProperties = Maps.newHashMap();
      scheduleProperties.put("oneKey", "oneValue");
      scheduleProperties.put("anotherKey", "anotherValue");
      scheduleProperties.put("someKey", "someValue");
      scheduleProperties.putAll(config.scheduleProperties);

      if (config.addWorkflow && config.addSchedule1) {
        schedule(
          buildSchedule(SCHEDULE, ProgramType.WORKFLOW, WORKFLOW_NAME)
          .setDescription("Sample schedule")
          .setProperties(scheduleProperties)
          .triggerByTime("0/15 * * * * ?"));
      }
      if (config.addWorkflow && config.addSchedule2) {
        schedule(buildSchedule(SCHEDULE_2, ProgramType.WORKFLOW, WORKFLOW_NAME)
                   .setDescription("Sample schedule")
                   .setProperties(scheduleProperties)
                   .triggerByTime("0/30 * * * * ?"));
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
    private final Map<String, String> scheduleProperties;

    public AppConfig() {
      this(true, true, false);
    }

    public AppConfig(boolean addWorkflow, boolean addSchedule1, boolean addSchedule2) {
      this(addWorkflow, addSchedule1, addSchedule2, Collections.emptyMap());
    }

    public AppConfig(boolean addWorkflow, boolean addSchedule1, boolean addSchedule2,
                     Map<String, String> scheduleProperties) {
      this.addWorkflow = addWorkflow;
      this.addSchedule1 = addSchedule1;
      this.addSchedule2 = addSchedule2;
      this.scheduleProperties = scheduleProperties;
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

  /**
   * A dummy worker that does nothing, but only wait for being stopped.
   */
  public static final class DummyWorker extends AbstractWorker {

    private final CountDownLatch stopLatch = new CountDownLatch(1);

    @Override
    public void run() {
      Uninterruptibles.awaitUninterruptibly(stopLatch);
    }

    @Override
    public void stop() {
      stopLatch.countDown();
    }
  }

  private class SampleMR extends AbstractMapReduce {
    @Override
    public void configure() { }
  }

  private class SampleSpark extends AbstractSpark {
    @Override
    public void configure() { }
  }
}

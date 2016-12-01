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
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Application with workflow scheduling based on a {@link StreamSizeSchedule}.
 */
public class AppWithStreamSizeSchedule extends AbstractApplication {

  @Override
  public void configure() {
    try {
      setName("AppWithStreamSizeSchedule");
      setDescription("Sample application");
      ObjectStores.createObjectStore(getConfigurer(), "input", String.class);
      ObjectStores.createObjectStore(getConfigurer(), "output", String.class);
      addWorkflow(new SampleWorkflow());
      addStream(new Stream("stream"));

      Map<String, String> scheduleProperties = Maps.newHashMap();
      scheduleProperties.put("oneKey", "oneValue");
      scheduleProperties.put("anotherKey", "anotherValue");
      scheduleProperties.put("someKey", "someValue");

      scheduleWorkflow(Schedules.builder("SampleSchedule1").createDataSchedule(Schedules.Source.STREAM, "stream", 1),
                       "SampleWorkflow", scheduleProperties);
      scheduleWorkflow(Schedules.builder("SampleSchedule2").createDataSchedule(Schedules.Source.STREAM, "stream", 2),
                       "SampleWorkflow", scheduleProperties);
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
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
      LOG.info("Ran dummy action");
      try {
        TimeUnit.MILLISECONDS.sleep(500);
        Preconditions.checkArgument(getContext().getRuntimeArguments().get("oneKey").equals("oneValue"));
        Preconditions.checkArgument(getContext().getRuntimeArguments().get("anotherKey").equals("anotherValue"));
        Preconditions.checkArgument(getContext().getRuntimeArguments().get("someKey").equals("someValue"));
      } catch (InterruptedException e) {
        LOG.info("Interrupted");
      }
    }
  }
}

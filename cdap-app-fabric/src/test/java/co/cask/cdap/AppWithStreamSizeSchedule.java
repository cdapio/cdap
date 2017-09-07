/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * Application with workflow scheduling based on a {@link StreamSizeSchedule}.
 */
public class AppWithStreamSizeSchedule extends AbstractApplication {

  public static final Map<String, String> SCHEDULE_PROPS = ImmutableMap.of(
      "oneKey", "oneValue",
      "anotherKey", "anotherValue",
      "someKey", "someValue");

  @Override
  public void configure() {
    setName("AppWithStreamSizeSchedule");
    setDescription("Sample application");
    addWorkflow(new SampleWorkflow());
    addStream(new Stream("stream"));

    scheduleWorkflow(Schedules.builder("SampleSchedule1").createDataSchedule(Schedules.Source.STREAM, "stream", 1),
                     "SampleWorkflow", SCHEDULE_PROPS);
    scheduleWorkflow(Schedules.builder("SampleSchedule2").createDataSchedule(Schedules.Source.STREAM, "stream", 2),
                     "SampleWorkflow", SCHEDULE_PROPS);
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
    @Override
    public void run() {
      Preconditions.checkArgument(getContext().getRuntimeArguments().get("oneKey").equals("oneValue"));
      Preconditions.checkArgument(getContext().getRuntimeArguments().get("anotherKey").equals("anotherValue"));
      Preconditions.checkArgument(getContext().getRuntimeArguments().get("someKey").equals("someValue"));

      List<TriggerInfo> triggerInfos = getContext().getTriggeringScheduleInfo().getTriggerInfos();
      Preconditions.checkState(triggerInfos.size() == 1);
      TriggerInfo info = triggerInfos.get(0);
      Preconditions.checkState(info.getType() == TriggerInfo.Type.STREAM_SIZE);
    }
  }
}

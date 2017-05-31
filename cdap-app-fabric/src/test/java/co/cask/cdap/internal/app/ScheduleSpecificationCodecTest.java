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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ScheduleSpecificationCodecTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .create();

  @Test
  public void testTimeSchedule() throws Exception {
    TimeSchedule timeSchedule = (TimeSchedule) Schedules.builder("foo")
      .setDescription("bar")
      .createTimeSchedule("cronEntry");
    ScheduleProgramInfo programInfo = new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, "testWorkflow");
    ImmutableMap<String, String> properties = ImmutableMap.of("a", "b", "c", "d");
    ScheduleSpecification specification = new ScheduleSpecification(timeSchedule, programInfo, properties);

    String jsonStr = GSON.toJson(specification);
    ScheduleSpecification deserialized = GSON.fromJson(jsonStr, ScheduleSpecification.class);

    Assert.assertEquals(specification, deserialized);
  }

  @Test
  public void testStreamSizeSchedule() throws Exception {
    Schedule dataSchedule = Schedules.builder("foo")
      .setDescription("bar")
      .createDataSchedule(Schedules.Source.STREAM, "stream", 10);
    ScheduleProgramInfo programInfo = new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, "testWorkflow");
    ImmutableMap<String, String> properties = ImmutableMap.of("a", "b", "c", "d");
    ScheduleSpecification specification = new ScheduleSpecification(dataSchedule, programInfo, properties);

    String jsonStr = GSON.toJson(specification);
    ScheduleSpecification deserialized = GSON.fromJson(jsonStr, ScheduleSpecification.class);

    Assert.assertEquals(specification, deserialized);
  }

  @Test
  public void testBackwardsCompatibility() throws Exception {
    // Before 2.8, the ScheduleSpecificationCodec used to have the same behavior as what Gson would do by
    // default, and only Schedules existed. We make sure that ScheduleSpecification persisted before
    // 2.8 can still be deserialized using the new codec.
    String cronEntry = "* * * * *";
    Schedule schedule = new OldSchedule("foo", "bar", cronEntry);
    ScheduleProgramInfo programInfo = new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, "testWorkflow");
    ImmutableMap<String, String> properties = ImmutableMap.of("a", "b", "c", "d");
    ScheduleSpecification specification = new ScheduleSpecification(schedule, programInfo, properties);

    // Use default Gson to serialize
    String jsonStr = new Gson().toJson(specification);

    ScheduleSpecification deserialized = GSON.fromJson(jsonStr, ScheduleSpecification.class);
    ScheduleSpecification expectedSpec = new ScheduleSpecification(
      Schedules.builder(schedule.getName()).setDescription(schedule.getDescription()).createTimeSchedule(cronEntry),
      programInfo, properties);

    Assert.assertEquals(expectedSpec, deserialized);
  }

  @Test
  public void testAppConfigurerRoute() throws Exception {
    Application app = new AbstractApplication() {
      @Override
      public void configure() {
        // intentionally use the deprecated scheduleWorkflow method to for timeSchedule
        // to test TimeSchedule deserialization
        scheduleWorkflow(Schedules.builder("timeSchedule").createTimeSchedule("0 * * * *"), "workflow");
        scheduleWorkflow(Schedules.builder("streamSizeSchedule")
                           .createDataSchedule(Schedules.Source.STREAM, "stream", 1),
                         "workflow");
      }
    };
    ApplicationSpecification specification = Specifications.from(app);

    ApplicationSpecificationAdapter gsonAdapater =
      ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    String jsonStr = gsonAdapater.toJson(specification);

    ApplicationSpecification deserializedSpec = gsonAdapater.fromJson(jsonStr);
    Assert.assertEquals(new TimeSchedule("timeSchedule", "", "0 * * * *"),
                        deserializedSpec.getSchedules().get("timeSchedule").getSchedule());
    Assert.assertEquals(new StreamSizeSchedule("streamSizeSchedule", "", "stream", 1),
                        deserializedSpec.getSchedules().get("streamSizeSchedule").getSchedule());
  }

  // the Schedule class used to look like this prior to v2.8
  private class OldSchedule extends Schedule {
    private final String cronEntry;

    OldSchedule(String name, String description, String cronEntry) {
      super(name, description, null);
      this.cronEntry = cronEntry;
    }
  }
}

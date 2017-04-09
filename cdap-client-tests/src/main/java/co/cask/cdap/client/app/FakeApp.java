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

package co.cask.cdap.client.app;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.schedule.Schedules;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import javax.annotation.Nullable;

/**
 *
 */
public class FakeApp extends AbstractApplication<FakeApp.AppConfig> {

  public static final String NAME = "FakeApp";
  public static final String STREAM_NAME = "fakeStream";
  public static final String DS_NAME = "fakeds";

  public static final String TIME_SCHEDULE_NAME = "someSchedule";
  public static final String SCHEDULE_CRON = "0 0 1 1 *";
  public static final String STREAM_SCHEDULE_NAME = "streamSchedule";
  public static final int STREAM_TRIGGER_MB = 10000;

  public static final List<String> FLOWS = Lists.newArrayList(FakeFlow.NAME);
  public static final List<String> MAPREDUCES = Lists.newArrayList();
  public static final List<String> SPARK = Lists.newArrayList(FakeSpark.NAME);
  public static final List<String> WORKFLOWS = Lists.newArrayList(FakeWorkflow.NAME);
  public static final List<String> SERVICES = Lists.newArrayList(PingService.NAME, PrefixedEchoHandler.NAME);
  public static final List<String> ALL_PROGRAMS = ImmutableList.<String>builder()
    .addAll(FLOWS)
    .addAll(MAPREDUCES)
    .addAll(WORKFLOWS)
    .addAll(SPARK)
    .addAll(SERVICES)
    .build();

  /**
   * Application Config Class to control schedule creation
   */
  public static class AppConfig extends Config {
    private final boolean addTimeSchedule;
    private final boolean addSizeSchedule;
    private final String timeScheduleName;
    private final String sizeScheduleName;
    private final String timeScheduleCron;


    public AppConfig() {
      this.addTimeSchedule = true;
      this.addSizeSchedule = true;
      this.timeScheduleName = TIME_SCHEDULE_NAME;
      this.sizeScheduleName = STREAM_SCHEDULE_NAME;
      this.timeScheduleCron = SCHEDULE_CRON;
    }

    public AppConfig(boolean addTimeSchedule, boolean addSizeSchedule, @Nullable String timeScheduleName,
                     @Nullable String sizeScheduleName, String timeScheduleCron) {
      this.addTimeSchedule = addTimeSchedule;
      this.addSizeSchedule = addSizeSchedule;
      this.timeScheduleName = timeScheduleName == null ? TIME_SCHEDULE_NAME : timeScheduleName;
      this.sizeScheduleName = sizeScheduleName == null ? STREAM_SCHEDULE_NAME : sizeScheduleName;
      this.timeScheduleCron = timeScheduleCron == null ? SCHEDULE_CRON : timeScheduleCron;
    }
  }
  @Override
  public void configure() {
    setName(NAME);
    addStream(new Stream(STREAM_NAME));
    addDatasetModule(FakeDatasetModule.NAME, FakeDatasetModule.class);
    createDataset(DS_NAME, FakeDataset.class.getName());
    addFlow(new FakeFlow());
    addSpark(new FakeSpark());
    addWorkflow(new FakeWorkflow());
    AppConfig config = getConfig();
    if (config.addTimeSchedule) {
      scheduleWorkflow(Schedules.builder(config.timeScheduleName).createTimeSchedule(config.timeScheduleCron),
                       FakeWorkflow.NAME);
    }
    if (config.addSizeSchedule) {
      scheduleWorkflow(Schedules.builder(config.sizeScheduleName)
                         .createDataSchedule(Schedules.Source.STREAM, STREAM_NAME, STREAM_TRIGGER_MB),
                       FakeWorkflow.NAME);
    }
    addService(PingService.NAME, new PingService());
    addService(PrefixedEchoHandler.NAME, new PrefixedEchoHandler());
  }
}

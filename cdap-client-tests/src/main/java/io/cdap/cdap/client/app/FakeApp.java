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

package io.cdap.cdap.client.app;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ProgramType;

import java.util.List;
import javax.annotation.Nullable;

/**
 *
 */
public class FakeApp extends AbstractApplication<FakeApp.AppConfig> {

  public static final String NAME = "FakeApp";
  public static final String DS_NAME = "fakeds";

  public static final String TIME_SCHEDULE_NAME = "someSchedule";
  public static final String SCHEDULE_CRON = "0 0 1 1 *";

  public static final List<String> MAPREDUCES = Lists.newArrayList();
  public static final List<String> SPARK = Lists.newArrayList(FakeSpark.NAME);
  public static final List<String> WORKFLOWS = Lists.newArrayList(FakeWorkflow.NAME);
  public static final List<String> SERVICES = Lists.newArrayList(PingService.NAME, PrefixedEchoHandler.NAME,
                                                                 DatasetWriterService.NAME);
  public static final List<String> ALL_PROGRAMS = ImmutableList.<String>builder()
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
    private final String timeScheduleName;
    private final String timeScheduleCron;


    public AppConfig() {
      this.addTimeSchedule = true;
      this.timeScheduleName = TIME_SCHEDULE_NAME;
      this.timeScheduleCron = SCHEDULE_CRON;
    }

    public AppConfig(boolean addTimeSchedule, @Nullable String timeScheduleName,
                     String timeScheduleCron) {
      this.addTimeSchedule = addTimeSchedule;
      this.timeScheduleName = timeScheduleName == null ? TIME_SCHEDULE_NAME : timeScheduleName;
      this.timeScheduleCron = timeScheduleCron == null ? SCHEDULE_CRON : timeScheduleCron;
    }
  }
  @Override
  public void configure() {
    setName(NAME);
    addDatasetModule(FakeDatasetModule.NAME, FakeDatasetModule.class);
    createDataset(DS_NAME, FakeDataset.class.getName());
    addSpark(new FakeSpark());
    addWorkflow(new FakeWorkflow());
    AppConfig config = getConfig();
    if (config.addTimeSchedule) {
      schedule(buildSchedule(config.timeScheduleName, ProgramType.WORKFLOW, FakeWorkflow.NAME)
                 .triggerByTime(config.timeScheduleCron));
    }
    addService(PingService.NAME, new PingService());
    addService(PrefixedEchoHandler.NAME, new PrefixedEchoHandler());
    addService(DatasetWriterService.NAME, new DatasetWriterService());
  }
}

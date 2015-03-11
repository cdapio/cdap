/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.schedule.Schedules;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 *
 */
public class FakeApp extends AbstractApplication {

  public static final String NAME = "FakeApp";
  public static final String STREAM_NAME = "fakeStream";
  public static final String DS_NAME = "fakeds";
  public static final String SCHEDULE_NAME = "someSchedule";
  public static final String STREAM_SCHEDULE_NAME = "streamSchedule";

  public static final List<String> FLOWS = Lists.newArrayList(FakeFlow.NAME);
  public static final List<String> PROCEDURES = Lists.newArrayList(FakeProcedure.NAME);
  public static final List<String> MAPREDUCES = Lists.newArrayList();
  public static final List<String> SPARK = Lists.newArrayList(FakeSpark.NAME);
  public static final List<String> WORKFLOWS = Lists.newArrayList(FakeWorkflow.NAME);
  public static final List<String> SERVICES = Lists.newArrayList(PingService.NAME, PrefixedEchoHandler.NAME);
  public static final List<String> ALL_PROGRAMS = ImmutableList.<String>builder()
    .addAll(FLOWS)
    .addAll(PROCEDURES)
    .addAll(MAPREDUCES)
    .addAll(WORKFLOWS)
    .addAll(SERVICES)
    .build();

  @Override
  public void configure() {
    setName(NAME);
    addStream(new Stream(STREAM_NAME));
    addDatasetModule(FakeDatasetModule.NAME, FakeDatasetModule.class);
    createDataset(DS_NAME, FakeDataset.class.getName());
    addProcedure(new FakeProcedure());
    addFlow(new FakeFlow());
    addWorkflow(new FakeWorkflow());
    scheduleWorkflow(Schedules.createTimeSchedule(SCHEDULE_NAME, "", "0 0 1 1 *"), FakeWorkflow.NAME);
    scheduleWorkflow(Schedules.createDataSchedule(STREAM_SCHEDULE_NAME, "", Schedules.Source.STREAM,
                                                  STREAM_NAME, 10000), FakeWorkflow.NAME);
    addService(PingService.NAME, new PingService());
    addService(PrefixedEchoHandler.NAME, new PrefixedEchoHandler());
  }
}

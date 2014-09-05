/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.reactor.client.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.twill.api.ResourceSpecification;

import java.util.List;

/**
 *
 */
public class FakeApp extends AbstractApplication {

  public static final String NAME = "FakeApp";
  public static final String STREAM_NAME = "fakeStream";
  public static final String DS_NAME = "fakeDS";

  public static final List<String> FLOWS = Lists.newArrayList(FakeFlow.NAME);
  public static final List<String> PROCEDURES = Lists.newArrayList(FakeProcedure.NAME);
  public static final List<String> MAPREDUCES = Lists.newArrayList();
  public static final List<String> WORKFLOWS = Lists.newArrayList();
  public static final List<String> SERVICES = Lists.newArrayList();
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
    addService(FakeService.NAME, new FakeService());
  }
}

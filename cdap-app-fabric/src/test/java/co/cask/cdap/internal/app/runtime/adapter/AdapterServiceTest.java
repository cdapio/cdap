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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.services.AdapterService;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.templates.AdapterDefinition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * AdapterService life cycle tests.
 */
public class AdapterServiceTest extends AppFabricTestBase {
  private static AdapterService adapterService;
  private static Store store;
  private static LocationFactory locationFactory;

  @BeforeClass
  public static void setup() throws Exception {
    adapterService = getInjector().getInstance(AdapterService.class);
    store = getInjector().getInstance(Store.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
  }

  @Test
  public void testUpgrade() throws Exception {
    // add app templates to the store
    Location dummyLocation = locationFactory.create(UUID.randomUUID().toString());
    dummyLocation.mkdirs();
    // manually add app spec for etl batch and realtime
    Map<String, WorkflowSpecification> workflowSpecs = ImmutableMap.of(
      "ETLWorkflow", new WorkflowSpecification(
        "co.cask.cdap.template.etl.batch.ETLWorkflow", "ETLWorkflow", "", Collections.<String, String>emptyMap(),
        ImmutableList.<WorkflowNode>of(
          new WorkflowActionNode("ETLMapReduce",
            new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, "ETLMapReduce")))
      ));
    Map<String, MapReduceSpecification> mrSpecs = ImmutableMap.of(
      "ETLMapReduce", new MapReduceSpecification(
        "co.cask.cdap.template.etl.batch.ETLMapReduce", "ETLMapReduce", "", null, null,
        Collections.<String>emptySet(), Collections.<String, String>emptyMap(), null, null)
    );
    Map<String, WorkerSpecification> workerSpecs = ImmutableMap.of(
      "ETLWorker", new WorkerSpecification(
        "co.cask.cdap.template.etl.realtime.ETLWorker", "ETLWorker", "", Collections.<String, String>emptyMap(),
        Collections.<String>emptySet(), null, 1)
    );
    ApplicationSpecification etlBatchSpec = new DefaultApplicationSpecification(
      "ETLBatch", "", "",
      null,
      Collections.<String, StreamSpecification>emptyMap(),
      Collections.<String, String>emptyMap(),
      Collections.<String, DatasetCreationSpec>emptyMap(),
      Collections.<String, FlowSpecification>emptyMap(),
      mrSpecs,
      Collections.<String, SparkSpecification>emptyMap(),
      workflowSpecs,
      Collections.<String, ServiceSpecification>emptyMap(),
      Collections.<String, ScheduleSpecification>emptyMap(),
      Collections.<String, WorkerSpecification>emptyMap(),
      Collections.<String, Plugin>emptyMap()
    );
    ApplicationSpecification etlRealtimeSpec = new DefaultApplicationSpecification(
      "ETLRealtime", "", "",
      null,
      Collections.<String, StreamSpecification>emptyMap(),
      Collections.<String, String>emptyMap(),
      Collections.<String, DatasetCreationSpec>emptyMap(),
      Collections.<String, FlowSpecification>emptyMap(),
      Collections.<String, MapReduceSpecification>emptyMap(),
      Collections.<String, SparkSpecification>emptyMap(),
      Collections.<String, WorkflowSpecification>emptyMap(),
      Collections.<String, ServiceSpecification>emptyMap(),
      Collections.<String, ScheduleSpecification>emptyMap(),
      workerSpecs,
      Collections.<String, Plugin>emptyMap()
    );
    Id.Application etlBatchId = Id.Application.from(Id.Namespace.DEFAULT, "ETLBatch");
    Id.Application etlRealtimeId = Id.Application.from(Id.Namespace.DEFAULT, "ETLRealtime");

    store.addApplication(etlBatchId, etlBatchSpec, dummyLocation);
    store.addApplication(etlRealtimeId, etlRealtimeSpec, dummyLocation);

    // add adapters to the store
    AdapterDefinition batchAdapter = AdapterDefinition.builder(
      "batch", Id.Program.from(Id.Namespace.DEFAULT, etlBatchId.getId(), ProgramType.WORKFLOW, "ETLWorkflow"))
      .setScheduleSpec(new ScheduleSpecification(
        Schedules.createTimeSchedule("name", "", "0 0 0 1 1"),
        new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, "ETLWorkflow"),
        Collections.<String, String>emptyMap()))
      .build();
    AdapterDefinition realtimeAdapter = AdapterDefinition.builder(
      "realtime", Id.Program.from(Id.Namespace.DEFAULT, etlRealtimeId.getId(), ProgramType.WORKER, "ETLWorker"))
      .setInstances(1)
      .build();
    store.addAdapter(Id.Namespace.DEFAULT, batchAdapter);
    store.addAdapter(Id.Namespace.DEFAULT, realtimeAdapter);

    // run upgrade
    adapterService.upgrade();

    // make sure apps are gone
    Assert.assertNull(store.getApplication(etlBatchId));
    Assert.assertNull(store.getApplication(etlRealtimeId));

    // make sure adapters are gone
    Assert.assertNull(store.getAdapter(Id.Namespace.DEFAULT, "batch"));
    Assert.assertNull(store.getAdapter(Id.Namespace.DEFAULT, "realtime"));
  }
}

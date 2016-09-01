/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.proto.codec;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link Id.NamespacedId}.
 */
public class NamespacedIdCodecTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();

  private final Id.Namespace ns = Id.Namespace.from("ns");
  private final Id.Application app = Id.Application.from(ns, "app");
  private final Id.Program program = Id.Program.from(app, ProgramType.CUSTOM_ACTION, "action");
  private final Id.Flow flow = Id.Flow.from(app, "flow");
  private final Id.Flow.Flowlet flowlet = Id.Flow.Flowlet.from(flow, "flowlet");
  private final Id.Service service = Id.Service.from(app, "service");
  private final Id.Schedule schedule = Id.Schedule.from(app, "schedule");
  private final Id.Worker worker = Id.Worker.from(app, "worker");
  private final Id.Workflow workflow = Id.Workflow.from(app, "workflow");
  private final Id.DatasetInstance dataset = Id.DatasetInstance.from(ns, "ds");
  private final Id.Stream stream = Id.Stream.from(ns, "stream");

  @Test
  public void testNamespacedIdCodec() {
    String nsJson = GSON.toJson(ns);
    Assert.assertEquals(ns, GSON.fromJson(nsJson, Id.Namespace.class));
    String appJson = GSON.toJson(app, Id.NamespacedId.class);
    Assert.assertEquals(app, GSON.fromJson(appJson, Id.NamespacedId.class));
    String programJson = GSON.toJson(program, Id.NamespacedId.class);
    Assert.assertEquals(program, GSON.fromJson(programJson, Id.NamespacedId.class));
    String flowJson = GSON.toJson(flow, Id.NamespacedId.class);
    Assert.assertEquals(flow, GSON.fromJson(flowJson, Id.NamespacedId.class));
    String flowletJson = GSON.toJson(flowlet, Id.NamespacedId.class);
    Assert.assertEquals(flowlet, GSON.fromJson(flowletJson, Id.NamespacedId.class));
    String serviceJson = GSON.toJson(service, Id.NamespacedId.class);
    Assert.assertEquals(service, GSON.fromJson(serviceJson, Id.NamespacedId.class));
    String scheduleJson = GSON.toJson(schedule, Id.NamespacedId.class);
    Assert.assertEquals(schedule, GSON.fromJson(scheduleJson, Id.NamespacedId.class));
    String workerJson = GSON.toJson(worker, Id.NamespacedId.class);
    Assert.assertEquals(worker, GSON.fromJson(workerJson, Id.NamespacedId.class));
    String workflowJson = GSON.toJson(workflow, Id.NamespacedId.class);
    Assert.assertEquals(workflow, GSON.fromJson(workflowJson, Id.NamespacedId.class));
    String datasetJson = GSON.toJson(dataset, Id.NamespacedId.class);
    Assert.assertEquals(dataset, GSON.fromJson(datasetJson, Id.NamespacedId.class));
    String streamJson = GSON.toJson(stream, Id.NamespacedId.class);
    Assert.assertEquals(stream, GSON.fromJson(streamJson, Id.NamespacedId.class));
  }

  @Test
  public void testWithMetadataRecord() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("k1", "v1");
    Set<String> tags = new LinkedHashSet<>();
    tags.add("tag1");
    tags.add("t1");
    // verify with Id.Application
    MetadataRecord appRecord = new MetadataRecord(app.toEntityId(), MetadataScope.USER, properties, tags);
    String appRecordJson = GSON.toJson(appRecord);
    Assert.assertEquals(appRecord, GSON.fromJson(appRecordJson, MetadataRecord.class));
    // verify with Id.Program
    MetadataRecord programRecord = new MetadataRecord(program.toEntityId(), MetadataScope.USER, properties, tags);
    String programRecordJson = GSON.toJson(programRecord);
    Assert.assertEquals(programRecord, GSON.fromJson(programRecordJson, MetadataRecord.class));
    // verify with Id.Flow
    MetadataRecord flowRecord = new MetadataRecord(flow.toEntityId(), MetadataScope.USER, properties, tags);
    String flowRecordJson = GSON.toJson(flowRecord);
    Assert.assertEquals(flowRecord, GSON.fromJson(flowRecordJson, MetadataRecord.class));
    // verify with Id.Flow.Flowlet
    MetadataRecord flowletRecord = new MetadataRecord(flowlet.toEntityId(), MetadataScope.USER, properties, tags);
    String flowletRecordJson = GSON.toJson(flowletRecord);
    Assert.assertEquals(flowletRecord, GSON.fromJson(flowletRecordJson, MetadataRecord.class));
    // verify with Id.Service
    MetadataRecord serviceRecord = new MetadataRecord(service.toEntityId(), MetadataScope.USER, properties, tags);
    String serviceRecordJson = GSON.toJson(serviceRecord);
    Assert.assertEquals(serviceRecord, GSON.fromJson(serviceRecordJson, MetadataRecord.class));
    // verify with Id.Schedule
    MetadataRecord scheduleRecord = new MetadataRecord(schedule.toEntityId(), MetadataScope.USER, properties, tags);
    String scheduleRecordJson = GSON.toJson(scheduleRecord);
    Assert.assertEquals(scheduleRecord, GSON.fromJson(scheduleRecordJson, MetadataRecord.class));
    // verify with Id.Worker
    MetadataRecord workerRecord = new MetadataRecord(worker.toEntityId(), MetadataScope.USER, properties, tags);
    String workerRecordJson = GSON.toJson(workerRecord);
    Assert.assertEquals(workerRecord, GSON.fromJson(workerRecordJson, MetadataRecord.class));
    // verify with Id.Workflow
    MetadataRecord workflowRecord = new MetadataRecord(workflow.toEntityId(), MetadataScope.USER, properties, tags);
    String workflowRecordJson = GSON.toJson(workflowRecord);
    Assert.assertEquals(workflowRecord, GSON.fromJson(workflowRecordJson, MetadataRecord.class));
    // verify with Id.DatasetInstance
    MetadataRecord datasetRecord = new MetadataRecord(dataset.toEntityId(), MetadataScope.USER, properties, tags);
    String datasetRecordJson = GSON.toJson(datasetRecord);
    Assert.assertEquals(datasetRecord, GSON.fromJson(datasetRecordJson, MetadataRecord.class));
    // verify with Id.Stream
    MetadataRecord streamRecord = new MetadataRecord(stream.toEntityId(), MetadataScope.USER, properties, tags);
    String streamRecordJson = GSON.toJson(streamRecord);
    Assert.assertEquals(streamRecord, GSON.fromJson(streamRecordJson, MetadataRecord.class));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupported() {
    Id.DatasetModule module = Id.DatasetModule.from(ns, "module");
    String moduleJson = GSON.toJson(module, Id.NamespacedId.class);
    GSON.fromJson(moduleJson, Id.NamespacedId.class);
  }
}

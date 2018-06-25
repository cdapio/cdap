/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.metadata;

import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.FlowId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.WorkerId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link MetadataRecordV2}
 */
public class MetadataRecordV2Test {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();

  private final NamespaceId ns = new NamespaceId("ns2");
  private final ApplicationId applicationId = new ApplicationId("ns2", "app2");
  private final ProgramId flowId1 = new ProgramId("ns3", "app4", ProgramType.FLOW, "flow5");
  private final FlowId flowId2 = new FlowId("ns3", "app4", "flow5");
  private final FlowletId flowletId = new FlowletId("ns3", "app4", "flow5", "flowlet2");
  private final ServiceId serviceId = new ServiceId(applicationId, "service");
  private final ScheduleId scheduleId = new ScheduleId("ns", "app", "schedule");
  private final WorkerId workerId = new WorkerId(applicationId, "worker");
  private final WorkflowId workflowId = new WorkflowId(applicationId, "wflow");
  private final DatasetId datasetId = new DatasetId("ns", "ds2");
  private final StreamId streamId = new StreamId("ns", "stream1");

  @Test
  public void testWithMetadataRecord() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("k1", "v1");
    Set<String> tags = new LinkedHashSet<>();
    tags.add("tag1");
    tags.add("t1");
    // verify with ApplicationId
    MetadataRecordV2 appRecord = new MetadataRecordV2(applicationId, MetadataScope.USER, properties, tags);
    String appRecordJson = GSON.toJson(appRecord);
    Assert.assertEquals(appRecord, GSON.fromJson(appRecordJson, MetadataRecordV2.class));
    // verify with ProgramId
    MetadataRecordV2 programRecord = new MetadataRecordV2(flowId1, MetadataScope.USER, properties, tags);
    String programRecordJson = GSON.toJson(programRecord);
    Assert.assertEquals(programRecord, GSON.fromJson(programRecordJson, MetadataRecordV2.class));
    // verify with FlowId
    MetadataRecordV2 flowRecord = new MetadataRecordV2(flowId2, MetadataScope.USER, properties, tags);
    String flowRecordJson = GSON.toJson(flowRecord);
    Assert.assertEquals(flowRecord, GSON.fromJson(flowRecordJson, MetadataRecordV2.class));
    // verify with FlowletId
    MetadataRecordV2 flowletRecord = new MetadataRecordV2(flowletId, MetadataScope.USER, properties, tags);
    String flowletRecordJson = GSON.toJson(flowletRecord);
    Assert.assertEquals(flowletRecord, GSON.fromJson(flowletRecordJson, MetadataRecordV2.class));
    // verify with Id.Service
    MetadataRecordV2 serviceRecord = new MetadataRecordV2(serviceId, MetadataScope.USER, properties, tags);
    String serviceRecordJson = GSON.toJson(serviceRecord);
    Assert.assertEquals(serviceRecord, GSON.fromJson(serviceRecordJson, MetadataRecordV2.class));
    // verify with Id.Schedule
    MetadataRecordV2 scheduleRecord = new MetadataRecordV2(scheduleId, MetadataScope.USER, properties, tags);
    String scheduleRecordJson = GSON.toJson(scheduleRecord);
    Assert.assertEquals(scheduleRecord, GSON.fromJson(scheduleRecordJson, MetadataRecordV2.class));
    // verify with Id.Worker
    MetadataRecordV2 workerRecord = new MetadataRecordV2(workerId, MetadataScope.USER, properties, tags);
    String workerRecordJson = GSON.toJson(workerRecord);
    Assert.assertEquals(workerRecord, GSON.fromJson(workerRecordJson, MetadataRecordV2.class));
    // verify with Id.Workflow
    MetadataRecordV2 workflowRecord = new MetadataRecordV2(workflowId, MetadataScope.USER, properties, tags);
    String workflowRecordJson = GSON.toJson(workflowRecord);
    Assert.assertEquals(workflowRecord, GSON.fromJson(workflowRecordJson, MetadataRecordV2.class));
    // verify with Id.DatasetInstance
    MetadataRecordV2 datasetRecord = new MetadataRecordV2(datasetId, MetadataScope.USER, properties, tags);
    String datasetRecordJson = GSON.toJson(datasetRecord);
    Assert.assertEquals(datasetRecord, GSON.fromJson(datasetRecordJson, MetadataRecordV2.class));
    // verify with Id.Stream
    MetadataRecordV2 streamRecord = new MetadataRecordV2(streamId, MetadataScope.USER, properties, tags);
    String streamRecordJson = GSON.toJson(streamRecord);
    Assert.assertEquals(streamRecord, GSON.fromJson(streamRecordJson, MetadataRecordV2.class));
  }
}

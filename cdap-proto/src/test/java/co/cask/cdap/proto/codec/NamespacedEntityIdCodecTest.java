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

package co.cask.cdap.proto.codec;

import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.FlowId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.id.WorkerId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

public class NamespacedEntityIdCodecTest {
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
  private final ArtifactId artifactId = new ArtifactId("ns", "artifact", "1.0-SNAPSHOT");
  private final DatasetId datasetId = new DatasetId("ns", "ds2");
  private final StreamId streamId = new StreamId("ns", "stream1");
  private final StreamViewId viewId = new StreamViewId("ns", "stream1", "view1");

  @Test
  public void testNamespacedIdCodec() {
    String nsJson = GSON.toJson(ns);
    Assert.assertEquals(ns, GSON.fromJson(nsJson, NamespacedEntityId.class));

    String appJson2 = GSON.toJson(applicationId);
    Assert.assertEquals(applicationId, GSON.fromJson(appJson2, NamespacedEntityId.class));

    String flowJson2 = GSON.toJson(flowId2);
    Assert.assertEquals(flowId2, GSON.fromJson(flowJson2, NamespacedEntityId.class));

    String programFlow = GSON.toJson(flowId1);
    Assert.assertEquals(flowId1, GSON.fromJson(programFlow, NamespacedEntityId.class));

    String flowlet = GSON.toJson(flowletId);
    Assert.assertEquals(flowletId, GSON.fromJson(flowlet, NamespacedEntityId.class));

    String service = GSON.toJson(serviceId);
    Assert.assertEquals(serviceId, GSON.fromJson(service, NamespacedEntityId.class));

    String schedule = GSON.toJson(scheduleId);
    Assert.assertEquals(scheduleId, GSON.fromJson(schedule, NamespacedEntityId.class));

    String worker = GSON.toJson(workerId);
    Assert.assertEquals(workerId, GSON.fromJson(worker, NamespacedEntityId.class));

    String wflow = GSON.toJson(workflowId);
    Assert.assertEquals(workflowId, GSON.fromJson(wflow, NamespacedEntityId.class));

    String artifact = GSON.toJson(artifactId);
    Assert.assertEquals(artifactId, GSON.fromJson(artifact, NamespacedEntityId.class));

    String dataset = GSON.toJson(datasetId);
    Assert.assertEquals(datasetId, GSON.fromJson(dataset, NamespacedEntityId.class));

    String stream = GSON.toJson(streamId);
    Assert.assertEquals(streamId, GSON.fromJson(stream, NamespacedEntityId.class));

    String view = GSON.toJson(viewId);
    Assert.assertEquals(viewId, GSON.fromJson(view, NamespacedEntityId.class));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupported() {
    DatasetModuleId module = new DatasetModuleId("ns", "module");
    String moduleJson = GSON.toJson(module, NamespacedEntityId.class);
    GSON.fromJson(moduleJson, NamespacedEntityId.class);
  }
}

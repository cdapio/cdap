/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.proto.codec;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.proto.id.WorkerId;
import io.cdap.cdap.proto.id.WorkflowId;
import org.junit.Assert;
import org.junit.Test;

public class NamespacedEntityIdCodecTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
    .create();

  private final NamespaceId ns = new NamespaceId("ns2");
  private final ApplicationId applicationId = new ApplicationId("ns2", "app2");
  private final ProgramId program1 = new ProgramId("ns3", "app4", ProgramType.WORKER, "worker5");
  private final ServiceId serviceId = new ServiceId(applicationId, "service");
  private final ScheduleId scheduleId = new ScheduleId("ns", "app", "schedule");
  private final WorkerId workerId = new WorkerId(applicationId, "worker");
  private final WorkflowId workflowId = new WorkflowId(applicationId, "wflow");
  private final ArtifactId artifactId = new ArtifactId("ns", "artifact", "1.0-SNAPSHOT");
  private final DatasetId datasetId = new DatasetId("ns", "ds2");

  @Test
  public void testNamespacedIdCodec() {
    String nsJson = GSON.toJson(ns);
    Assert.assertEquals(ns, GSON.fromJson(nsJson, NamespacedEntityId.class));

    String appJson2 = GSON.toJson(applicationId);
    Assert.assertEquals(applicationId, GSON.fromJson(appJson2, NamespacedEntityId.class));

    String programFlow = GSON.toJson(program1);
    Assert.assertEquals(program1, GSON.fromJson(programFlow, NamespacedEntityId.class));

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
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupported() {
    DatasetModuleId module = new DatasetModuleId("ns", "module");
    String moduleJson = GSON.toJson(module, NamespacedEntityId.class);
    GSON.fromJson(moduleJson, NamespacedEntityId.class);
  }
}

/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link OperationsDashboardHttpHandler}
 */
public class OperationsDashboardHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final String BASE_PATH = Constants.Gateway.API_VERSION_3;
  private static final Type DASHBOARD_DETAIL_TYPE = new TypeToken<List<DashboardProgramRunRecord>>() { }.getType();
  private static Store store;

  @BeforeClass
  public static void setup() throws Exception {
    store = getInjector().getInstance(DefaultStore.class);
  }

  @Test
  public void testDashboardDetail() throws Exception {
    ProgramId programId1 = new ProgramId("ns1", "app", ProgramType.WORKFLOW, "program");
    ArtifactId artifactId = new ArtifactId("ns1", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    long sourceId = 0;
    // run1
    ProgramRunId runId1 = programId1.run(RunIds.generate(TimeUnit.SECONDS.toMillis(10)));
    store.setProvisioning(runId1, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStop(runId1, 50, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));
    // run2
    ProgramRunId runId2 = programId1.run(RunIds.generate(TimeUnit.SECONDS.toMillis(60)));
    store.setProvisioning(runId2, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStop(runId2, 110, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));
    ProgramId programId2 = new ProgramId("ns2", "app", ProgramType.WORKFLOW, "program");
    // run3
    ProgramRunId runId3 = programId2.run(RunIds.generate(TimeUnit.SECONDS.toMillis(120)));
    store.setProvisioning(runId3, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStop(runId3, 200, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));
    // run4
    ProgramRunId runId4 = programId2.run(RunIds.generate(TimeUnit.SECONDS.toMillis(2000)));
    store.setProvisioning(runId4, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStop(runId4, 2200, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));

    ProgramId programId3 = new ProgramId("ns3", "app", ProgramType.WORKFLOW, "program");
    // run5
    ProgramRunId runId5 = programId3.run(RunIds.generate(TimeUnit.SECONDS.toMillis(120)));
    store.setProvisioning(runId5, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStop(runId5, 200, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));
    HttpResponse response = doGet(BASE_PATH + "/dashboard?start=100&duration=1440&namespace=ns1&namespace=ns2");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    List<DashboardProgramRunRecord> dashboardDetail = GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
    Assert.assertEquals(2, dashboardDetail.size());
  }
}

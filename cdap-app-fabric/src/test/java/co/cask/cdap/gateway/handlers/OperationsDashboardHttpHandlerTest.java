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
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    // a path to get ops dashboard results between time 100 and 100 + 1440 = 1540 from namesapce "ns1" and "ns2"
    String opsDashboardQueryPath = BASE_PATH + "/dashboard?start=100&duration=1440&namespace=ns1&namespace=ns2";
    // run1 will not be included in the query results since it stops before the query start time 100
    ProgramRunId run1 = programId1.run(RunIds.generate(TimeUnit.SECONDS.toMillis(10)));
    store.setProvisioning(run1, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStop(run1, 50, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));
    // run2 will be included in the query results since it starts before query end time
    // and stops after query start time
    ProgramRunId run2 = programId1.run(RunIds.generate(TimeUnit.SECONDS.toMillis(60)));
    store.setProvisioning(run2, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStop(run2, 110, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));
    ProgramId programId2 = new ProgramId("ns2", "app", ProgramType.WORKFLOW, "program");
    // run3 will be included in the query results since it starts before query end time
    // and stops after query start time
    ProgramRunId run3 = programId2.run(RunIds.generate(TimeUnit.SECONDS.toMillis(120)));
    store.setProvisioning(run3, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStop(run3, 200, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));
    // run4 will be included in the query results since it starts before query end time
    // and stops after query start time
    ProgramRunId run4 = programId2.run(RunIds.generate(TimeUnit.SECONDS.toMillis(60)));
    store.setProvisioning(run4, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStop(run4, 2200, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));
    // run5 will be included in the query results since it starts before query end time
    // and stops after query start time
    ProgramRunId run5 = programId2.run(RunIds.generate(TimeUnit.SECONDS.toMillis(60)));
    store.setProvisioning(run5, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStart(run5, "twillId", Collections.emptyMap(), Bytes.toBytes(++sourceId));
    // run6 will not be included in the query results since it starts after query end time
    ProgramRunId run6 = programId2.run(RunIds.generate(TimeUnit.SECONDS.toMillis(2000)));
    store.setProvisioning(run6, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStop(run6, 2200, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));

    ProgramId programId3 = new ProgramId("ns3", "app", ProgramType.WORKFLOW, "program");
    // run7 will not be included in the query results since it does not belong the namespaces in the query
    ProgramRunId run7 = programId3.run(RunIds.generate(TimeUnit.SECONDS.toMillis(120)));
    store.setProvisioning(run7, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setStop(run7, 200, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));
    // get ops dashboard query results
    HttpResponse response = doGet(opsDashboardQueryPath);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    List<DashboardProgramRunRecord> dashboardDetail = GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
    // assert the result contains 4 entries and run2, run3, run4, and run5 are there
    Assert.assertEquals(4, dashboardDetail.size());
    Set<String> runs = dashboardDetail.stream().map(DashboardProgramRunRecord::getRun).collect(Collectors.toSet());
    Assert.assertEquals(ImmutableSet.of(run2.getRun(), run3.getRun(), run4.getRun(), run5.getRun()), runs);
  }
}

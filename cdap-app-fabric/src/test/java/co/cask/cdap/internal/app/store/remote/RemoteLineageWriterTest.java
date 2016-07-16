/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store.remote;

import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.LineageStore;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Tests implementation of {@link RemoteLineageWriter}, by using it to perform writes/updates, and then using a
 * local {@link LineageStore} to verify the updates/writes.
 */
public class RemoteLineageWriterTest extends AppFabricTestBase {

  private static LineageStore lineageStore;
  private static RemoteLineageWriter remoteLineageWriter;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = getInjector();
    lineageStore = injector.getInstance(LineageStore.class);
    remoteLineageWriter = injector.getInstance(RemoteLineageWriter.class);
  }

  @Test
  public void testSimpleCase() {
    long now = System.currentTimeMillis();
    ApplicationId appId = NamespaceId.DEFAULT.app("test_app");
    ProgramId flowId = appId.flow("test_flow");
    ProgramRunId runId = flowId.run(RunIds.generate(now).getId());
    RunId twillRunId = RunIds.fromString(runId.getRun());
    DatasetId datasetId = NamespaceId.DEFAULT.dataset("test_dataset");
    StreamId streamId = NamespaceId.DEFAULT.stream("test_stream");

    Set<Relation> expectedRelations = new HashSet<>();

    // test null serialization
    remoteLineageWriter.addAccess(runId.toId(), datasetId.toId(), AccessType.READ, null);
    expectedRelations.add(new Relation(datasetId.toId(), flowId.toId(), AccessType.READ, twillRunId));

    Assert.assertEquals(ImmutableSet.of(flowId.toId(), datasetId.toId()), lineageStore.getEntitiesForRun(runId.toId()));

    Assert.assertEquals(expectedRelations,
                        lineageStore.getRelations(flowId.toId(), now, now + 1, Predicates.<Relation>alwaysTrue()));

    remoteLineageWriter.addAccess(runId.toId(), streamId.toId(), AccessType.READ);
    expectedRelations.add(new Relation(streamId.toId(), flowId.toId(), AccessType.READ, twillRunId));

    Assert.assertEquals(expectedRelations,
                        lineageStore.getRelations(flowId.toId(), now, now + 1, Predicates.<Relation>alwaysTrue()));

    remoteLineageWriter.addAccess(runId.toId(), streamId.toId(), AccessType.WRITE);
    expectedRelations.add(new Relation(streamId.toId(), flowId.toId(), AccessType.WRITE, twillRunId));

    Assert.assertEquals(expectedRelations,
                        lineageStore.getRelations(flowId.toId(), now, now + 1, Predicates.<Relation>alwaysTrue()));
  }
}

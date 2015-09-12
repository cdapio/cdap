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

package co.cask.cdap.metadata;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.metadata.serialize.LineageProto;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.SlowTests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests lineage recording and query.
 */
@Category(SlowTests.class)
public class LineageTest extends MetadataTestBase {
  private static final Gson GSON = new Gson();
  private static final String STOPPED = "STOPPED";

  @Test
  public void testLineage() throws Exception {
    final Id.Application app = Id.Application.from("default", AllProgramsApp.NAME);
    final Id.Flow flow = Id.Flow.from(app, AllProgramsApp.NoOpFlow.NAME);
    final Id.Stream stream = Id.Stream.from("default", AllProgramsApp.STREAM_NAME);
    final Id.DatasetInstance dataset = Id.DatasetInstance.from("default", AllProgramsApp.DATASET_NAME);

    deploy(AllProgramsApp.class);
    try {
      addProperties(dataset, ImmutableMap.of("data-key1", "data-value1"));
      Assert.assertEquals(ImmutableMap.of("data-key1", "data-value1"), getProperties(dataset));

      startProgram(flow);
      waitState(flow, ProgramRunStatus.RUNNING.toString());
      RunId flowRunId = getRunId(flow);
      stopProgram(flow);
      waitState(flow, STOPPED);

      long now = System.currentTimeMillis();
      long oneHourMillis = TimeUnit.HOURS.toMillis(1);
      HttpResponse httpResponse = fetchLineage(dataset, now - oneHourMillis, now + oneHourMillis, 10);
      LineageProto lineage = GSON.fromJson(httpResponse.getResponseBodyAsString(), LineageProto.class);

      LineageProto expected =
        new LineageProto(now - oneHourMillis, now + oneHourMillis,
                         ImmutableSet.of(
                           new Relation(dataset, flow, AccessType.UNKNOWN,
                                        ImmutableSet.of(flowRunId),
                                        ImmutableSet.of(Id.Flow.Flowlet.from(flow, AllProgramsApp.A.NAME)))
                         ));
      Assert.assertEquals(expected, lineage);
    } finally {
      deleteApp(app, 200);
    }
  }

  private RunId getRunId(Id.Program program) throws Exception {
    List<RunRecord> runRecords = getProgramRuns(program, "RUNNING");
    Assert.assertEquals(1, runRecords.size());
    return RunIds.fromString(runRecords.iterator().next().getPid());
  }
}

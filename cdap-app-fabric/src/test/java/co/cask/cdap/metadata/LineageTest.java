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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.metadata.serialize.LineageRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.SlowTests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Tests lineage recording and query.
 */
@Category(SlowTests.class)
public class LineageTest extends MetadataTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(LineageTest.class);

  private static final Gson GSON = new Gson();
  private static final String STOPPED = "STOPPED";

  @Test
  public void testFlowLineage() throws Exception {
    String namespace = "testFlowLineage";
    Id.Application app = Id.Application.from(namespace, AllProgramsApp.NAME);
    Id.Flow flow = Id.Flow.from(app, AllProgramsApp.NoOpFlow.NAME);
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespace, AllProgramsApp.DATASET_NAME);
    Id.Stream stream = Id.Stream.from(namespace, AllProgramsApp.STREAM_NAME);

    Assert.assertEquals(200, status(createNamespace(namespace)));
    try {
      Assert.assertEquals(200,
                          status(deploy(AllProgramsApp.class, Constants.Gateway.API_VERSION_3_TOKEN, namespace)));
      Assert.assertEquals(200, addProperties(dataset, ImmutableMap.of("data-key1", "data-value1")).getResponseCode());
      Assert.assertEquals(ImmutableMap.of("data-key1", "data-value1"), getProperties(dataset));

      RunId flowRunId = runAndWait(flow);
      waitForStop(flow, true);

      long now = System.currentTimeMillis();
      long oneHourMillis = TimeUnit.HOURS.toMillis(1);

      // Fetch dataset lineage
      HttpResponse httpResponse = fetchLineage(dataset, now - oneHourMillis, now + oneHourMillis, 10);
      Assert.assertEquals(200, httpResponse.getResponseCode());
      LineageRecord lineage = GSON.fromJson(httpResponse.getResponseBodyAsString(), LineageRecord.class);

      LineageRecord expected =
        new LineageRecord(now - oneHourMillis, now + oneHourMillis,
                         ImmutableSet.of(
                           new Relation(dataset, flow, AccessType.UNKNOWN,
                                        ImmutableSet.of(flowRunId),
                                        ImmutableSet.of(Id.Flow.Flowlet.from(flow, AllProgramsApp.A.NAME))),
                           new Relation(stream, flow, AccessType.READ,
                                        ImmutableSet.of(flowRunId),
                                        ImmutableSet.of(Id.Flow.Flowlet.from(flow, AllProgramsApp.A.NAME)))
                         ));
      Assert.assertEquals(expected, lineage);

      // Fetch stream lineage
      httpResponse = fetchLineage(stream, now - oneHourMillis, now + oneHourMillis, 10);
      Assert.assertEquals(200, httpResponse.getResponseCode());
      lineage = GSON.fromJson(httpResponse.getResponseBodyAsString(), LineageRecord.class);

      // same as dataset's lineage
      Assert.assertEquals(expected, lineage);
    } finally {
      try {
        deleteNamespace(namespace);
      } catch (Throwable e) {
        LOG.error("Got exception while deleting namespace {}", namespace, e);
      }
    }
  }

  @Test
  public void testAllProgramsLineage() throws Exception {
    String namespace = "testAllProgramsLineage";
    Id.Application app = Id.Application.from(namespace, AllProgramsApp.NAME);
    Id.Flow flow = Id.Flow.from(app, AllProgramsApp.NoOpFlow.NAME);
    Id.Program mapreduce = Id.Program.from(app, ProgramType.MAPREDUCE, AllProgramsApp.NoOpMR.NAME);
    Id.Program spark = Id.Program.from(app, ProgramType.SPARK, AllProgramsApp.NoOpSpark.NAME);
    Id.Program service = Id.Program.from(app, ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    Id.Program worker = Id.Program.from(app, ProgramType.WORKER, AllProgramsApp.NoOpWorker.NAME);
    Id.Program workflow = Id.Program.from(app, ProgramType.WORKFLOW, AllProgramsApp.NoOpWorkflow.NAME);
    Id.DatasetInstance dataset = Id.DatasetInstance.from(namespace, AllProgramsApp.DATASET_NAME);
    Id.Stream stream = Id.Stream.from(namespace, AllProgramsApp.STREAM_NAME);

    Assert.assertEquals(200, status(createNamespace(namespace)));
    try {
      Assert.assertEquals(200,
                          status(deploy(AllProgramsApp.class, Constants.Gateway.API_VERSION_3_TOKEN, namespace)));
      addProperties(dataset, ImmutableMap.of("data-key1", "data-value1"));
      Assert.assertEquals(ImmutableMap.of("data-key1", "data-value1"), getProperties(dataset));

      // Start all programs
      RunId flowRunId = runAndWait(flow);
      RunId mrRunId = runAndWait(mapreduce);
      RunId sparkRunId = runAndWait(spark);
      runAndWait(workflow);
      RunId workflowMrRunId = getRunId(mapreduce, mrRunId);
      RunId serviceRunId = runAndWait(service);
      // Worker makes a call to service to make it access datasets,
      // hence need to make sure service starts before worker, and stops after it.
      RunId workerRunId = runAndWait(worker);

      // Wait for programs to finish
      waitForStop(flow, true);
      waitForStop(mapreduce, false);
      waitForStop(spark, false);
      waitForStop(workflow, false);
      waitForStop(worker, false);
      waitForStop(service, true);

      long now = System.currentTimeMillis();
      long oneHourMillis = TimeUnit.HOURS.toMillis(1);

      // Fetch dataset lineage
      HttpResponse httpResponse = fetchLineage(dataset, now - oneHourMillis, now + oneHourMillis, 10);
      Assert.assertEquals(200, httpResponse.getResponseCode());
      LineageRecord lineage = GSON.fromJson(httpResponse.getResponseBodyAsString(), LineageRecord.class);

      // dataset is accessed by all programs
      LineageRecord expected =
        new LineageRecord(now - oneHourMillis, now + oneHourMillis,
                          ImmutableSet.of(
                            // Dataset access
                            new Relation(dataset, flow, AccessType.UNKNOWN, ImmutableSet.of(flowRunId),
                                         ImmutableSet.of(Id.Flow.Flowlet.from(flow, AllProgramsApp.A.NAME))),
                            new Relation(dataset, mapreduce, AccessType.UNKNOWN, ImmutableSet.of(mrRunId)),
                            new Relation(dataset, spark, AccessType.UNKNOWN, ImmutableSet.of(sparkRunId)),
                            new Relation(dataset, mapreduce, AccessType.UNKNOWN, ImmutableSet.of(workflowMrRunId)),
                            new Relation(dataset, service, AccessType.UNKNOWN, ImmutableSet.of(serviceRunId)),
                            new Relation(dataset, worker, AccessType.UNKNOWN, ImmutableSet.of(workerRunId)),

                            // Stream access
                            new Relation(stream, flow, AccessType.READ, ImmutableSet.of(flowRunId),
                                         ImmutableSet.of(Id.Flow.Flowlet.from(flow, AllProgramsApp.A.NAME))),
                            new Relation(stream, mapreduce, AccessType.READ, ImmutableSet.of(mrRunId)),
                            new Relation(stream, spark, AccessType.READ, ImmutableSet.of(sparkRunId)),
                            new Relation(stream, mapreduce, AccessType.READ, ImmutableSet.of(workflowMrRunId)),
                            new Relation(stream, worker, AccessType.WRITE, ImmutableSet.of(workerRunId))
                          ));
      Assert.assertEquals(expected, lineage);

      // Fetch stream lineage
      httpResponse = fetchLineage(stream, now - oneHourMillis, now + oneHourMillis, 10);
      Assert.assertEquals(200, httpResponse.getResponseCode());
      lineage = GSON.fromJson(httpResponse.getResponseBodyAsString(), LineageRecord.class);

      // stream too is accessed by all programs
      Assert.assertEquals(expected, lineage);
    } finally {
      try {
        deleteNamespace(namespace);
      } catch (Throwable e) {
        LOG.error("Got exception while deleting namespace {}", namespace, e);
      }
    }
  }

  private RunId runAndWait(Id.Program program) throws Exception {
    LOG.info("Starting program {}", program);
    startProgram(program);
    waitState(program, ProgramRunStatus.RUNNING.toString());
    return getRunId(program);
  }

  private void waitForStop(Id.Program program, boolean needsStop) throws Exception {
    if (needsStop && getProgramStatus(program).equals(ProgramRunStatus.RUNNING.toString())) {
      LOG.info("Stopping program {}", program);
      stopProgram(program);
    }
    waitState(program, STOPPED);
    LOG.info("Program {} has stopped", program);
  }

  private RunId getRunId(Id.Program program) throws Exception {
    return getRunId(program, null);
  }

  private RunId getRunId(final Id.Program program, @Nullable final RunId exclude) throws Exception {
    final AtomicReference<Iterable<RunRecord>> runRecords = new AtomicReference<>();
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        runRecords.set(Iterables.filter(getProgramRuns(program, "RUNNING"),
                                        new Predicate<RunRecord>() {
                                          @Override
                                          public boolean apply(RunRecord input) {
                                            return exclude == null || !input.getPid().equals(exclude.getId());
                                          }
                                        }));
        return Iterables.size(runRecords.get());
      }
    }, 60, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
    Assert.assertEquals(1, Iterables.size(runRecords.get()));
    return RunIds.fromString(Iterables.getFirst(runRecords.get(), null).getPid());
  }

  private int status(org.apache.http.HttpResponse response) {
    return response.getStatusLine().getStatusCode();
  }
}

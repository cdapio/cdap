/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.client.app.AllProgramsApp;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.Lineage;
import io.cdap.cdap.data2.metadata.lineage.LineageSerializer;
import io.cdap.cdap.data2.metadata.lineage.Relation;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.metadata.lineage.CollapseType;
import io.cdap.cdap.proto.metadata.lineage.LineageRecord;
import io.cdap.cdap.test.SlowTests;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Tests lineage recording and query.
 */
@Category(SlowTests.class)
public class LineageHttpHandlerTestRun extends MetadataTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(LineageHttpHandlerTestRun.class);

  @Test
  public void testInvalidFieldLineageResponse() throws Exception {
    DatasetId datasetId = new DatasetId("ns1", "test");
    // time cannot be negative
    fetchFieldLineage(datasetId, -100, 200, "both", BadRequestException.class);
    fetchFieldLineage(datasetId, 100, -200, "both", BadRequestException.class);
    // start must less than end
    fetchFieldLineage(datasetId, 200, 100, "both", BadRequestException.class);
    // direction must be one of incoming, outgoing or both, and cannot be null
    fetchFieldLineage(datasetId, 100, 200, "random", BadRequestException.class);
    fetchFieldLineage(datasetId, 100, 200, null, BadRequestException.class);
  }

  @Test
  public void testAllProgramsLineage() throws Exception {
    NamespaceId namespace = new NamespaceId("testAllProgramsLineage");
    ApplicationId app = namespace.app(AllProgramsApp.NAME);
    ProgramId mapreduce = app.mr(AllProgramsApp.NoOpMR.NAME);
    ProgramId mapreduce2 = app.mr(AllProgramsApp.NoOpMR2.NAME);
    ProgramId spark = app.spark(AllProgramsApp.NoOpSpark.NAME);
    ProgramId service = app.service(AllProgramsApp.NoOpService.NAME);
    ProgramId worker = app.worker(AllProgramsApp.NoOpWorker.NAME);
    ProgramId workflow = app.workflow(AllProgramsApp.NoOpWorkflow.NAME);
    DatasetId dataset = namespace.dataset(AllProgramsApp.DATASET_NAME);
    DatasetId dataset2 = namespace.dataset(AllProgramsApp.DATASET_NAME2);
    DatasetId dataset3 = namespace.dataset(AllProgramsApp.DATASET_NAME3);

    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace.getNamespace()).build());
    try {
      appClient.deploy(namespace, createAppJarFile(AllProgramsApp.class));

      // Add metadata
      ImmutableSet<String> sparkTags = ImmutableSet.of("spark-tag1", "spark-tag2");
      addTags(spark, sparkTags);
      Assert.assertEquals(sparkTags, getTags(spark, MetadataScope.USER));

      ImmutableSet<String> workerTags = ImmutableSet.of("worker-tag1");
      addTags(worker, workerTags);
      Assert.assertEquals(workerTags, getTags(worker, MetadataScope.USER));

      ImmutableMap<String, String> datasetProperties = ImmutableMap.of("data-key1", "data-value1");
      addProperties(dataset, datasetProperties);
      Assert.assertEquals(datasetProperties, getProperties(dataset, MetadataScope.USER));

      // Start all programs
      RunId mrRunId = runAndWait(mapreduce);
      RunId mrRunId2 = runAndWait(mapreduce2);
      RunId sparkRunId = runAndWait(spark);
      runAndWait(workflow);
      RunId workflowMrRunId = getRunId(mapreduce, mrRunId);
      RunId serviceRunId = runAndWait(service);
      // Worker makes a call to service to make it access datasets,
      // hence need to make sure service starts before worker, and stops after it.
      RunId workerRunId = runAndWait(worker);

      // Wait for programs to finish
      waitForStop(mapreduce, false);
      waitForStop(mapreduce2, false);
      waitForStop(spark, false);
      waitForStop(workflow, false);
      waitForStop(worker, false);
      waitForStop(service, true);

      long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
      long oneHour = TimeUnit.HOURS.toSeconds(1);

      // Fetch dataset lineage
      LineageRecord lineage = fetchLineage(dataset, now - oneHour, now + oneHour, toSet(CollapseType.ACCESS), 10);

      // dataset is accessed by all programs
      LineageRecord expected =
        LineageSerializer.toLineageRecord(
          now - oneHour,
          now + oneHour,
          new Lineage(ImmutableSet.of(
            // Dataset access
            new Relation(dataset, mapreduce, AccessType.WRITE, mrRunId),
            new Relation(dataset3, mapreduce, AccessType.READ, mrRunId),
            new Relation(dataset, mapreduce2, AccessType.WRITE, mrRunId2),
            new Relation(dataset2, mapreduce2, AccessType.READ, mrRunId2),
            new Relation(dataset, spark, AccessType.READ, sparkRunId),
            new Relation(dataset2, spark, AccessType.WRITE, sparkRunId),
            new Relation(dataset3, spark, AccessType.READ, sparkRunId),
            new Relation(dataset3, spark, AccessType.WRITE, sparkRunId),
            new Relation(dataset, mapreduce, AccessType.WRITE, workflowMrRunId),
            new Relation(dataset3, mapreduce, AccessType.READ, workflowMrRunId),
            new Relation(dataset, service, AccessType.WRITE, serviceRunId),
            new Relation(dataset, worker, AccessType.WRITE, workerRunId)
          )),
          toSet(CollapseType.ACCESS));
      Assert.assertEquals(expected, lineage);

    } finally {
      namespaceClient.delete(namespace);
    }
  }

  @Test
  public void testLineageForNonExistingEntity() {
    DatasetId datasetInstance = NamespaceId.DEFAULT.dataset("dummy");
    fetchLineage(datasetInstance, -100, 200, 10, BadRequestException.class);
    fetchLineage(datasetInstance, 100, -200, 10, BadRequestException.class);
    fetchLineage(datasetInstance, 200, 100, 10, BadRequestException.class);
    fetchLineage(datasetInstance, 100, 200, -10, BadRequestException.class);
  }

  private RunId runAndWait(final ProgramId program) throws Exception {
    LOG.info("Starting program {}", program);
    programClient.start(program);
    return getRunId(program);
  }

  private void waitForStop(ProgramId program, boolean needsStop) throws Exception {
    if (needsStop && !programClient.getStatus(program).equals(ProgramStatus.STOPPED.toString())) {
      LOG.info("Stopping program {}", program);
      programClient.stop(program);
    }
    programClient.waitForStatus(program, ProgramStatus.STOPPED, 15, TimeUnit.SECONDS);
    LOG.info("Program {} has stopped", program);
  }

  private RunId getRunId(ProgramId program) throws Exception {
    return getRunId(program, null);
  }

  private RunId getRunId(final ProgramId program, @Nullable final RunId exclude) throws Exception {
    AtomicReference<Iterable<RunRecord>> runRecords = new AtomicReference<>();
    Tasks.waitFor(1, () -> {
      runRecords.set(
        programClient.getProgramRuns(program, ProgramRunStatus.ALL.name(), 0, Long.MAX_VALUE, Integer.MAX_VALUE)
          .stream()
          .filter(record -> (exclude == null || !record.getPid().equals(exclude.getId())) &&
            record.getStatus() != ProgramRunStatus.PENDING)
          .collect(Collectors.toList()));
      return Iterables.size(runRecords.get());
    }, 60, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
    Assert.assertEquals(1, Iterables.size(runRecords.get()));
    return RunIds.fromString(Iterables.getFirst(runRecords.get(), null).getPid());
  }

  @SafeVarargs
  private static <T> Set<T> toSet(T... elements) {
    return ImmutableSet.copyOf(elements);
  }
}

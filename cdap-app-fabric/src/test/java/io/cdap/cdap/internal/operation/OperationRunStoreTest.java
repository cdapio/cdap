/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operation;

import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsRequest;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationMeta;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class OperationRunStoreTest {

  protected static TransactionRunner transactionRunner;
  private final AtomicInteger sourceId = new AtomicInteger();
  private final AtomicLong runIdTime = new AtomicLong(System.currentTimeMillis());
  private final String testNamespace = "test";

  private final PullAppsRequest input = new PullAppsRequest(Collections.emptySet());


  @Before
  public void before() throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore operationRunsStore = new OperationRunStore(context);
      operationRunsStore.clearData();
    });
  }

  @Test
  public void testGetOperation() throws Exception {
    OperationRunDetail expectedDetail = insertRun(testNamespace, "LIST",
        OperationRunStatus.RUNNING);
    String testId = expectedDetail.getRun().getId();
    OperationRunId runId = new OperationRunId(testNamespace, testId);

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore store = new OperationRunStore(context);
      OperationRunDetail gotDetail = store.getOperation(runId);
      Assert.assertEquals(expectedDetail, gotDetail);
      try {
        store.getOperation(new OperationRunId(Namespace.DEFAULT.getId(), testId));
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testUpdateMetadata() throws Exception {
    OperationRunDetail expectedDetail = insertRun(testNamespace, "LIST",
        OperationRunStatus.RUNNING);
    String testId = expectedDetail.getRun().getId();
    OperationRunId runId = new OperationRunId(testNamespace, testId);

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore store = new OperationRunStore(context);
      OperationRunDetail gotDetail = store.getOperation(runId);
      Assert.assertEquals(expectedDetail, gotDetail);

      OperationMeta updatedMeta = new OperationMeta(Collections.emptySet(),
          Instant.ofEpochMilli(runIdTime.incrementAndGet()),
          Instant.ofEpochMilli(runIdTime.incrementAndGet()));
      OperationRun updatedRun = OperationRun.builder(expectedDetail.getRun())
          .setMetadata(updatedMeta).build();
      OperationRunDetail updatedDetail = OperationRunDetail.builder(expectedDetail)
          .setRun(updatedRun)
          .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
          .build();
      store.updateOperationMeta(runId, updatedMeta, updatedDetail.getSourceId());
      gotDetail = store.getOperation(runId);
      Assert.assertEquals(updatedDetail, gotDetail);

      try {
        store.updateOperationMeta(
            new OperationRunId(Namespace.DEFAULT.getId(), testId),
            updatedMeta,
            updatedDetail.getSourceId());
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testUpdateStatus() throws Exception {
    OperationRunDetail expectedDetail = insertRun(testNamespace, "LIST",
        OperationRunStatus.RUNNING);
    String testId = expectedDetail.getRun().getId();
    OperationRunId runId = new OperationRunId(testNamespace, testId);

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore store = new OperationRunStore(context);
      OperationRunDetail gotDetail = store.getOperation(runId);
      Assert.assertEquals(expectedDetail, gotDetail);

      OperationRun updatedRun = OperationRun.builder(expectedDetail.getRun())
          .setStatus(OperationRunStatus.STOPPING)
          .build();
      OperationRunDetail updatedDetail = OperationRunDetail.builder(expectedDetail)
          .setRun(updatedRun)
          .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
          .build();
      store.updateOperationStatus(runId, updatedRun.getStatus(),
          updatedDetail.getSourceId());
      gotDetail = (OperationRunDetail) store.getOperation(runId);
      Assert.assertEquals(updatedDetail, gotDetail);

      try {
        store.updateOperationStatus(
            new OperationRunId(Namespace.DEFAULT.getId(), testId),
            updatedRun.getStatus(),
            updatedDetail.getSourceId())
        ;
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testFailOperation() throws Exception {
    OperationRunDetail expectedDetail = insertRun(testNamespace, "LIST",
        OperationRunStatus.RUNNING);
    String testId = expectedDetail.getRun().getId();
    OperationRunId runId = new OperationRunId(testNamespace, testId);

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore store = new OperationRunStore(context);
      OperationRunDetail gotDetail = (OperationRunDetail) store.getOperation(runId);
      Assert.assertEquals(expectedDetail, gotDetail);

      OperationError error = new OperationError("operation failed", Collections.emptyList());
      OperationRun updatedRun = OperationRun.builder(expectedDetail.getRun())
          .setStatus(OperationRunStatus.FAILED)
          .setError(error)
          .build();
      OperationRunDetail updatedDetail = OperationRunDetail.builder(expectedDetail)
          .setRun(updatedRun)
          .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
          .build();
      store.failOperationRun(runId, error, updatedDetail.getSourceId());
      gotDetail = (OperationRunDetail) store.getOperation(runId);
      Assert.assertEquals(updatedDetail, gotDetail);

      try {
        store.failOperationRun(
            new OperationRunId(Namespace.DEFAULT.getId(), testId),
            error,
            updatedDetail.getSourceId()
        );
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testScanOperation() throws Exception {
    List<OperationRunDetail> insertedRuns = insertTestRuns();
    // get a filtered list of testNamespace runs
    List<OperationRunDetail> testNamespaceRuns = insertedRuns.stream()
        .filter(detail -> detail.getRunId().getNamespace().equals(testNamespace))
        .collect(Collectors.toList());

    TransactionRunners.run(transactionRunner, context -> {
      List<OperationRunDetail> gotRuns = new ArrayList<>();
      List<OperationRunDetail> expectedRuns;
      ScanOperationRunsRequest request;

      OperationRunStore store = new OperationRunStore(context);

      // verify the scan without filters picks all runs for testNamespace
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace).build();
      store.scanOperations(request, gotRuns::add);
      expectedRuns = testNamespaceRuns;
      Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

      // verify limit
      gotRuns.clear();
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace).setLimit(2).build();
      store.scanOperations(request, gotRuns::add);
      expectedRuns = testNamespaceRuns.stream().limit(2).collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

      // verify the scan with type filter
      gotRuns.clear();
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace)
          .setFilter(new OperationRunFilter("PUSH", null)).build();
      store.scanOperations(request, gotRuns::add);
      expectedRuns = testNamespaceRuns.stream()
          .filter(detail -> detail.getRun().getType().equals("PUSH"))
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

      // verify the scan with status filter
      gotRuns.clear();
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace)
          .setFilter(new OperationRunFilter("PUSH", OperationRunStatus.FAILED)).build();
      expectedRuns = testNamespaceRuns.stream()
          .filter(detail -> detail.getRun().getType().equals("PUSH"))
          .filter(detail -> detail.getRun().getStatus().equals(OperationRunStatus.FAILED))
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

      gotRuns.clear();
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace).setLimit(20)
          .setFilter(new OperationRunFilter("PULL", OperationRunStatus.FAILED)).build();
      store.scanOperations(request, gotRuns::add);
      expectedRuns = testNamespaceRuns.stream()
          .filter(detail -> detail.getRun().getType().equals("PULL"))
          .filter(detail -> detail.getRun().getStatus().equals(OperationRunStatus.FAILED))
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());
    }, Exception.class);
  }

  private OperationRunDetail insertRun(String namespace, String type,
      OperationRunStatus status)
      throws IOException, OperationRunAlreadyExistsException {
    long startTime = runIdTime.incrementAndGet();
    String id = RunIds.generate(startTime).getId();
    OperationRun run = OperationRun.builder()
        .setRunId(id)
        .setStatus(status)
        .setType(type)
        .setMetadata(
            new OperationMeta(Collections.emptySet(), Instant.ofEpochMilli(startTime), null))
        .build();
    OperationRunId runId = new OperationRunId(namespace, id);
    OperationRunDetail detail = OperationRunDetail.builder()
        .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
        .setRunId(runId)
        .setRun(run)
        .setPullAppsRequest(input)
        .build();
    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore operationRunStore = new OperationRunStore(context);
      operationRunStore.createOperationRun(runId, detail);
    }, IOException.class, OperationRunAlreadyExistsException.class);
    return detail;
  }

  private List<OperationRunDetail> insertTestRuns() throws Exception {
    List<OperationRunDetail> details = new ArrayList<>();
    // insert 10 runs with increasing start time in two namespaces
    // 5 would be in running state 5 in Failed
    // 5 would be of type PUSH 5 would be of type PULL
    for (int i = 0; i < 5; i++) {
      details.add(insertRun(testNamespace, "PUSH", OperationRunStatus.RUNNING));
      details.add(insertRun(Namespace.DEFAULT.getId(), "PUSH", OperationRunStatus.RUNNING));
      details.add(insertRun(testNamespace, "PULL", OperationRunStatus.FAILED));
      details.add(insertRun(Namespace.DEFAULT.getId(), "PULL", OperationRunStatus.RUNNING));
    }
    // The runs are added in increasing start time, hence reversing the List
    Collections.reverse(details);
    return details;
  }

}

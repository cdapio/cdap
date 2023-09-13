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

package io.cdap.cdap.internal.operations;

import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.store.AppMetadataStoreTest;
import io.cdap.cdap.internal.app.store.OperationRunDetail;
import io.cdap.cdap.proto.operationrun.OperationError;
import io.cdap.cdap.proto.operationrun.OperationMeta;
import io.cdap.cdap.proto.operationrun.OperationRun;
import io.cdap.cdap.proto.operationrun.OperationRunStatus;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OperationRunsStoreTest {

  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStoreTest.class);

  protected static TransactionRunner transactionRunner;
  private final AtomicInteger sourceId = new AtomicInteger();
  private final AtomicLong runIdTime = new AtomicLong(System.currentTimeMillis());
  private final String testNamespace = "test";


  @Before
  public void before() throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      OperationRunsStore operationRunsStore = new OperationRunsStore(context);
      operationRunsStore.deleteAllTables();
    });
  }

  @Test
  public void testGetOperation() throws Exception {
    OperationRunDetail<String> expectedDetail = insertRun(testNamespace, "LIST",
        OperationRunStatus.RUNNING);
    String testId = expectedDetail.getRun().getId();

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunsStore store = new OperationRunsStore(context);
      OperationRunDetail<String> gotDetail = (OperationRunDetail<String>) store.getOperation(
          testNamespace, testId);
      Assert.assertEquals(expectedDetail, gotDetail);
      try {

        store.getOperation(Namespace.DEFAULT.getId(), testId);
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testUpdateMetadata() throws Exception {
    OperationRunDetail<String> expectedDetail = insertRun(testNamespace, "LIST",
        OperationRunStatus.RUNNING);
    String testId = expectedDetail.getRun().getId();

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunsStore store = new OperationRunsStore(context);
      OperationRunDetail<String> gotDetail = (OperationRunDetail<String>) store.getOperation(
          testNamespace, testId);
      Assert.assertEquals(expectedDetail, gotDetail);

      OperationMeta updatedMeta = new OperationMeta(Collections.emptySet(),
          Instant.ofEpochMilli(runIdTime.incrementAndGet()),
          Instant.ofEpochMilli(runIdTime.incrementAndGet()));
      OperationRun updatedRun = OperationRun.builder(expectedDetail.getRun())
          .setMetadata(updatedMeta).build();
      OperationRunDetail<String> updatedDetail = OperationRunDetail.<String>builder(expectedDetail)
          .setRun(updatedRun)
          .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
          .build();
      store.updateOperationMeta(testNamespace, testId, updatedMeta,
          updatedDetail.getSourceId());
      gotDetail = (OperationRunDetail<String>) store.getOperation(
          testNamespace, testId);
      Assert.assertEquals(updatedDetail, gotDetail);

      try {
        store.updateOperationMeta(Namespace.DEFAULT.getId(), testId, updatedMeta,
            updatedDetail.getSourceId());
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testUpdateStatus() throws Exception {
    OperationRunDetail<String> expectedDetail = insertRun(testNamespace, "LIST",
        OperationRunStatus.RUNNING);
    String testId = expectedDetail.getRun().getId();

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunsStore store = new OperationRunsStore(context);
      OperationRunDetail<String> gotDetail = (OperationRunDetail<String>) store.getOperation(
          testNamespace, testId);
      Assert.assertEquals(expectedDetail, gotDetail);

      OperationRun updatedRun = OperationRun.builder(expectedDetail.getRun())
          .setStatus(OperationRunStatus.STOPPING)
          .build();
      OperationRunDetail<String> updatedDetail = OperationRunDetail.<String>builder(expectedDetail)
          .setRun(updatedRun)
          .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
          .build();
      store.updateOperationStatus(testNamespace, testId, updatedRun.getStatus(),
          updatedDetail.getSourceId());
      gotDetail = (OperationRunDetail<String>) store.getOperation(
          testNamespace, testId);
      Assert.assertEquals(updatedDetail, gotDetail);

      try {
        store.updateOperationStatus(Namespace.DEFAULT.getId(), testId,
            updatedRun.getStatus(), updatedDetail.getSourceId());
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testFailOperation() throws Exception {
    OperationRunDetail<String> expectedDetail = insertRun(testNamespace, "LIST",
        OperationRunStatus.RUNNING);
    String testId = expectedDetail.getRun().getId();

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunsStore store = new OperationRunsStore(context);
      OperationRunDetail<String> gotDetail = (OperationRunDetail<String>) store.getOperation(
          testNamespace, testId);
      Assert.assertEquals(expectedDetail, gotDetail);

      OperationError error = new OperationError("operation failed", Collections.emptyList());
      OperationRun updatedRun = OperationRun.builder(expectedDetail.getRun())
          .setStatus(OperationRunStatus.FAILED)
          .setError(error)
          .build();
      OperationRunDetail<String> updatedDetail = OperationRunDetail.<String>builder(expectedDetail)
          .setRun(updatedRun)
          .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
          .build();
      store.failOperationRun(testNamespace, testId, error,
          updatedDetail.getSourceId());
      gotDetail = (OperationRunDetail<String>) store.getOperation(
          testNamespace, testId);
      Assert.assertEquals(updatedDetail, gotDetail);

      try {
        store.failOperationRun(Namespace.DEFAULT.getId(), testId, error,
            updatedDetail.getSourceId());
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testScanOperation() throws Exception {
    insertTestRuns();

    // TODO(samik) verify the actual list
    TransactionRunners.run(transactionRunner, context -> {
      OperationRunsStore store = new OperationRunsStore(context);

      // verify the scan without filters picks all runs for testNamespace
      List<OperationRunDetail> gotRuns = new ArrayList<>();
      ScanOperationRunsRequest request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace).setLimit(20).build();
      store.scanRuns(request, 5, (id, detail) -> gotRuns.add(detail));
      Assert.assertEquals(10, gotRuns.size());

      // verify the scan with type filter
      gotRuns.clear();
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace).setLimit(20)
          .setFilter(new OperationRunFilter("PUSH", null)).build();
      store.scanRuns(request, 5, (id, detail) -> gotRuns.add(detail));
      Assert.assertEquals(5, gotRuns.size());

      // verify the scan with status filter
      gotRuns.clear();
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace).setLimit(20)
          .setFilter(new OperationRunFilter("PUSH", OperationRunStatus.FAILED)).build();
      store.scanRuns(request, 5, (id, detail) -> gotRuns.add(detail));
      Assert.assertEquals(0, gotRuns.size());

      gotRuns.clear();
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace).setLimit(20)
          .setFilter(new OperationRunFilter("PULL", OperationRunStatus.FAILED)).build();
      store.scanRuns(request, 5, (id, detail) -> gotRuns.add(detail));
      Assert.assertEquals(5, gotRuns.size());
    }, Exception.class);
  }

  private OperationRunDetail<String> insertRun(String namespace, String type,
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
    OperationRunDetail<String> detail = OperationRunDetail.<String>builder()
        .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
        .setRun(run)
        .setRequest("")
        .build();
    TransactionRunners.run(transactionRunner, context -> {
      OperationRunsStore operationRunsStore = new OperationRunsStore(context);
      operationRunsStore.createOperationRun(namespace, id, detail);
    }, IOException.class, OperationRunAlreadyExistsException.class);
    return detail;
  }

  private void insertTestRuns() throws Exception {
    // insert 10 runs with increasing start time in two namespaces
    // 5 would be in running state 5 in Failed
    // 5 would be of type PUSH 5 would be of type PULL
    for (int i = 0; i < 5; i++) {
      insertRun(testNamespace, "PUSH",
          OperationRunStatus.RUNNING);
      insertRun(Namespace.DEFAULT.getId(), "PUSH", OperationRunStatus.RUNNING);
      insertRun(testNamespace, "PULL",
          OperationRunStatus.FAILED);
      insertRun(Namespace.DEFAULT.getId(), "PULL", OperationRunStatus.RUNNING);
    }
  }

}

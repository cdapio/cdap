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

import com.google.common.io.Closeables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.AppFabric;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.proto.operation.OperationType;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class OperationLifecycleManagerTest extends OperationTestBase {
  protected static TransactionRunner transactionRunner;
  private static final String testNamespace = "test";
  private static OperationLifecycleManager operationLifecycleManager;
  private static int batchSize;

  @ClassRule 
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static EmbeddedPostgres postgres;

  @Before
  public void before() throws Exception {
    TransactionRunners.run(
        transactionRunner,
        context -> {
          OperationRunStore operationRunsStore = new OperationRunStore(context);
          operationRunsStore.clearData();
        });
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    postgres = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());
    Injector injector =
        Guice.createInjector(
            new ConfigModule(cConf),
            new LocalLocationModule(),
            new SystemDatasetRuntimeModule().getInMemoryModules(),
            new StorageModule(),
            new AbstractModule() {
              @Override
              protected void configure() {
                bind(MetricsCollectionService.class)
                    .to(NoOpMetricsCollectionService.class)
                    .in(Scopes.SINGLETON);
              }
            });

    transactionRunner = injector.getInstance(TransactionRunner.class);
    operationLifecycleManager =
        new OperationLifecycleManager(transactionRunner, Mockito.mock(OperationRuntime.class));
    StoreDefinition.OperationRunsStore.create(injector.getInstance(StructuredTableAdmin.class));
    batchSize = cConf.getInt(AppFabric.STREAMING_BATCH_SIZE);
  }

  @AfterClass
  public static void afterClass() {
    Closeables.closeQuietly(postgres);
  }

  @Test
  public void testScanOperations() throws Exception {
    List<OperationRunDetail> insertedRuns = insertTestRuns(transactionRunner);
    // get a filtered list of testNamespace runs
    List<OperationRunDetail> testNamespaceRuns =
        insertedRuns.stream()
            .filter(detail -> detail.getRunId().getNamespace().equals(testNamespace))
            .collect(Collectors.toList());

    TransactionRunners.run(
        transactionRunner,
        context -> {
          List<OperationRunDetail> gotRuns = new ArrayList<>();
          List<OperationRunDetail> expectedRuns;
          ScanOperationRunsRequest request;

          // verify the scan without filters picks all runs for testNamespace
          request = ScanOperationRunsRequest.builder().setNamespace(testNamespace).build();
          operationLifecycleManager.scanOperations(request, batchSize, gotRuns::add);
          expectedRuns = testNamespaceRuns;
          Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

          // verify limit
          gotRuns.clear();
          request =
              ScanOperationRunsRequest.builder().setNamespace(testNamespace).setLimit(2).build();
          operationLifecycleManager.scanOperations(request, batchSize, gotRuns::add);
          expectedRuns = testNamespaceRuns.stream().limit(2).collect(Collectors.toList());
          Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

          // verify the scan with type filter
          gotRuns.clear();
          request =
              ScanOperationRunsRequest.builder()
                  .setNamespace(testNamespace)
                  .setFilter(new OperationRunFilter(OperationType.PUSH_APPS, null))
                  .build();
          operationLifecycleManager.scanOperations(request, batchSize, gotRuns::add);
          expectedRuns =
              testNamespaceRuns.stream()
                  .filter(detail -> detail.getRun().getType().equals(OperationType.PUSH_APPS))
                  .collect(Collectors.toList());
          Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

          // verify the scan with status filter and limit
          gotRuns.clear();
          request =
              ScanOperationRunsRequest.builder()
                  .setNamespace(testNamespace)
                  .setLimit(2)
                  .setFilter(
                      new OperationRunFilter(OperationType.PULL_APPS, OperationRunStatus.FAILED))
                  .build();
          operationLifecycleManager.scanOperations(request, batchSize, gotRuns::add);
          expectedRuns =
              testNamespaceRuns.stream()
                  .filter(detail -> detail.getRun().getType().equals(OperationType.PULL_APPS))
                  .filter(detail -> detail.getRun().getStatus().equals(OperationRunStatus.FAILED))
                  .limit(2)
                  .collect(Collectors.toList());
          Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

          // verify the scan with status filter
          gotRuns.clear();
          request =
              ScanOperationRunsRequest.builder()
                  .setNamespace(testNamespace)
                  .setFilter(
                      new OperationRunFilter(OperationType.PULL_APPS, OperationRunStatus.FAILED))
                  .build();
          operationLifecycleManager.scanOperations(request, batchSize, gotRuns::add);
          expectedRuns =
              testNamespaceRuns.stream()
                  .filter(detail -> detail.getRun().getType().equals(OperationType.PULL_APPS))
                  .filter(detail -> detail.getRun().getStatus().equals(OperationRunStatus.FAILED))
                  .collect(Collectors.toList());
          Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());
        });
  }

  @Test
  public void testGetOperation() throws Exception {
    OperationRunDetail expectedDetail =
        insertRun(
            testNamespace, OperationType.PUSH_APPS, OperationRunStatus.RUNNING, transactionRunner);
    String testId = expectedDetail.getRun().getId();
    OperationRunId runId = new OperationRunId(testNamespace, testId);

    TransactionRunners.run(
        transactionRunner,
        context -> {
          OperationRunDetail gotDetail = operationLifecycleManager.getOperationRun(runId);
          Assert.assertEquals(expectedDetail, gotDetail);
          try {
            operationLifecycleManager.getOperationRun(
                new OperationRunId(Namespace.DEFAULT.getId(), testId));
            Assert.fail("Found unexpected run in default namespace");
          } catch (OperationRunNotFoundException e) {
            // expected
          }
        },
        Exception.class);
  }
}

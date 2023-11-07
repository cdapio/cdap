package io.cdap.cdap.internal.operation;

import com.esotericsoftware.minlog.Log;
import com.google.common.io.Closeables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.AppFabric;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.common.lang.Exceptions;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsRequest;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationMeta;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.proto.operation.OperationType;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OperationLifecycleManagerTest extends AppFabricTestBase {
  protected static TransactionRunner transactionRunner;
  private static final AtomicInteger sourceId = new AtomicInteger();
  private static final AtomicLong runIdTime = new AtomicLong(System.currentTimeMillis());
  private static final String testNamespace = "test";
  private static OperationLifecycleManager operationLifecycleManager;
  private static int batchSize;
  private static final PullAppsRequest input = new PullAppsRequest(Collections.emptySet(), null);

  @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static EmbeddedPostgres pg;

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
    pg = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());
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
    operationLifecycleManager = injector.getInstance(OperationLifecycleManager.class);
    StoreDefinition.OperationRunsStore.create(injector.getInstance(StructuredTableAdmin.class));
    batchSize = cConf.getInt(AppFabric.STREAMING_BATCH_SIZE);
  }

  @AfterClass
  public static void afterClass(){
    try{
    pg.close();
    }
    catch(Exception e){}
  }

  @Test
  public void testScanOperations() throws Exception {
    List<OperationRunDetail> insertedRuns = insertTestRuns();
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
          operationLifecycleManager.scanOperations(request, batchSize, d -> gotRuns.add(d));
          expectedRuns = testNamespaceRuns;
          Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

          // verify limit
          gotRuns.clear();
          request =
              ScanOperationRunsRequest.builder().setNamespace(testNamespace).setLimit(2).build();
          operationLifecycleManager.scanOperations(request, batchSize, d -> gotRuns.add(d));
          expectedRuns = testNamespaceRuns.stream().limit(2).collect(Collectors.toList());
          Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

          // verify the scan with type filter
          gotRuns.clear();
          request =
              ScanOperationRunsRequest.builder()
                  .setNamespace(testNamespace)
                  .setFilter(new OperationRunFilter(OperationType.PUSH_APPS, null))
                  .build();
          operationLifecycleManager.scanOperations(request, batchSize, d -> gotRuns.add(d));
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
          operationLifecycleManager.scanOperations(request, batchSize, d -> gotRuns.add(d));
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
          operationLifecycleManager.scanOperations(request, batchSize, d -> gotRuns.add(d));
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
        insertRun(testNamespace, OperationType.PUSH_APPS, OperationRunStatus.RUNNING);
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

  private static OperationRunDetail insertRun(
      String namespace, OperationType type, OperationRunStatus status)
      throws IOException, OperationRunAlreadyExistsException {
    long startTime = runIdTime.incrementAndGet();
    String id = RunIds.generate(startTime).getId();
    OperationRun run =
        OperationRun.builder()
            .setRunId(id)
            .setStatus(status)
            .setType(type)
            .setMetadata(
                new OperationMeta(Collections.emptySet(), Instant.ofEpochMilli(startTime), null))
            .build();
    OperationRunId runId = new OperationRunId(namespace, id);
    OperationRunDetail detail =
        OperationRunDetail.builder()
            .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
            .setRunId(runId)
            .setRun(run)
            .setPullAppsRequest(input)
            .build();
    TransactionRunners.run(
        transactionRunner,
        context -> {
          OperationRunStore operationRunStore = new OperationRunStore(context);
          operationRunStore.createOperationRun(runId, detail);
        },
        IOException.class,
        OperationRunAlreadyExistsException.class);
    return detail;
  }

  private static List<OperationRunDetail> insertTestRuns() throws Exception {
    List<OperationRunDetail> details = new ArrayList<>();
    // insert 10 runs with increasing start time in two namespaces
    // 5 would be in running state 5 in Failed
    // 5 would be of type PUSH 5 would be of type PULL
    for (int i = 0; i < 5; i++) {
      details.add(insertRun(testNamespace, OperationType.PUSH_APPS, OperationRunStatus.RUNNING));
      details.add(
          insertRun(
              Namespace.DEFAULT.getId(), OperationType.PUSH_APPS, OperationRunStatus.RUNNING));
      details.add(insertRun(testNamespace, OperationType.PULL_APPS, OperationRunStatus.FAILED));
      details.add(
          insertRun(
              Namespace.DEFAULT.getId(), OperationType.PULL_APPS, OperationRunStatus.RUNNING));
    }
    // The runs are added in increasing start time, hence reversing the List
    Collections.reverse(details);
    return details;
  }
}

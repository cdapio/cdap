package io.cdap.cdap.internal.operation;

import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsRequest;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationMeta;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.proto.operation.OperationType;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class OperationTestBase extends AppFabricTestBase {
  private static final AtomicInteger sourceId = new AtomicInteger();
  private static final AtomicLong runIdTime = new AtomicLong(System.currentTimeMillis());
  private static final String testNamespace = "test";
  private static final PullAppsRequest input = new PullAppsRequest(Collections.emptySet(), null);

  protected static OperationRunDetail insertRun(
      String namespace,
      OperationType type,
      OperationRunStatus status,
      TransactionRunner transactionRunner)
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

  protected static List<OperationRunDetail> insertTestRuns(TransactionRunner transactionRunner)
      throws Exception {
    List<OperationRunDetail> details = new ArrayList<>();
    // insert 10 runs with increasing start time in two namespaces
    // 5 would be in running state 5 in Failed
    // 5 would be of type PUSH 5 would be of type PULL
    for (int i = 0; i < 5; i++) {
      details.add(
          insertRun(
              testNamespace,
              OperationType.PUSH_APPS,
              OperationRunStatus.RUNNING,
              transactionRunner));
      details.add(
          insertRun(
              Namespace.DEFAULT.getId(),
              OperationType.PUSH_APPS,
              OperationRunStatus.RUNNING,
              transactionRunner));
      details.add(
          insertRun(
              testNamespace,
              OperationType.PULL_APPS,
              OperationRunStatus.FAILED,
              transactionRunner));
      details.add(
          insertRun(
              Namespace.DEFAULT.getId(),
              OperationType.PULL_APPS,
              OperationRunStatus.RUNNING,
              transactionRunner));
    }
    // The runs are added in increasing start time, hence reversing the List
    Collections.reverse(details);
    return details;
  }
}

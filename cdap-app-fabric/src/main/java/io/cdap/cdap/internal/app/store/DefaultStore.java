/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.worker.WorkerSpecification;
import io.cdap.cdap.api.workflow.WorkflowActionNode;
import io.cdap.cdap.api.workflow.WorkflowNode;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.app.ForwardingApplicationSpecification;
import io.cdap.cdap.internal.app.store.state.AppStateKey;
import io.cdap.cdap.internal.app.store.state.AppStateKeyValue;
import io.cdap.cdap.internal.app.store.state.AppStateTable;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.ProgramHistory;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunCountResult;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.WorkflowStatistics;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the Store that ultimately places data into MetaDataTable.
 */
public class DefaultStore implements Store {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultStore.class);

  // mds is specific for metadata, we do not want to add workflow stats related information to the mds,
  // as it is not specifically metadata
  private static final DatasetId WORKFLOW_STATS_INSTANCE_ID = NamespaceId.SYSTEM.dataset(
      "workflow.stats");
  private static final Map<String, String> EMPTY_STRING_MAP = Collections.emptyMap();
  // If store does not support reverse scanning, we reorder in memory.
  // This is maximum number of items we will reorder at a time
  // before repeating the scan. We would need to store so many key in memory to serve the answer
  private static final int MAX_REORDER_BATCH = 1000;

  private final TransactionRunner transactionRunner;
  private final int maxReorderBatch;

  @Inject
  public DefaultStore(TransactionRunner transactionRunner) {
    this(transactionRunner, MAX_REORDER_BATCH);
  }

  @VisibleForTesting
  DefaultStore(TransactionRunner transactionRunner, int maxReorderBatch) {
    this.transactionRunner = transactionRunner;
    this.maxReorderBatch = maxReorderBatch;
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by app mds.
   *
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework)
      throws IOException, DatasetManagementException, UnauthorizedException {
    framework.addInstance(Table.class.getName(), AppMetadataStore.APP_META_INSTANCE_ID,
        DatasetProperties.EMPTY);
    framework.addInstance(Table.class.getName(), WORKFLOW_STATS_INSTANCE_ID,
        DatasetProperties.EMPTY);
  }

  private AppMetadataStore getAppMetadataStore(StructuredTableContext context) {
    return AppMetadataStore.create(context);
  }

  private WorkflowTable getWorkflowTable(StructuredTableContext context)
      throws TableNotFoundException {
    return new WorkflowTable(context.getTable(StoreDefinition.WorkflowStore.WORKFLOW_STATISTICS));
  }

  @Override
  public ProgramDescriptor loadProgram(ProgramId id) throws NotFoundException {
    ApplicationMeta appMeta = TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getApplication(id.getParent());
    });

    if (appMeta == null) {
      throw new ApplicationNotFoundException(id.getParent());
    }

    Store.ensureProgramExists(id, appMeta.getSpec());
    return new ProgramDescriptor(id, appMeta.getSpec());
  }

  @Override
  public ProgramDescriptor loadProgram(ProgramReference ref) throws NotFoundException {
    ApplicationMeta appMeta = TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getLatest(ref.getParent());
    });

    if (appMeta == null) {
      throw new ApplicationNotFoundException(ref.getParent());
    }

    ProgramId id = ref.id(appMeta.getSpec().getAppVersion());

    Store.ensureProgramExists(id, appMeta.getSpec());
    return new ProgramDescriptor(id, appMeta.getSpec());
  }

  @Override
  public void setProvisioning(ProgramRunId id, Map<String, String> runtimeArgs,
      Map<String, String> systemArgs, byte[] sourceId, ArtifactId artifactId) {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).recordProgramProvisioning(id, runtimeArgs, systemArgs, sourceId,
          artifactId);
    });
  }

  @Override
  public void setProvisioned(ProgramRunId id, int numNodes, byte[] sourceId) {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).recordProgramProvisioned(id, numNodes, sourceId);
    });
  }

  @Override
  public void setStart(ProgramRunId id, @Nullable String twillRunId, Map<String, String> systemArgs,
      byte[] sourceId) {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).recordProgramStart(id, twillRunId, systemArgs, sourceId);
    });
  }

  @Override
  public void setRunning(ProgramRunId id, long runTime, String twillRunId, byte[] sourceId) {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).recordProgramRunning(id, runTime, twillRunId, sourceId);
    });
  }

  @Override
  public void setStopping(ProgramRunId id, byte[] sourceId, long stoppingTsSecs,
      long terminateTsSecs) {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).recordProgramStopping(id, sourceId, stoppingTsSecs,
          terminateTsSecs);
    });
  }

  @Override
  public void setStop(ProgramRunId id, long endTime, ProgramRunStatus runStatus, byte[] sourceId) {
    setStop(id, endTime, runStatus, null, sourceId);
  }

  @Override
  public void setStop(ProgramRunId id, long endTime, ProgramRunStatus runStatus,
      BasicThrowable failureCause, byte[] sourceId) {
    Preconditions.checkArgument(runStatus != null, "Run state of program run should be defined");
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metaStore = getAppMetadataStore(context);
      metaStore.recordProgramStop(id, endTime, runStatus, failureCause, sourceId);

      // This block has been added so that completed workflow runs can be logged to the workflow dataset
      WorkflowId workflowId = new WorkflowId(id.getParent().getParent(), id.getProgram());
      if (id.getType() == ProgramType.WORKFLOW && runStatus == ProgramRunStatus.COMPLETED) {
        WorkflowTable workflowTable = getWorkflowTable(context);
        recordCompletedWorkflow(metaStore, workflowTable, workflowId, id.getRun());
      }
      // todo: delete old history data
    });
  }

  private void recordCompletedWorkflow(AppMetadataStore metaStore, WorkflowTable workflowTable,
      WorkflowId workflowId, String runId)
      throws IOException, TableNotFoundException {
    RunRecordDetail runRecord = metaStore.getRun(workflowId.run(runId));
    if (runRecord == null) {
      return;
    }
    ApplicationId app = workflowId.getParent();
    ApplicationSpecification appSpec = getApplicationSpec(metaStore, app);
    if (appSpec == null || appSpec.getWorkflows() == null
        || appSpec.getWorkflows().get(workflowId.getProgram()) == null) {
      LOG.warn("Missing ApplicationSpecification for {}, "
              + "potentially caused by application removal right after stopping workflow {}", app,
          workflowId);
      return;
    }

    boolean workFlowNodeFailed = false;
    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(workflowId.getProgram());
    Map<String, WorkflowNode> nodeIdMap = workflowSpec.getNodeIdMap();
    List<WorkflowTable.ProgramRun> programRunsList = new ArrayList<>();
    for (Map.Entry<String, String> entry : runRecord.getProperties().entrySet()) {
      if (!("workflowToken".equals(entry.getKey()) || "runtimeArgs".equals(entry.getKey())
          || "workflowNodeState".equals(entry.getKey()))) {
        WorkflowActionNode workflowNode = (WorkflowActionNode) nodeIdMap.get(entry.getKey());
        ProgramType programType = ProgramType.valueOfSchedulableType(
            workflowNode.getProgram().getProgramType());
        ProgramId innerProgram = app.program(programType, entry.getKey());
        RunRecordDetail innerProgramRun = metaStore.getRun(innerProgram.run(entry.getValue()));
        if (innerProgramRun != null && innerProgramRun.getStatus()
            .equals(ProgramRunStatus.COMPLETED)) {
          Long stopTs = innerProgramRun.getStopTs();
          // since the program is completed, the stop ts cannot be null
          if (stopTs == null) {
            LOG.warn("Since the program has completed, expected its stop time to not be null. "
                    + "Not writing workflow completed record for Program = {}, Workflow = {}, Run = {}",
                innerProgram, workflowId, runRecord);
            workFlowNodeFailed = true;
            break;
          }
          programRunsList.add(new WorkflowTable.ProgramRun(entry.getKey(), entry.getValue(),
              programType, stopTs - innerProgramRun.getStartTs()));
        } else {
          workFlowNodeFailed = true;
          break;
        }
      }
    }

    if (workFlowNodeFailed) {
      return;
    }

    workflowTable.write(workflowId, runRecord, programRunsList);
  }

  @Override
  public void deleteWorkflowStats(ApplicationId id) {
    TransactionRunners.run(transactionRunner, context -> {
      getWorkflowTable(context).delete(id);
    });
  }

  @Override
  public void setSuspend(ProgramRunId id, byte[] sourceId, long suspendTime) {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).recordProgramSuspend(id, sourceId, suspendTime);
    });
  }

  @Override
  public void setResume(ProgramRunId id, byte[] sourceId, long resumeTime) {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).recordProgramResumed(id, sourceId, resumeTime);
    });
  }

  @Override
  @Nullable
  public WorkflowStatistics getWorkflowStatistics(NamespaceId namespaceId, String appName,
      String workflowName,
      long startTime, long endTime, List<Double> percentiles) {
    return TransactionRunners.run(transactionRunner, context -> {
      ApplicationMeta latestAppMeta = getAppMetadataStore(context).getLatest(
          namespaceId.appReference(appName));
      if (latestAppMeta == null) {
        // if app is not found then there is no workflow stats
        return null;
      }
      WorkflowId id = new WorkflowId(namespaceId.getNamespace(), appName,
          latestAppMeta.getSpec().getAppVersion(),
          workflowName);
      return getWorkflowTable(context).getStatistics(id, startTime, endTime, percentiles);
    });
  }

  @Override
  public WorkflowTable.WorkflowRunRecord getWorkflowRun(WorkflowId workflowId, String runId) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getWorkflowTable(context).getRecord(workflowId, runId);
    });
  }

  @Override
  public WorkflowTable.WorkflowRunRecord getWorkflowRun(NamespaceId namespaceId, String appName,
      String workflowName,
      String runId) {
    return TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetaStore = getAppMetadataStore(context);
      WorkflowId workflowId = getWorkflowIdFromRun(appMetaStore, namespaceId, appName, workflowName,
          runId);
      return getWorkflowTable(context).getRecord(workflowId, runId);
    });
  }

  @Override
  public Collection<WorkflowTable.WorkflowRunRecord> retrieveSpacedRecords(NamespaceId namespaceId,
      String appName,
      String workflowName, String runId, int limit,
      long timeInterval) {
    return TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetaStore = getAppMetadataStore(context);
      WorkflowId workflow = getWorkflowIdFromRun(appMetaStore, namespaceId, appName, workflowName,
          runId);
      return getWorkflowTable(context).getDetailsOfRange(workflow, runId, limit, timeInterval);
    });
  }

  /**
   * Fetches the run record for particular run of a program without version.
   *
   * @param appMetaStore AppMetadataStore
   * @param namespaceId namespace id
   * @param appName application name
   * @param workflowName the name of the workflow program
   * @param runId the run id
   * @return run record for the specified program and runRef, null if not found
   */
  private WorkflowId getWorkflowIdFromRun(AppMetadataStore appMetaStore, NamespaceId namespaceId,
      String appName, String workflowName, String runId)
      throws IOException, NotFoundException {
    ProgramReference programRef = namespaceId.appReference(appName)
        .program(ProgramType.WORKFLOW, workflowName);
    // Fetch run record ignoring version
    RunRecordDetail runRecord = appMetaStore.getRun(programRef, runId);
    if (runRecord == null) {
      throw new NotFoundException(
          String.format("No run record found for program %s and runID: %s", programRef, runId));
    }

    ApplicationId appId = namespaceId.app(appName, runRecord.getVersion());
    ApplicationMeta appMeta = appMetaStore.getApplication(appId);
    if (appMeta == null) {
      throw new ApplicationNotFoundException(appId);
    }
    return new WorkflowId(appId, workflowName);
  }

  @Override
  public Map<ProgramRunId, RunRecordDetail> getAllRuns(ProgramReference programReference,
      ProgramRunStatus status,
      long startTime, long endTime, int limit,
      Predicate<RunRecordDetail> filter) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getAllProgramRuns(programReference, status, startTime,
          endTime, limit,
          filter);
    });
  }

  @Override
  public Map<ProgramRunId, RunRecordDetail> getRuns(ProgramId id, ProgramRunStatus status,
      long startTime, long endTime, int limit) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getRuns(id, status, startTime, endTime, limit, null);
    });
  }

  @Override
  public Map<ProgramRunId, RunRecordDetail> getRuns(ProgramRunStatus status,
      Predicate<RunRecordDetail> filter) {
    return getRuns(status, 0L, Long.MAX_VALUE, Integer.MAX_VALUE, filter);
  }

  @Override
  public Map<ProgramRunId, RunRecordDetail> getRuns(ProgramRunStatus status, long startTime,
      long endTime, int limit,
      Predicate<RunRecordDetail> filter) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getRuns(null, status, startTime, endTime, limit, filter);
    });
  }

  @Override
  public Map<ProgramRunId, RunRecordDetail> getRuns(ApplicationId applicationId,
      ProgramRunStatus status, int limit,
      @Nullable Predicate<RunRecordDetail> filter) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getRuns(applicationId, status, limit, filter);
    });
  }

  @Override
  public Map<ProgramRunId, RunRecordDetail> getRuns(Set<ProgramRunId> programRunIds) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getRuns(programRunIds);
    });
  }

  @Override
  public List<ProgramHistory> getRuns(Collection<ProgramReference> programs,
      ProgramRunStatus status,
      long startTime, long endTime, int limitPerProgram) {
    return TransactionRunners.run(transactionRunner, context -> {
      List<ProgramHistory> result = new ArrayList<>(programs.size());
      AppMetadataStore appMetadataStore = getAppMetadataStore(context);

      Set<ProgramId> existingPrograms = appMetadataStore.filterProgramsExistence(programs);
      Map<ProgramReference, ProgramId> programRefsMap =
          existingPrograms.stream()
              .collect(Collectors.toMap(ProgramId::getProgramReference, p -> p));

      for (ProgramReference programRef : programs) {
        ProgramId latestProgramId = programRefsMap.get(programRef);
        if (latestProgramId == null) {
          ProgramId defaultVersion = programRef.id(ApplicationId.DEFAULT_VERSION);
          result.add(new ProgramHistory(defaultVersion, Collections.emptyList(),
              new ProgramNotFoundException(defaultVersion)));
          continue;
        }

        List<RunRecord> runs = appMetadataStore.getRuns(latestProgramId, status, startTime, endTime,
                limitPerProgram, null)
            .values().stream()
            .map(record -> RunRecord.builder(record).build()).collect(Collectors.toList());
        result.add(new ProgramHistory(latestProgramId, runs, null));
      }

      return result;
    });
  }

  @Override
  public int countActiveRuns(@Nullable Integer limit) {
    return TransactionRunners.run(transactionRunner,
        context -> (int) getAppMetadataStore(context).countActiveRuns(limit));
  }

  @Override
  public void scanActiveRuns(int txBatchSize, Consumer<RunRecordDetail> consumer) {
    AtomicReference<AppMetadataStore.Cursor> cursorRef = new AtomicReference<>(
        AppMetadataStore.Cursor.EMPTY);

    AtomicInteger count = new AtomicInteger();
    do {
      count.set(0);
      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore.create(context)
            .scanActiveRuns(cursorRef.get(), txBatchSize, (cursor, runRecordDetail) -> {
              count.incrementAndGet();
              cursorRef.set(cursor);
              consumer.accept(runRecordDetail);
            });
      });
    } while (count.get() > 0);
  }

  @Override
  public Map<ProgramRunId, RunRecordDetail> getAllActiveRuns(
      ApplicationReference applicationReference) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getActiveRuns(applicationReference);
    });
  }

  @Override
  public Map<ProgramRunId, RunRecordDetail> getActiveRuns(NamespaceId namespaceId) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getActiveRuns(namespaceId);
    });
  }

  @Override
  public Map<ProgramRunId, RunRecordDetail> getActiveRuns(Set<NamespaceId> namespaces,
      Predicate<RunRecordDetail> filter) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getActiveRuns(namespaces, filter);
    });
  }

  @Override
  public Map<ProgramRunId, RunRecordDetail> getActiveRuns(ApplicationId applicationId) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getActiveRuns(applicationId);
    });
  }

  @Override
  public Map<ProgramRunId, RunRecordDetail> getActiveRuns(ProgramId programId) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getActiveRuns(programId);
    });
  }

  @Override
  public Map<ProgramId, Collection<RunRecordDetail>> getActiveRuns(
      Collection<ProgramReference> programRefs) {
    return TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = getAppMetadataStore(context);
      // Get the active runs for programs that exist
      return appMetadataStore.getActiveRuns(appMetadataStore.filterProgramsExistence(programRefs));
    });
  }

  /**
   * Returns run record for a given run id.
   *
   * @param id program run id
   * @return run record for runid
   */
  @Override
  public RunRecordDetail getRun(ProgramRunId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getRun(id);
    });
  }

  /**
   * Returns run record for a given run reference.
   *
   * @param programRef versionless program id of the run
   * @param runId the run id
   * @return run record for run reference
   */
  @Override
  public RunRecordDetail getRun(ProgramReference programRef, String runId) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getRun(programRef, runId);
    });
  }

  @Override
  public void markApplicationsLatest(Collection<ApplicationId> appIds)
      throws IOException, ApplicationNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore mds = getAppMetadataStore(context);
      for (ApplicationId appId : appIds) {
        mds.markAsLatest(appId);
      }
    }, IOException.class, ApplicationNotFoundException.class);
  }

  @Override
  public int addApplication(ApplicationId id, ApplicationMeta meta) throws ConflictException {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).createApplicationVersion(id, meta);
    }, ConflictException.class);
  }

  @Override
  public void updateApplicationSourceControlMeta(Map<ApplicationId, SourceControlMeta> updateRequests)
      throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore mds = getAppMetadataStore(context);
      for (Map.Entry<ApplicationId, SourceControlMeta> updateRequest : updateRequests.entrySet()) {
        try {
          mds.updateAppScmMeta(updateRequest.getKey(), updateRequest.getValue());
        } catch (ApplicationNotFoundException e) {
          // ignore this exception and continue updating the other applications
        }
      }
    }, IOException.class);
  }

  // todo: this method should be moved into DeletedProgramHandlerState, bad design otherwise
  @Override
  public List<ProgramSpecification> getDeletedProgramSpecifications(ApplicationReference appRef,
      ApplicationSpecification appSpec) {

    ApplicationMeta existing = TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getLatest(appRef);
    });

    List<ProgramSpecification> deletedProgramSpecs = Lists.newArrayList();

    if (existing != null) {
      ApplicationSpecification existingAppSpec = existing.getSpec();

      Map<String, ProgramSpecification> existingSpec = ImmutableMap.<String, ProgramSpecification>builder()
          .putAll(existingAppSpec.getMapReduce())
          .putAll(existingAppSpec.getSpark())
          .putAll(existingAppSpec.getWorkflows())
          .putAll(existingAppSpec.getServices())
          .putAll(existingAppSpec.getWorkers())
          .build();

      Map<String, ProgramSpecification> newSpec = ImmutableMap.<String, ProgramSpecification>builder()
          .putAll(appSpec.getMapReduce())
          .putAll(appSpec.getSpark())
          .putAll(appSpec.getWorkflows())
          .putAll(appSpec.getServices())
          .putAll(appSpec.getWorkers())
          .build();

      MapDifference<String, ProgramSpecification> mapDiff = Maps.difference(existingSpec, newSpec);
      deletedProgramSpecs.addAll(mapDiff.entriesOnlyOnLeft().values());
    }

    return deletedProgramSpecs;
  }

  @Override
  public void setWorkerInstances(ProgramId id, int instances) {
    Preconditions.checkArgument(instances > 0, "Cannot change number of worker instances to %s",
        instances);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metaStore = getAppMetadataStore(context);
      ApplicationSpecification appSpec = getApplicationSpec(metaStore, id.getParent());
      WorkerSpecification workerSpec = getWorkerSpecOrFail(id, appSpec);
      WorkerSpecification newSpecification = new WorkerSpecification(workerSpec.getClassName(),
          workerSpec.getName(),
          workerSpec.getDescription(),
          workerSpec.getProperties(),
          workerSpec.getDatasets(),
          workerSpec.getResources(),
          instances, workerSpec.getPlugins());
      ApplicationSpecification newAppSpec = replaceWorkerInAppSpec(appSpec, id, newSpecification);
      metaStore.updateAppSpec(id.getParent(), newAppSpec);
    });

    LOG.trace(
        "Setting program instances: namespace: {}, application: {}, worker: {}, new instances count: {}",
        id.getNamespaceId(), id.getApplication(), id.getProgram(), instances);
  }

  @Override
  public void setServiceInstances(ProgramId id, int instances) {
    Preconditions.checkArgument(instances > 0, "Cannot change number of service instances to %s",
        instances);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metaStore = getAppMetadataStore(context);
      ApplicationSpecification appSpec = getApplicationSpec(metaStore, id.getParent());
      ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);

      // Create a new spec copy from the old one, except with updated instances number
      serviceSpec = new ServiceSpecification(serviceSpec.getClassName(), serviceSpec.getName(),
          serviceSpec.getDescription(), serviceSpec.getHandlers(),
          serviceSpec.getResources(), instances, serviceSpec.getPlugins());

      ApplicationSpecification newAppSpec = replaceServiceSpec(appSpec, id.getProgram(),
          serviceSpec);
      metaStore.updateAppSpec(id.getParent(), newAppSpec);
    });

    LOG.trace(
        "Setting program instances: namespace: {}, application: {}, service: {}, new instances count: {}",
        id.getNamespaceId(), id.getApplication(), id.getProgram(), instances);
  }

  @Override
  public int getServiceInstances(ProgramId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      ApplicationSpecification appSpec = getApplicationSpec(getAppMetadataStore(context),
          id.getParent());
      ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);
      return serviceSpec.getInstances();
    });
  }

  @Override
  public int getWorkerInstances(ProgramId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      ApplicationSpecification appSpec = getApplicationSpec(getAppMetadataStore(context),
          id.getParent());
      WorkerSpecification workerSpec = getWorkerSpecOrFail(id, appSpec);
      return workerSpec.getInstances();
    });
  }

  @Override
  public void removeApplication(ApplicationReference appRef) {
    LOG.trace("Removing application: namespace: {}, application: {}", appRef.getNamespace(),
        appRef.getApplication());

    TransactionRunners.run(transactionRunner, context -> {
      getAppStateTable(context).deleteAll(appRef.getNamespaceId(), appRef.getApplication());
      AppMetadataStore metaStore = getAppMetadataStore(context);
      metaStore.deleteApplication(appRef);
      metaStore.deleteProgramHistory(appRef);
    });
  }

  @Override
  public void removeApplication(ApplicationId id) {
    LOG.trace("Removing application: namespace: {}, application: {}", id.getNamespace(),
              id.getApplication(), id.getVersion());

    TransactionRunners.run(transactionRunner, context -> {
      getAppStateTable(context).deleteAll(id.getNamespaceId(), id.getApplication());
      AppMetadataStore metaStore = getAppMetadataStore(context);
      metaStore.deleteApplication(id.getNamespace(), id.getApplication(), id.getVersion());
      metaStore.deleteProgramHistory(id.getNamespace(), id.getApplication(), id.getVersion());
    });
  }

  @Override
  public void removeAll(NamespaceId id) {
    LOG.trace("Removing all applications of namespace with id: {}", id.getNamespace());

    TransactionRunners.run(transactionRunner, context -> {
      getAppStateTable(context).deleteAll(id);
      AppMetadataStore metaStore = getAppMetadataStore(context);
      metaStore.deleteApplications(id.getNamespace());
      metaStore.deleteProgramHistory(id);
    });
  }

  @Override
  public Map<String, String> getRuntimeArguments(ProgramRunId programRunId) {
    return TransactionRunners.run(transactionRunner, context -> {
      RunRecordDetail runRecord = getAppMetadataStore(context).getRun(programRunId);
      if (runRecord != null) {
        return runRecord.getUserArgs();
      }
      LOG.debug("Runtime arguments for program {}, run {} not found. Returning empty.",
          programRunId.getProgram(), programRunId.getRun());
      return EMPTY_STRING_MAP;
    });
  }

  @Nullable
  @Override
  public ApplicationMeta getApplicationMetadata(ApplicationId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getApplicationMeta(getAppMetadataStore(context), id);
    });
  }

  @Nullable
  @Override
  public ApplicationSpecification getApplication(ApplicationId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getApplicationSpec(getAppMetadataStore(context), id);
    });
  }

  @Override
  public void scanApplications(int txBatchSize,
      BiConsumer<ApplicationId, ApplicationMeta> consumer) {
    scanApplications(ScanApplicationsRequest.builder().setLatestOnly(true).build(), txBatchSize, consumer);
  }

  @Override
  public boolean scanApplications(ScanApplicationsRequest request, int txBatchSize,
      BiConsumer<ApplicationId, ApplicationMeta> consumer) {

    AtomicReference<ScanApplicationsRequest> requestRef = new AtomicReference<>(request);
    AtomicReference<ApplicationId> lastKey = new AtomicReference<>();
    AtomicInteger currentLimit = new AtomicInteger(request.getLimit());

    while (currentLimit.get() > 0) {
      AtomicInteger count = new AtomicInteger();

      try {
        TransactionRunners.run(transactionRunner, context -> {
          getAppMetadataStore(context).scanApplications(requestRef.get(), entry -> {
            lastKey.set(entry.getKey());
            currentLimit.decrementAndGet();
            consumer.accept(entry.getKey(), entry.getValue());
            return count.incrementAndGet() < txBatchSize && currentLimit.get() > 0;
          });
        });
      } catch (UnsupportedOperationException e) {
        if (requestRef.get().getSortOrder() != SortOrder.DESC || count.get() != 0) {
          throw e;
        }
        return scanApplicationsWithReorder(requestRef.get(), txBatchSize, consumer);
      }

      if (lastKey.get() == null) {
        break;
      }
      ScanApplicationsRequest nextBatchRequest = ScanApplicationsRequest
          .builder(requestRef.get())
          .setScanFrom(lastKey.get())
          .setLimit(currentLimit.get())
          .build();
      requestRef.set(nextBatchRequest);
      lastKey.set(null);
    }
    return currentLimit.get() == 0;
  }

  /**
   * Special case where we are asked to get applications in descending order and the store does not
   * support it. We scan keys in large batches and serve backwards
   *
   * @return if we read records up to request limit
   */
  private boolean scanApplicationsWithReorder(ScanApplicationsRequest request,
      int txBatchSize,
      BiConsumer<ApplicationId, ApplicationMeta> consumer) {
    AtomicReference<ScanApplicationsRequest> forwardRequest =
        new AtomicReference<>(ScanApplicationsRequest.builder(request)
            .setSortOrder(SortOrder.ASC)
            .setScanFrom(request.getScanTo())
            .setScanTo(request.getScanFrom())
            .setLimit(Integer.MAX_VALUE)
            .build());
    AtomicBoolean needMoreScans = new AtomicBoolean(true);
    Deque<ApplicationId> ids = new ArrayDeque<>(1 + Math.min(maxReorderBatch, request.getLimit()));
    AtomicInteger currentLimit = new AtomicInteger(request.getLimit());
    while (needMoreScans.get()) {
      needMoreScans.set(false);
      TransactionRunners.run(transactionRunner, context -> {
        getAppMetadataStore(context).scanApplications(forwardRequest.get(), entry -> {
          if (ids.size() >= currentLimit.get()) {
            ids.removeFirst();
          }
          if (ids.size() >= maxReorderBatch) {
            ids.removeFirst();
            needMoreScans.set(true);
          }
          ids.add(entry.getKey());
          return true;
        });
      });
      if (needMoreScans.get()) {
        forwardRequest.set(ScanApplicationsRequest
            .builder(forwardRequest.get())
            .setScanTo(ids.getFirst())
            .build());
      }
      while (!ids.isEmpty()) {
        TransactionRunners.run(transactionRunner, context -> {
          for (int i = 0; !ids.isEmpty() && i < txBatchSize; i++) {
            ApplicationId id = ids.removeLast();
            consumer.accept(id, getApplicationMeta(getAppMetadataStore(context), id));
            currentLimit.decrementAndGet();
          }
        });
      }
    }
    return currentLimit.get() == 0;
  }

  @Override
  public Map<ApplicationId, ApplicationMeta> getApplications(Collection<ApplicationId> ids) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getApplicationsForAppIds(ids);
    });
  }

  @Override
  public void setAppSourceControlMeta(ApplicationId appId, SourceControlMeta sourceControlMeta) {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).setAppSourceControlMeta(appId, sourceControlMeta);
    });
  }

  @Override
  @Nullable
  public SourceControlMeta getAppSourceControlMeta(ApplicationReference appRef) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getAppSourceControlMeta(appRef);
    });
  }

  @Override
  public Map<ProgramReference, ProgramId> getPrograms(Collection<ProgramReference> references) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).filterProgramsExistence(references).stream()
          .collect(Collectors.toMap(ProgramId::getProgramReference, e -> e));
    });
  }

  @Override
  @Nullable
  public ApplicationMeta getLatest(ApplicationReference appRef) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getLatest(appRef);
    });
  }

  @Override
  public Collection<ApplicationId> getAllAppVersionsAppIds(ApplicationReference appRef) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getAllAppVersionsAppIds(appRef);
    });
  }

  @Override
  public WorkflowToken getWorkflowToken(WorkflowId workflowId, String workflowRunId) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getWorkflowToken(workflowId, workflowRunId);
    });
  }

  @Override
  public List<WorkflowNodeStateDetail> getWorkflowNodeStates(ProgramRunId workflowRunId) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getWorkflowNodeStates(workflowRunId);
    });
  }

  @VisibleForTesting
  void clear() {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).deleteAllAppMetadataTables();
      getWorkflowTable(context).deleteAll();
    });
  }

  @Nullable
  private ApplicationSpecification getApplicationSpec(AppMetadataStore mds,
      ApplicationId id) throws IOException, TableNotFoundException {
    return Optional.ofNullable(getApplicationMeta(mds, id)).map(ApplicationMeta::getSpec)
        .orElse(null);
  }

  @Nullable
  private ApplicationMeta getApplicationMeta(AppMetadataStore mds,
      ApplicationId id) throws IOException, TableNotFoundException {
    return mds.getApplication(id);
  }

  private static ApplicationSpecification replaceServiceSpec(ApplicationSpecification appSpec,
      String serviceName,
      ServiceSpecification serviceSpecification) {
    return new ApplicationSpecificationWithChangedServices(appSpec, serviceName,
        serviceSpecification);
  }

  private static class ApplicationSpecificationWithChangedServices extends
      ForwardingApplicationSpecification {

    private final String serviceName;
    private final ServiceSpecification serviceSpecification;

    private ApplicationSpecificationWithChangedServices(ApplicationSpecification delegate,
        String serviceName, ServiceSpecification serviceSpecification) {
      super(delegate);
      this.serviceName = serviceName;
      this.serviceSpecification = serviceSpecification;
    }

    @Override
    public Map<String, ServiceSpecification> getServices() {
      Map<String, ServiceSpecification> services = Maps.newHashMap(super.getServices());
      services.put(serviceName, serviceSpecification);
      return services;
    }
  }

  private static ServiceSpecification getServiceSpecOrFail(ProgramId id,
      @Nullable ApplicationSpecification appSpec) {
    if (appSpec == null) {
      throw new NoSuchElementException(
          "no such application @ namespace id: " + id.getNamespaceId()
              + ", app id: " + id.getApplication());
    }

    ServiceSpecification spec = appSpec.getServices().get(id.getProgram());
    if (spec == null) {
      throw new NoSuchElementException("no such service @ namespace id: " + id.getNamespace()
          + ", app id: " + id.getApplication()
          + ", service id: " + id.getProgram());
    }
    return spec;
  }

  private static WorkerSpecification getWorkerSpecOrFail(ProgramId id,
      @Nullable ApplicationSpecification appSpec) {
    if (appSpec == null) {
      throw new NoSuchElementException(
          "no such application @ namespace id: " + id.getNamespaceId()
              + ", app id: " + id.getApplication());
    }

    WorkerSpecification workerSpecification = appSpec.getWorkers().get(id.getProgram());
    if (workerSpecification == null) {
      throw new NoSuchElementException("no such worker @ namespace id: " + id.getNamespaceId()
          + ", app id: " + id.getApplication()
          + ", worker id: " + id.getProgram());
    }
    return workerSpecification;
  }

  private static ApplicationSpecification replaceWorkerInAppSpec(ApplicationSpecification appSpec,
      ProgramId id,
      WorkerSpecification workerSpecification) {
    return new ApplicationSpecificationWithChangedWorkers(appSpec, id.getProgram(),
        workerSpecification);
  }

  private static class ApplicationSpecificationWithChangedWorkers extends
      ForwardingApplicationSpecification {

    private final String workerId;
    private final WorkerSpecification workerSpecification;

    private ApplicationSpecificationWithChangedWorkers(ApplicationSpecification delegate,
        String workerId,
        WorkerSpecification workerSpec) {
      super(delegate);
      this.workerId = workerId;
      this.workerSpecification = workerSpec;
    }

    @Override
    public Map<String, WorkerSpecification> getWorkers() {
      Map<String, WorkerSpecification> workers = Maps.newHashMap(super.getWorkers());
      workers.put(workerId, workerSpecification);
      return workers;
    }
  }

  @Override
  public Set<RunId> getRunningInRange(long startTimeInSecs, long endTimeInSecs) {
    Set<RunId> runs = new HashSet<>();
    runs.addAll(TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getRunningInRangeActive(startTimeInSecs, endTimeInSecs);
    }));
    runs.addAll(TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getRunningInRangeCompleted(startTimeInSecs,
          endTimeInSecs);
    }));
    return runs;
  }

  @Override
  public long getProgramRunCount(ProgramId programId) throws NotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = getAppMetadataStore(context);
      ApplicationSpecification appSpec = getApplicationSpec(appMetadataStore,
          programId.getParent());
      // app not found
      if (appSpec == null) {
        throw new NotFoundException(programId.getParent());
      }
      ProgramSpecification programSpec = getExistingAppProgramSpecification(appSpec,
          programId.getProgramReference());
      // program not found
      if (programSpec == null) {
        throw new NotFoundException(programId);
      }
      return appMetadataStore.getProgramRunCount(programId);
    }, NotFoundException.class);
  }

  @Override
  public long getProgramTotalRunCount(ProgramReference programReference) throws NotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = getAppMetadataStore(context);
      ApplicationMeta appMeta = getLatest(programReference.getParent());
      // app not found
      if (appMeta == null) {
        throw new NotFoundException(programReference.getParent());
      }
      ProgramSpecification programSpec = getExistingAppProgramSpecification(appMeta.getSpec(),
          programReference);
      // program not found
      if (programSpec == null) {
        throw new NotFoundException(programReference);
      }
      return appMetadataStore.getProgramTotalRunCount(programReference);
    }, NotFoundException.class);
  }

  @Override
  public List<RunCountResult> getProgramTotalRunCounts(Collection<ProgramReference> programRefs) {
    return TransactionRunners.run(transactionRunner, context -> {
      List<RunCountResult> result = new ArrayList<>();
      AppMetadataStore appMetadataStore = getAppMetadataStore(context);

      Set<ProgramReference> existingPrograms = appMetadataStore.filterProgramsExistence(programRefs)
          .stream().map(
              ProgramId::getProgramReference).collect(Collectors.toSet());
      Map<ProgramReference, Long> runCounts = appMetadataStore.getProgramTotalRunCounts(
          existingPrograms);
      for (ProgramReference programRef : programRefs) {
        if (!existingPrograms.contains(programRef)) {
          result.add(new RunCountResult(programRef, null, new NotFoundException(programRef)));
        } else {
          result.add(new RunCountResult(programRef, runCounts.get(programRef), null));
        }
      }
      return result;
    });
  }

  @Override
  public Optional<byte[]> getState(AppStateKey request) throws ApplicationNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      verifyApplicationExists(context, request.getNamespaceId(), request.getAppName());
      return getAppStateTable(context).get(request);
    }, ApplicationNotFoundException.class);
  }

  @Override
  public void saveState(AppStateKeyValue request) throws ApplicationNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      verifyApplicationExists(context, request.getNamespaceId(), request.getAppName());
      getAppStateTable(context).save(request);
    }, ApplicationNotFoundException.class);
  }

  @Override
  public void deleteState(AppStateKey request) throws ApplicationNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      verifyApplicationExists(context, request.getNamespaceId(), request.getAppName());
      getAppStateTable(context).delete(request);
    }, ApplicationNotFoundException.class);
  }

  @Override
  public void deleteAllStates(NamespaceId namespaceId, String appName)
      throws ApplicationNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      verifyApplicationExists(context, namespaceId, appName);
      getAppStateTable(context).deleteAll(namespaceId, appName);
    }, ApplicationNotFoundException.class);
  }

  private AppStateTable getAppStateTable(StructuredTableContext context)
      throws TableNotFoundException {
    return new AppStateTable(context);
  }

  private void verifyApplicationExists(StructuredTableContext context,
      NamespaceId namespaceId,
      String appName) throws ApplicationNotFoundException, IOException {
    // Check if app exists
    ApplicationMeta latest = getAppMetadataStore(context).getLatest(
        namespaceId.appReference(appName));
    if (latest == null || latest.getSpec() == null) {
      throw new ApplicationNotFoundException(namespaceId.app(appName));
    }
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program}.
   *
   * @param appSpec the {@link ApplicationSpecification} of the existing application
   * @param programReference the {@link ProgramReference} for which the {@link
   *     ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}
   */
  @Nullable
  private ProgramSpecification getExistingAppProgramSpecification(ApplicationSpecification appSpec,
      ProgramReference programReference) {
    String programName = programReference.getProgram();
    ProgramType type = programReference.getType();
    switch (type) {
      case MAPREDUCE:
        return appSpec.getMapReduce().get(programName);
      case SPARK:
        return appSpec.getSpark().get(programName);
      case WORKFLOW:
        return appSpec.getWorkflows().get(programName);
      case SERVICE:
        return appSpec.getServices().get(programName);
      case WORKER:
        return appSpec.getWorkers().get(programName);
      default:
        return null;
    }
  }
}

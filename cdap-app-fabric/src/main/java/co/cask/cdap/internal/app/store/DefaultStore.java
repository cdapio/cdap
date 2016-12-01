/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.internal.app.ForwardingApplicationSpecification;
import co.cask.cdap.internal.app.ForwardingFlowSpecification;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.WorkflowStatistics;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Implementation of the Store that ultimately places data into MetaDataTable.
 */
public class DefaultStore implements Store {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultStore.class);
  private static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
  // mds is specific for metadata, we do not want to add workflow stats related information to the mds,
  // as it is not specifically metadata
  private static final DatasetId WORKFLOW_STATS_INSTANCE_ID = NamespaceId.SYSTEM.dataset("workflow.stats");
  private static final Gson GSON = new Gson();
  private static final Map<String, String> EMPTY_STRING_MAP = ImmutableMap.of();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final CConfiguration configuration;
  private final DatasetFramework dsFramework;
  private final Transactional transactional;

  @Inject
  public DefaultStore(CConfiguration conf, DatasetFramework framework, TransactionSystemClient txClient) {
    this.configuration = conf;
    this.dsFramework = framework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(framework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by app mds.
   *
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(Table.class.getName(), APP_META_INSTANCE_ID, DatasetProperties.EMPTY);
    framework.addInstance(Table.class.getName(), WORKFLOW_STATS_INSTANCE_ID, DatasetProperties.EMPTY);
  }

  private AppMetadataStore getAppMetadataStore(DatasetContext datasetContext) throws IOException,
                                                                                     DatasetManagementException {
    Table table = DatasetsUtil.getOrCreateDataset(datasetContext, dsFramework, APP_META_INSTANCE_ID,
                                                  Table.class.getName(), DatasetProperties.EMPTY);
    return new AppMetadataStore(table, configuration);
  }

  private WorkflowDataset getWorkflowDataset(DatasetContext datasetContext) throws IOException,
                                                                                   DatasetManagementException {
    Table table = DatasetsUtil.getOrCreateDataset(datasetContext, dsFramework, WORKFLOW_STATS_INSTANCE_ID,
                                                  Table.class.getName(), DatasetProperties.EMPTY);
    return new WorkflowDataset(table);
  }

  @Override
  public ProgramDescriptor loadProgram(final ProgramId id) throws IOException, ApplicationNotFoundException,
                                                                   ProgramNotFoundException {
    ApplicationMeta appMeta = txExecute(transactional, new TxCallable<ApplicationMeta>() {
      @Override
      public ApplicationMeta call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getApplication(id.getNamespace(), id.getApplication(), id.getVersion());
      }
    });

    if (appMeta == null) {
      throw new ApplicationNotFoundException(id.getParent());
    }

    if (!programExists(id, appMeta.getSpec())) {
      throw new ProgramNotFoundException(id);
    }

    return new ProgramDescriptor(id, appMeta.getSpec());
  }

  @Override
  public boolean compareAndSetStatus(final ProgramId id, final String pid, final ProgramRunStatus expectedStatus,
                                     final ProgramRunStatus newStatus) {
    Preconditions.checkArgument(expectedStatus != null, "Expected of program run should be defined");
    Preconditions.checkArgument(newStatus != null, "New state of program run should be defined");
    return txExecute(transactional, new TxCallable<Boolean>() {
      @Override
      public Boolean call(DatasetContext context) throws Exception {
        AppMetadataStore mds = getAppMetadataStore(context);
        RunRecordMeta target = mds.getRun(id, pid);
        if (target.getStatus() == expectedStatus) {
          long now = System.currentTimeMillis();
          long nowSecs = TimeUnit.MILLISECONDS.toSeconds(now);
          switch (newStatus) {
            case RUNNING:
              Map<String, String> runtimeArgs = GSON.fromJson(target.getProperties().get("runtimeArgs"),
                                                              STRING_MAP_TYPE);
              Map<String, String> systemArgs = GSON.fromJson(target.getProperties().get("systemArgs"),
                                                             STRING_MAP_TYPE);
              if (runtimeArgs == null) {
                runtimeArgs = EMPTY_STRING_MAP;
              }
              if (systemArgs == null) {
                systemArgs = EMPTY_STRING_MAP;
              }
              mds.recordProgramStart(id, pid, nowSecs, target.getTwillRunId(), runtimeArgs, systemArgs);
              break;
            case SUSPENDED:
              mds.recordProgramSuspend(id, pid);
              break;
            case COMPLETED:
            case KILLED:
            case FAILED:
              BasicThrowable failureCause = newStatus == ProgramRunStatus.FAILED
                ? new BasicThrowable(new Throwable("Marking run record as failed since no running program found."))
                : null;
              mds.recordProgramStop(id, pid, nowSecs, newStatus, failureCause);
              break;
            default:
              break;
          }
          return true;
        }
        return false;
      }
    });
  }

  @Override
  public void setStart(final ProgramId id, final String pid, final long startTime,
                       final String twillRunId, final Map<String, String> runtimeArgs,
                       final Map<String, String> systemArgs) {
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        getAppMetadataStore(context).recordProgramStart(id, pid, startTime, twillRunId, runtimeArgs, systemArgs);
      }
    });
  }

  @Override
  public void setStart(ProgramId id, String pid, long startTime) {
    setStart(id, pid, startTime, null, EMPTY_STRING_MAP, EMPTY_STRING_MAP);
  }

  @Override
  public void setStop(final ProgramId id, final String pid, final long endTime, final ProgramRunStatus runStatus) {
    setStop(id, pid, endTime, runStatus, null);
  }

  @Override
  public void setStop(final ProgramId id, final String pid, final long endTime, final ProgramRunStatus runStatus,
                      final BasicThrowable failureCause) {
    Preconditions.checkArgument(runStatus != null, "Run state of program run should be defined");
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        AppMetadataStore metaStore = getAppMetadataStore(context);
        metaStore.recordProgramStop(id, pid, endTime, runStatus, failureCause);

        // This block has been added so that completed workflow runs can be logged to the workflow dataset
        WorkflowId workflowId = new WorkflowId(id.getParent(), id.getProgram());
        if (id.getType() == ProgramType.WORKFLOW && runStatus == ProgramRunStatus.COMPLETED) {
          recordCompletedWorkflow(metaStore, getWorkflowDataset(context), workflowId, pid);
        }
        // todo: delete old history data
      }
    });
  }

  private void recordCompletedWorkflow(AppMetadataStore metaStore, WorkflowDataset workflowDataset,
                                       WorkflowId workflowId, String runId) {
    RunRecordMeta runRecord = metaStore.getRun(workflowId, runId);
    if (runRecord == null) {
      return;
    }
    ApplicationId app = workflowId.getParent();
    ApplicationSpecification appSpec = getApplicationSpec(metaStore, app);
    if (appSpec == null || appSpec.getWorkflows() == null
      || appSpec.getWorkflows().get(workflowId.getProgram()) == null) {
      LOG.warn("Missing ApplicationSpecification for {}, " +
                 "potentially caused by application removal right after stopping workflow {}", app, workflowId);
      return;
    }

    boolean workFlowNodeFailed = false;
    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(workflowId.getProgram());
    Map<String, WorkflowNode> nodeIdMap = workflowSpec.getNodeIdMap();
    final List<WorkflowDataset.ProgramRun> programRunsList = new ArrayList<>();
    for (Map.Entry<String, String> entry : runRecord.getProperties().entrySet()) {
      if (!("workflowToken".equals(entry.getKey()) || "runtimeArgs".equals(entry.getKey())
        || "workflowNodeState".equals(entry.getKey()))) {
        WorkflowActionNode workflowNode = (WorkflowActionNode) nodeIdMap.get(entry.getKey());
        ProgramType programType = ProgramType.valueOfSchedulableType(workflowNode.getProgram().getProgramType());
        ProgramId innerProgram = app.program(programType, entry.getKey());
        RunRecordMeta innerProgramRun = metaStore.getRun(innerProgram, entry.getValue());
        if (innerProgramRun != null && innerProgramRun.getStatus().equals(ProgramRunStatus.COMPLETED)) {
          Long stopTs = innerProgramRun.getStopTs();
          // since the program is completed, the stop ts cannot be null
          if (stopTs == null) {
            LOG.warn("Since the program has completed, expected its stop time to not be null. " +
                       "Not writing workflow completed record for Program = {}, Workflow = {}, Run = {}",
                     innerProgram, workflowId, runRecord);
            workFlowNodeFailed = true;
            break;
          }
          programRunsList.add(new WorkflowDataset.ProgramRun(entry.getKey(), entry.getValue(),
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

    workflowDataset.write(workflowId, runRecord, programRunsList);
  }

  @Override
  public void deleteWorkflowStats(final ApplicationId id) {
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        getWorkflowDataset(context).delete(id);
      }
    });
  }

  @Override
  public void setSuspend(final ProgramId id, final String pid) {
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
       getAppMetadataStore(context).recordProgramSuspend(id, pid);
      }
    });
  }

  @Override
  public void setResume(final ProgramId id, final String pid) {
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        getAppMetadataStore(context).recordProgramResumed(id, pid);
      }
    });
  }

  @Nullable
  public WorkflowStatistics getWorkflowStatistics(final WorkflowId id, final long startTime,
                                                  final long endTime, final List<Double> percentiles) {
    return txExecute(transactional, new TxCallable<WorkflowStatistics>() {
      @Override
      public WorkflowStatistics call(DatasetContext context) throws Exception {
        return getWorkflowDataset(context).getStatistics(id, startTime, endTime, percentiles);
      }
    });
  }

  @Override
  public WorkflowDataset.WorkflowRunRecord getWorkflowRun(final WorkflowId workflowId, final String runId) {
    return txExecute(transactional, new TxCallable<WorkflowDataset.WorkflowRunRecord>() {
      @Override
      public WorkflowDataset.WorkflowRunRecord call(DatasetContext context) throws Exception {
        return getWorkflowDataset(context).getRecord(workflowId, runId);
      }
    });
  }

  @Override
  public Collection<WorkflowDataset.WorkflowRunRecord> retrieveSpacedRecords(final WorkflowId workflow,
                                                                             final String runId,
                                                                             final int limit,
                                                                             final long timeInterval) {
    return txExecute(transactional, new TxCallable<Collection<WorkflowDataset.WorkflowRunRecord>>() {
      @Override
      public Collection<WorkflowDataset.WorkflowRunRecord> call(DatasetContext context) throws Exception {
        return getWorkflowDataset(context).getDetailsOfRange(workflow, runId, limit, timeInterval);
      }
    });
  }

  @Override
  public Map<ProgramRunId, RunRecordMeta> getRuns(final ProgramId id, final ProgramRunStatus status,
                                     final long startTime, final long endTime, final int limit) {
    return getRuns(id, status, startTime, endTime, limit, null);
  }

  @Override
  public Map<ProgramRunId, RunRecordMeta> getRuns(final ProgramId id, final ProgramRunStatus status,
                                     final long startTime, final long endTime, final int limit,
                                     @Nullable final Predicate<RunRecordMeta> filter) {
    return txExecute(transactional, new TxCallable<Map<ProgramRunId, RunRecordMeta>>() {
      @Override
      public Map<ProgramRunId, RunRecordMeta> call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getRuns(id, status, startTime, endTime, limit, filter);
      }
    });
  }

  @Override
  public Map<ProgramRunId, RunRecordMeta> getRuns(final ProgramRunStatus status,
                                                  final Predicate<RunRecordMeta> filter) {
    return txExecute(transactional, new TxCallable<Map<ProgramRunId, RunRecordMeta>>() {
      @Override
      public Map<ProgramRunId, RunRecordMeta> call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getRuns(status, filter);
      }
    });
  }

  @Override
  public Map<ProgramRunId, RunRecordMeta> getRuns(final Set<ProgramRunId> programRunIds) {
    return txExecute(transactional, new TxCallable<Map<ProgramRunId, RunRecordMeta>>() {
      @Override
      public Map<ProgramRunId, RunRecordMeta> call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getRuns(programRunIds);
      }
    });
  }

  /**
   * Returns run record for a given run.
   *
   * @param id program id
   * @param runId run id
   * @return run record for runid
   */
  @Override
  public RunRecordMeta getRun(final ProgramId id, final String runId) {
    return txExecute(transactional, new TxCallable<RunRecordMeta>() {
      @Override
      public RunRecordMeta call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getRun(id, runId);
      }
    });
  }

  @Override
  public void addApplication(final ApplicationId id, final ApplicationSpecification spec) {
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        getAppMetadataStore(context).writeApplication(id.getNamespace(), id.getApplication(), id.getVersion(), spec);
      }
    });
  }

  // todo: this method should be moved into DeletedProgramHandlerState, bad design otherwise
  @Override
  public List<ProgramSpecification> getDeletedProgramSpecifications(final ApplicationId id,
                                                                    ApplicationSpecification appSpec) {

    ApplicationMeta existing = txExecute(transactional, new TxCallable<ApplicationMeta>() {
      @Override
      public ApplicationMeta call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getApplication(id.getNamespace(), id.getApplication(), id.getVersion());
      }
    });

    List<ProgramSpecification> deletedProgramSpecs = Lists.newArrayList();

    if (existing != null) {
      ApplicationSpecification existingAppSpec = existing.getSpec();

      Map<String, ProgramSpecification> existingSpec = ImmutableMap.<String, ProgramSpecification>builder()
        .putAll(existingAppSpec.getMapReduce())
        .putAll(existingAppSpec.getSpark())
        .putAll(existingAppSpec.getWorkflows())
        .putAll(existingAppSpec.getFlows())
        .putAll(existingAppSpec.getServices())
        .putAll(existingAppSpec.getWorkers())
        .build();

      Map<String, ProgramSpecification> newSpec = ImmutableMap.<String, ProgramSpecification>builder()
        .putAll(appSpec.getMapReduce())
        .putAll(appSpec.getSpark())
        .putAll(appSpec.getWorkflows())
        .putAll(appSpec.getFlows())
        .putAll(appSpec.getServices())
        .putAll(appSpec.getWorkers())
        .build();

      MapDifference<String, ProgramSpecification> mapDiff = Maps.difference(existingSpec, newSpec);
      deletedProgramSpecs.addAll(mapDiff.entriesOnlyOnLeft().values());
    }

    return deletedProgramSpecs;
  }

  @Override
  public void addStream(final NamespaceId id, final StreamSpecification streamSpec) {
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        getAppMetadataStore(context).writeStream(id.getNamespace(), streamSpec);
      }
    });
  }

  @Override
  public StreamSpecification getStream(final NamespaceId id, final String name) {
    return txExecute(transactional, new TxCallable<StreamSpecification>() {
      @Override
      public StreamSpecification call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getStream(id.getNamespace(), name);
      }
    });
  }

  @Override
  public Collection<StreamSpecification> getAllStreams(final NamespaceId id) {
    return txExecute(transactional, new TxCallable<Collection<StreamSpecification>>() {
      @Override
      public Collection<StreamSpecification> call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getAllStreams(id.getNamespace());
      }
    });
  }

  @Override
  public FlowSpecification setFlowletInstances(final ProgramId id, final String flowletId, final int count) {
    Preconditions.checkArgument(count > 0, "Cannot change number of flowlet instances to %s", count);

    LOG.trace("Setting flowlet instances: namespace: {}, application: {}, flow: {}, flowlet: {}, " +
                "new instances count: {}", id.getNamespace(), id.getApplication(), id.getProgram(), flowletId, count);

    FlowSpecification flowSpec = txExecute(transactional, new TxCallable<FlowSpecification>() {
      @Override
      public FlowSpecification call(DatasetContext context) throws Exception {
        AppMetadataStore metaStore = getAppMetadataStore(context);
        ApplicationSpecification appSpec = getAppSpecOrFail(metaStore, id);
        ApplicationSpecification newAppSpec = updateFlowletInstancesInAppSpec(appSpec, id, flowletId, count);
        metaStore.updateAppSpec(id.getNamespace(), id.getApplication(), id.getVersion(), newAppSpec);
        return appSpec.getFlows().get(id.getProgram());
      }
    });

    LOG.trace("Set flowlet instances: namespace: {}, application: {}, flow: {}, flowlet: {}, instances now: {}",
              id.getNamespaceId(), id.getApplication(), id.getProgram(), flowletId, count);
    return flowSpec;
  }

  @Override
  public int getFlowletInstances(final ProgramId id, final String flowletId) {
    return txExecute(transactional, new TxCallable<Integer>() {
      @Override
      public Integer call(DatasetContext context) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(getAppMetadataStore(context), id);
        FlowSpecification flowSpec = getFlowSpecOrFail(id, appSpec);
        FlowletDefinition flowletDef = getFlowletDefinitionOrFail(flowSpec, flowletId, id);
        return flowletDef.getInstances();
      }
    });
  }

  @Override
  public void setWorkerInstances(final ProgramId id, final int instances) {
    Preconditions.checkArgument(instances > 0, "Cannot change number of worker instances to %s", instances);
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        AppMetadataStore metaStore = getAppMetadataStore(context);
        ApplicationSpecification appSpec = getAppSpecOrFail(metaStore, id);
        WorkerSpecification workerSpec = getWorkerSpecOrFail(id, appSpec);
        WorkerSpecification newSpecification = new WorkerSpecification(workerSpec.getClassName(),
                                                                       workerSpec.getName(),
                                                                       workerSpec.getDescription(),
                                                                       workerSpec.getProperties(),
                                                                       workerSpec.getDatasets(),
                                                                       workerSpec.getResources(),
                                                                       instances);
        ApplicationSpecification newAppSpec = replaceWorkerInAppSpec(appSpec, id, newSpecification);
        metaStore.updateAppSpec(id.getNamespace(), id.getApplication(), id.getVersion(), newAppSpec);

      }
    });

    LOG.trace("Setting program instances: namespace: {}, application: {}, worker: {}, new instances count: {}",
              id.getNamespaceId(), id.getApplication(), id.getProgram(), instances);
  }

  @Override
  public void setServiceInstances(final ProgramId id, final int instances) {
    Preconditions.checkArgument(instances > 0, "Cannot change number of service instances to %s", instances);
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        AppMetadataStore metaStore = getAppMetadataStore(context);
        ApplicationSpecification appSpec = getAppSpecOrFail(metaStore, id);
        ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);

        // Create a new spec copy from the old one, except with updated instances number
        serviceSpec = new ServiceSpecification(serviceSpec.getClassName(), serviceSpec.getName(),
                                               serviceSpec.getDescription(), serviceSpec.getHandlers(),
                                               serviceSpec.getResources(), instances);

        ApplicationSpecification newAppSpec = replaceServiceSpec(appSpec, id.getProgram(), serviceSpec);
        metaStore.updateAppSpec(id.getNamespace(), id.getApplication(), id.getVersion(), newAppSpec);
      }
    });

    LOG.trace("Setting program instances: namespace: {}, application: {}, service: {}, new instances count: {}",
              id.getNamespaceId(), id.getApplication(), id.getProgram(), instances);
  }

  @Override
  public int getServiceInstances(final ProgramId id) {
    return txExecute(transactional, new TxCallable<Integer>() {
      @Override
      public Integer call(DatasetContext context) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(getAppMetadataStore(context), id);
        ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);
        return serviceSpec.getInstances();
      }
    });
  }

  @Override
  public int getWorkerInstances(final ProgramId id) {
    return txExecute(transactional, new TxCallable<Integer>() {
      @Override
      public Integer call(DatasetContext context) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(getAppMetadataStore(context), id);
        WorkerSpecification workerSpec = getWorkerSpecOrFail(id, appSpec);
        return workerSpec.getInstances();
      }
    });
  }

  @Override
  public void removeApplication(final ApplicationId id) {
    LOG.trace("Removing application: namespace: {}, application: {}", id.getNamespace(), id.getApplication());

    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        AppMetadataStore metaStore = getAppMetadataStore(context);
        metaStore.deleteApplication(id.getNamespace(), id.getApplication(), id.getVersion());
        metaStore.deleteProgramHistory(id.getNamespace(), id.getApplication());
      }
    });
  }

  @Override
  public void removeAllApplications(final NamespaceId id) {
    LOG.trace("Removing all applications of namespace with id: {}", id.getNamespace());

    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        AppMetadataStore metaStore = getAppMetadataStore(context);
        metaStore.deleteApplications(id.getNamespace());
        metaStore.deleteProgramHistory(id.getNamespace());
      }
    });
  }

  @Override
  public void removeAll(final NamespaceId id) {
    LOG.trace("Removing all applications of namespace with id: {}", id.getNamespace());

    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        AppMetadataStore metaStore = getAppMetadataStore(context);
        metaStore.deleteApplications(id.getNamespace());
        metaStore.deleteAllStreams(id.getNamespace());
        metaStore.deleteProgramHistory(id.getNamespace());
      }
    });
  }

  @Override
  public Map<String, String> getRuntimeArguments(final ProgramRunId programRunId) {
    return txExecute(transactional, new TxCallable<Map<String, String>>() {
      @Override
      public Map<String, String> call(DatasetContext context) throws Exception {
        RunRecordMeta runRecord = getAppMetadataStore(context).getRun(programRunId.getParent(), programRunId.getRun());
        if (runRecord != null) {
          Map<String, String> properties = runRecord.getProperties();
          Map<String, String> runtimeArgs = GSON.fromJson(properties.get("runtimeArgs"), STRING_MAP_TYPE);
          if (runtimeArgs != null) {
            return runtimeArgs;
          }
        }
        LOG.debug("Runtime arguments for program {}, run {} not found. Returning empty.",
                  programRunId.getProgram(), programRunId.getRun());
        return EMPTY_STRING_MAP;
      }
    });
  }

  @Nullable
  @Override
  public ApplicationSpecification getApplication(final ApplicationId id) {
    return txExecute(transactional, new TxCallable<ApplicationSpecification>() {
      @Override
      public ApplicationSpecification call(DatasetContext context) throws Exception {
        return getApplicationSpec(getAppMetadataStore(context), id);
      }
    });
  }

  @Override
  public Collection<ApplicationSpecification> getAllApplications(final NamespaceId id) {
    return txExecute(transactional, new TxCallable<Collection<ApplicationSpecification>>() {
      @Override
      public Collection<ApplicationSpecification> call(DatasetContext context) throws Exception {
        return Lists.transform(
          getAppMetadataStore(context).getAllApplications(id.getNamespace()),
          new Function<ApplicationMeta, ApplicationSpecification>() {
            @Override
            public ApplicationSpecification apply(ApplicationMeta input) {
              return input.getSpec();
            }
        });
      }
    });
  }

  @Override
  public Collection<ApplicationSpecification> getAllAppVersions(final ApplicationId id) {
    return txExecute(transactional, new TxCallable<Collection<ApplicationSpecification>>() {
      @Override
      public Collection<ApplicationSpecification> call(DatasetContext context) throws Exception {
        return Lists.transform(
          getAppMetadataStore(context).getAllAppVersions(id.getNamespace(), id.getApplication()),
          new Function<ApplicationMeta, ApplicationSpecification>() {
            @Override
            public ApplicationSpecification apply(ApplicationMeta input) {
              return input.getSpec();
            }
          });
      }
    });
  }

  @Override
  public Collection<ApplicationId> getAllAppVersionsAppIds(final ApplicationId id) {
    return txExecute(transactional, new TxCallable<Collection<ApplicationId>>() {
      @Override
      public Collection<ApplicationId> call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getAllAppVersionsAppIds(id.getNamespace(), id.getApplication());
      }
    });
  }

  @Override
  public void addSchedule(final ProgramId program, final ScheduleSpecification scheduleSpecification) {
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        AppMetadataStore metaStore = getAppMetadataStore(context);
        ApplicationSpecification appSpec = getAppSpecOrFail(metaStore, program);
        Map<String, ScheduleSpecification> schedules = Maps.newHashMap(appSpec.getSchedules());
        String scheduleName = scheduleSpecification.getSchedule().getName();
        Preconditions.checkArgument(!schedules.containsKey(scheduleName), "Schedule with the name '" +
          scheduleName + "' already exists.");
        schedules.put(scheduleSpecification.getSchedule().getName(), scheduleSpecification);
        ApplicationSpecification newAppSpec = new AppSpecificationWithChangedSchedules(appSpec, schedules);
        metaStore.updateAppSpec(program.getNamespace(), program.getApplication(), program.getVersion(), newAppSpec);
      }
    });
  }

  @Override
  public void deleteSchedule(final ProgramId program, final String scheduleName) {
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        AppMetadataStore metaStore = getAppMetadataStore(context);
        ApplicationSpecification appSpec = getAppSpecOrFail(metaStore, program);
        Map<String, ScheduleSpecification> schedules = Maps.newHashMap(appSpec.getSchedules());
        ScheduleSpecification removed = schedules.remove(scheduleName);
        if (removed == null) {
          throw new NoSuchElementException("No such schedule @ namespace id: " + program.getNamespaceId() +
                                             ", app id: " + program.getApplication() +
                                             ", program id: " + program.getProgram() +
                                             ", schedule name: " + scheduleName);
        }

        ApplicationSpecification newAppSpec = new AppSpecificationWithChangedSchedules(appSpec, schedules);
        metaStore.updateAppSpec(program.getNamespace(), program.getApplication(), program.getVersion(), newAppSpec);
      }
    });
  }

  private static class AppSpecificationWithChangedSchedules extends ForwardingApplicationSpecification {
    private final Map<String, ScheduleSpecification> newSchedules;

    private AppSpecificationWithChangedSchedules(ApplicationSpecification delegate,
                                                 Map<String, ScheduleSpecification> newSchedules) {
      super(delegate);
      this.newSchedules = newSchedules;
    }

    @Override
    public Map<String, ScheduleSpecification> getSchedules() {
      return newSchedules;
    }
  }

  @Override
  public boolean applicationExists(final ApplicationId id) {
    return getApplication(id) != null;
  }

  @Override
  public boolean programExists(final ProgramId id) {
    ApplicationSpecification appSpec = getApplication(id.getParent());
    return appSpec != null && programExists(id, appSpec);
  }

  private boolean programExists(ProgramId id, ApplicationSpecification appSpec) {
    switch (id.getType()) {
      case FLOW:      return appSpec.getFlows().containsKey(id.getProgram());
      case MAPREDUCE: return appSpec.getMapReduce().containsKey(id.getProgram());
      case SERVICE:   return appSpec.getServices().containsKey(id.getProgram());
      case SPARK:     return appSpec.getSpark().containsKey(id.getProgram());
      case WEBAPP:    return false;
      case WORKER:    return appSpec.getWorkers().containsKey(id.getProgram());
      case WORKFLOW:  return appSpec.getWorkflows().containsKey(id.getProgram());
      default:        throw new IllegalArgumentException("Unexpected ProgramType " + id.getType());
    }
  }

  @Override
  public void updateWorkflowToken(final ProgramRunId workflowRunId, final WorkflowToken token) {
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        getAppMetadataStore(context).updateWorkflowToken(workflowRunId, token);
      }
    });
  }

  @Override
  public WorkflowToken getWorkflowToken(final WorkflowId workflowId, final String workflowRunId) {
    return txExecute(transactional, new TxCallable<WorkflowToken>() {
      @Override
      public WorkflowToken call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getWorkflowToken(workflowId, workflowRunId);
      }
    });
  }

  @Override
  public void addWorkflowNodeState(final ProgramRunId workflowRunId, final WorkflowNodeStateDetail nodeStateDetail) {
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        getAppMetadataStore(context).addWorkflowNodeState(workflowRunId, nodeStateDetail);
      }
    });
  }

  @Override
  public List<WorkflowNodeStateDetail> getWorkflowNodeStates(final ProgramRunId workflowRunId) {
    return txExecute(transactional, new TxCallable<List<WorkflowNodeStateDetail>>() {
      @Override
      public List<WorkflowNodeStateDetail> call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getWorkflowNodeStates(workflowRunId);
      }
    });
  }

  @VisibleForTesting
  void clear() throws Exception {
    truncate(dsFramework.getAdmin(APP_META_INSTANCE_ID, null));
    truncate(dsFramework.getAdmin(WORKFLOW_STATS_INSTANCE_ID, null));
  }

  public void upgradeAppVersion() throws Exception {
    txExecute(transactional, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        getAppMetadataStore(context).upgradeVersionKeys();
      }
    });
  }

  private void truncate(DatasetAdmin admin) throws Exception {
    if (admin != null) {
      admin.truncate();
    }
  }

  private ApplicationSpecification getApplicationSpec(AppMetadataStore mds, ApplicationId id) {
    ApplicationMeta meta = mds.getApplication(id.getNamespace(), id.getApplication(), id.getVersion());
    return meta == null ? null : meta.getSpec();
  }

  private static ApplicationSpecification replaceServiceSpec(ApplicationSpecification appSpec,
                                                             String serviceName,
                                                             ServiceSpecification serviceSpecification) {
    return new ApplicationSpecificationWithChangedServices(appSpec, serviceName, serviceSpecification);
  }

  private static final class ApplicationSpecificationWithChangedServices extends ForwardingApplicationSpecification {
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

  private static FlowletDefinition getFlowletDefinitionOrFail(FlowSpecification flowSpec,
                                                              String flowletId, ProgramId id) {
    FlowletDefinition flowletDef = flowSpec.getFlowlets().get(flowletId);
    if (flowletDef == null) {
      throw new NoSuchElementException("no such flowlet @ namespace id: " + id.getNamespace() +
                                           ", app id: " + id.getApplication() +
                                           ", flow id: " + id.getProgram() +
                                           ", flowlet id: " + flowletId);
    }
    return flowletDef;
  }

  private static FlowSpecification getFlowSpecOrFail(ProgramId id, ApplicationSpecification appSpec) {
    FlowSpecification flowSpec = appSpec.getFlows().get(id.getProgram());
    if (flowSpec == null) {
      throw new NoSuchElementException("no such flow @ namespace id: " + id.getNamespace() +
                                           ", app id: " + id.getApplication() +
                                           ", flow id: " + id.getProgram());
    }
    return flowSpec;
  }

  private static ServiceSpecification getServiceSpecOrFail(ProgramId id, ApplicationSpecification appSpec) {
    ServiceSpecification spec = appSpec.getServices().get(id.getProgram());
    if (spec == null) {
      throw new NoSuchElementException("no such service @ namespace id: " + id.getNamespace() +
                                           ", app id: " + id.getApplication() +
                                           ", service id: " + id.getProgram());
    }
    return spec;
  }

  private static WorkerSpecification getWorkerSpecOrFail(ProgramId id, ApplicationSpecification appSpec) {
    WorkerSpecification workerSpecification = appSpec.getWorkers().get(id.getProgram());
    if (workerSpecification == null) {
      throw new NoSuchElementException("no such worker @ namespace id: " + id.getNamespaceId() +
                                         ", app id: " + id.getApplication() +
                                         ", worker id: " + id.getProgram());
    }
    return workerSpecification;
  }

  private static ApplicationSpecification updateFlowletInstancesInAppSpec(ApplicationSpecification appSpec,
                                                                          ProgramId id, String flowletId, int count) {

    FlowSpecification flowSpec = getFlowSpecOrFail(id, appSpec);
    FlowletDefinition flowletDef = getFlowletDefinitionOrFail(flowSpec, flowletId, id);

    final FlowletDefinition adjustedFlowletDef = new FlowletDefinition(flowletDef, count);
    return replaceFlowletInAppSpec(appSpec, id, flowSpec, adjustedFlowletDef);
  }

  private ApplicationSpecification getAppSpecOrFail(AppMetadataStore mds, ProgramId id) {
    ApplicationSpecification appSpec = getApplicationSpec(mds, id.getParent());
    if (appSpec == null) {
      throw new NoSuchElementException("no such application @ namespace id: " + id.getNamespaceId() +
                                           ", app id: " + id.getApplication());
    }
    return appSpec;
  }

  private static class FlowSpecificationWithChangedFlowlets extends ForwardingFlowSpecification {
    private final FlowletDefinition adjustedFlowletDef;

    private FlowSpecificationWithChangedFlowlets(FlowSpecification delegate,
                                                 FlowletDefinition adjustedFlowletDef) {
      super(delegate);
      this.adjustedFlowletDef = adjustedFlowletDef;
    }

    @Override
    public Map<String, FlowletDefinition> getFlowlets() {
      Map<String, FlowletDefinition> flowlets = Maps.newHashMap(super.getFlowlets());
      flowlets.put(adjustedFlowletDef.getFlowletSpec().getName(), adjustedFlowletDef);
      return flowlets;
    }
  }

  private static ApplicationSpecification replaceFlowletInAppSpec(final ApplicationSpecification appSpec,
                                                                  final ProgramId id,
                                                                  final FlowSpecification flowSpec,
                                                                  final FlowletDefinition adjustedFlowletDef) {
    // as app spec is immutable we have to do this trick
    return replaceFlowInAppSpec(appSpec, id, new FlowSpecificationWithChangedFlowlets(flowSpec, adjustedFlowletDef));
  }

  private static ApplicationSpecification replaceFlowInAppSpec(final ApplicationSpecification appSpec,
                                                               final ProgramId id,
                                                               final FlowSpecification newFlowSpec) {
    // as app spec is immutable we have to do this trick
    return new ApplicationSpecificationWithChangedFlows(appSpec, id.getProgram(), newFlowSpec);
  }

  private static final class ApplicationSpecificationWithChangedFlows extends ForwardingApplicationSpecification {
    private final FlowSpecification newFlowSpec;
    private final String flowId;

    private ApplicationSpecificationWithChangedFlows(ApplicationSpecification delegate,
                                                     String flowId, FlowSpecification newFlowSpec) {
      super(delegate);
      this.newFlowSpec = newFlowSpec;
      this.flowId = flowId;
    }

    @Override
    public Map<String, FlowSpecification> getFlows() {
      Map<String, FlowSpecification> flows = Maps.newHashMap(super.getFlows());
      flows.put(flowId, newFlowSpec);
      return flows;
    }
  }

  private static ApplicationSpecification replaceWorkerInAppSpec(final ApplicationSpecification appSpec,
                                                                 final ProgramId id,
                                                                 final WorkerSpecification workerSpecification) {
    return new ApplicationSpecificationWithChangedWorkers(appSpec, id.getProgram(), workerSpecification);
  }

  private static final class ApplicationSpecificationWithChangedWorkers extends ForwardingApplicationSpecification {
    private final String workerId;
    private final WorkerSpecification workerSpecification;

    private ApplicationSpecificationWithChangedWorkers(ApplicationSpecification delegate, String workerId,
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

  public Set<RunId> getRunningInRange(final long startTimeInSecs, final long endTimeInSecs) {
    return txExecute(transactional, new TxCallable<Set<RunId>>() {
      @Override
      public Set<RunId> call(DatasetContext context) throws Exception {
        return getAppMetadataStore(context).getRunningInRange(startTimeInSecs, endTimeInSecs);
      }
    });
  }

  /**
   * Executes the given callable with a transaction. Any exception will result in {@link RuntimeException}.
   */
  private <V> V txExecute(Transactional transactional, TxCallable<V> callable) {
    try {
      return Transactions.execute(transactional, callable);
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }

  /**
   * Executes the given runnable with a transaction. Any exception will result in {@link RuntimeException}.
   */
  private void txExecute(Transactional transactional, TxRunnable runnable) {
    try {
      transactional.execute(runnable);
    } catch (TransactionFailureException e) {
      throw Transactions.propagate(e);
    }
  }
}

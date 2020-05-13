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
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
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
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.app.ForwardingApplicationSpecification;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.ProgramHistory;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunCountResult;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.WorkflowStatistics;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Implementation of the Store that ultimately places data into MetaDataTable.
 */
public class DefaultStore implements Store {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStore.class);

  // mds is specific for metadata, we do not want to add workflow stats related information to the mds,
  // as it is not specifically metadata
  private static final DatasetId WORKFLOW_STATS_INSTANCE_ID = NamespaceId.SYSTEM.dataset("workflow.stats");
  private static final Gson GSON = new Gson();
  private static final Map<String, String> EMPTY_STRING_MAP = ImmutableMap.of();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private TransactionRunner transactionRunner;

  @Inject
  public DefaultStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by app mds.
   *
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(Table.class.getName(), AppMetadataStore.APP_META_INSTANCE_ID, DatasetProperties.EMPTY);
    framework.addInstance(Table.class.getName(), WORKFLOW_STATS_INSTANCE_ID, DatasetProperties.EMPTY);
  }

  private AppMetadataStore getAppMetadataStore(StructuredTableContext context) {
    return AppMetadataStore.create(context);
  }

  private WorkflowTable getWorkflowTable(StructuredTableContext context) throws TableNotFoundException {
    return new WorkflowTable(context.getTable(StoreDefinition.WorkflowStore.WORKFLOW_STATISTICS));
  }

  @Override
  public ProgramDescriptor loadProgram(ProgramId id) throws NotFoundException {
    ApplicationMeta appMeta = TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getApplication(id.getNamespace(), id.getApplication(), id.getVersion());
    });

    if (appMeta == null) {
      throw new ApplicationNotFoundException(id.getParent());
    }

    Store.ensureProgramExists(id, appMeta.getSpec());
    return new ProgramDescriptor(id, appMeta.getSpec());
  }

  @Override
  public void setProvisioning(ProgramRunId id, Map<String, String> runtimeArgs,
                              Map<String, String> systemArgs, byte[] sourceId, ArtifactId artifactId) {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).recordProgramProvisioning(id, runtimeArgs, systemArgs, sourceId, artifactId);
    });
  }

  @Override
  public void setProvisioned(ProgramRunId id, int numNodes, byte[] sourceId) {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).recordProgramProvisioned(id, numNodes, sourceId);
    });
  }

  @Override
  public void setStart(ProgramRunId id, @Nullable String twillRunId, Map<String, String> systemArgs, byte[] sourceId) {
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
      LOG.warn("Missing ApplicationSpecification for {}, " +
                 "potentially caused by application removal right after stopping workflow {}", app, workflowId);
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
        ProgramType programType = ProgramType.valueOfSchedulableType(workflowNode.getProgram().getProgramType());
        ProgramId innerProgram = app.program(programType, entry.getKey());
        RunRecordDetail innerProgramRun = metaStore.getRun(innerProgram.run(entry.getValue()));
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
  public WorkflowStatistics getWorkflowStatistics(WorkflowId id, long startTime,
                                                  long endTime, List<Double> percentiles) {
    return TransactionRunners.run(transactionRunner, context -> {
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
  public Collection<WorkflowTable.WorkflowRunRecord> retrieveSpacedRecords(WorkflowId workflow,
                                                                           String runId,
                                                                           int limit,
                                                                           long timeInterval) {
    return  TransactionRunners.run(transactionRunner, context -> {
      return getWorkflowTable(context).getDetailsOfRange(workflow, runId, limit, timeInterval);
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
  public Map<ProgramRunId, RunRecordDetail> getRuns(ApplicationId applicationId, ProgramRunStatus status, int limit,
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
  public int countActiveRuns(@Nullable Integer limit) {
    return TransactionRunners.run(transactionRunner,
                                  context -> (int) getAppMetadataStore(context).countActiveRuns(limit));
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
  public Map<ProgramId, Collection<RunRecordDetail>> getActiveRuns(Collection<ProgramId> programIds) {
    return TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = getAppMetadataStore(context);
      // Get the active runs for programs that exist
      return getAppMetadataStore(context).getActiveRuns(appMetadataStore.filterProgramsExistence(programIds));
    });
  }

  /**
   * Returns run record for a given run.
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

  @Override
  public void addApplication(ApplicationId id, ApplicationSpecification spec) {
    TransactionRunners.run(transactionRunner, context -> {
      getAppMetadataStore(context).writeApplication(id.getNamespace(), id.getApplication(), id.getVersion(), spec);
    });
  }

  // todo: this method should be moved into DeletedProgramHandlerState, bad design otherwise
  @Override
  public List<ProgramSpecification> getDeletedProgramSpecifications(ApplicationId id,
                                                                    ApplicationSpecification appSpec) {

    ApplicationMeta existing = TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getApplication(id.getNamespace(), id.getApplication(), id.getVersion());
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
    Preconditions.checkArgument(instances > 0, "Cannot change number of worker instances to %s", instances);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metaStore = getAppMetadataStore(context);
      ApplicationSpecification appSpec = getAppSpecOrFail(metaStore, id);
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

    LOG.trace("Setting program instances: namespace: {}, application: {}, worker: {}, new instances count: {}",
              id.getNamespaceId(), id.getApplication(), id.getProgram(), instances);
  }

  @Override
  public void setServiceInstances(ProgramId id, int instances) {
    Preconditions.checkArgument(instances > 0, "Cannot change number of service instances to %s", instances);
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metaStore = getAppMetadataStore(context);
      ApplicationSpecification appSpec = getAppSpecOrFail(metaStore, id);
      ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);

      // Create a new spec copy from the old one, except with updated instances number
      serviceSpec = new ServiceSpecification(serviceSpec.getClassName(), serviceSpec.getName(),
                                             serviceSpec.getDescription(), serviceSpec.getHandlers(),
                                             serviceSpec.getResources(), instances, serviceSpec.getPlugins());

      ApplicationSpecification newAppSpec = replaceServiceSpec(appSpec, id.getProgram(), serviceSpec);
      metaStore.updateAppSpec(id.getParent(), newAppSpec);
    });

    LOG.trace("Setting program instances: namespace: {}, application: {}, service: {}, new instances count: {}",
              id.getNamespaceId(), id.getApplication(), id.getProgram(), instances);
  }

  @Override
  public int getServiceInstances(ProgramId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      ApplicationSpecification appSpec = getAppSpecOrFail(getAppMetadataStore(context), id);
      ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);
      return serviceSpec.getInstances();
    });
  }

  @Override
  public int getWorkerInstances(ProgramId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      ApplicationSpecification appSpec = getAppSpecOrFail(getAppMetadataStore(context), id);
      WorkerSpecification workerSpec = getWorkerSpecOrFail(id, appSpec);
      return workerSpec.getInstances();
    });
  }

  @Override
  public void removeApplication(ApplicationId id) {
    LOG.trace("Removing application: namespace: {}, application: {}", id.getNamespace(), id.getApplication());

    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metaStore = getAppMetadataStore(context);
      metaStore.deleteApplication(id.getNamespace(), id.getApplication(), id.getVersion());
      metaStore.deleteProgramHistory(id.getNamespace(), id.getApplication(), id.getVersion());
    });
  }

  @Override
  public void removeAll(NamespaceId id) {
    LOG.trace("Removing all applications of namespace with id: {}", id.getNamespace());

    TransactionRunners.run(transactionRunner, context -> {
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
  public ApplicationSpecification getApplication(ApplicationId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getApplicationSpec(getAppMetadataStore(context), id);
    });
  }

  @Override
  public Collection<ApplicationSpecification> getAllApplications(NamespaceId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getAllApplications(id.getNamespace()).stream()
        .map(ApplicationMeta::getSpec).collect(Collectors.toList());
    });
  }

  @Override
  public Map<ApplicationId, ApplicationSpecification> getApplications(Collection<ApplicationId> ids) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getApplicationsForAppIds(ids).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSpec()));
    });
  }

  @Override
  public Collection<ApplicationSpecification> getAllAppVersions(ApplicationId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getAllAppVersions(id.getNamespace(), id.getApplication()).stream()
        .map(ApplicationMeta::getSpec).collect(Collectors.toList());
    });
  }

  @Override
  public Collection<ApplicationId> getAllAppVersionsAppIds(ApplicationId id) {
    return TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getAllAppVersionsAppIds(id.getNamespace(), id.getApplication());
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

  private ApplicationSpecification getApplicationSpec(AppMetadataStore mds, ApplicationId id)
    throws IOException, TableNotFoundException {
    ApplicationMeta meta = mds.getApplication(id.getNamespace(), id.getApplication(), id.getVersion());
    return meta == null ? null : meta.getSpec();
  }

  private static ApplicationSpecification replaceServiceSpec(ApplicationSpecification appSpec,
                                                             String serviceName,
                                                             ServiceSpecification serviceSpecification) {
    return new ApplicationSpecificationWithChangedServices(appSpec, serviceName, serviceSpecification);
  }

  private static class ApplicationSpecificationWithChangedServices extends ForwardingApplicationSpecification {
    private String serviceName;
    private ServiceSpecification serviceSpecification;

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

  private ApplicationSpecification getAppSpecOrFail(AppMetadataStore mds, ProgramId id)
    throws IOException, TableNotFoundException {
    return getAppSpecOrFail(mds, id.getParent());
  }

  private ApplicationSpecification getAppSpecOrFail(AppMetadataStore mds, ApplicationId id)
    throws IOException, TableNotFoundException {
    ApplicationSpecification appSpec = getApplicationSpec(mds, id);
    if (appSpec == null) {
      throw new NoSuchElementException("no such application @ namespace id: " + id.getNamespaceId() +
                                         ", app id: " + id.getApplication());
    }
    return appSpec;
  }

  private static ApplicationSpecification replaceWorkerInAppSpec(ApplicationSpecification appSpec,
                                                                 ProgramId id,
                                                                 WorkerSpecification workerSpecification) {
    return new ApplicationSpecificationWithChangedWorkers(appSpec, id.getProgram(), workerSpecification);
  }

  private static class ApplicationSpecificationWithChangedWorkers extends ForwardingApplicationSpecification {
    private String workerId;
    private WorkerSpecification workerSpecification;

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

  @Override
  public Set<RunId> getRunningInRange(long startTimeInSecs, long endTimeInSecs) {
    Set<RunId> runs = new HashSet<>();
    runs.addAll(TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getRunningInRangeActive(startTimeInSecs, endTimeInSecs);
    }));
    runs.addAll(TransactionRunners.run(transactionRunner, context -> {
      return getAppMetadataStore(context).getRunningInRangeCompleted(startTimeInSecs, endTimeInSecs);
    }));
    return runs;
  }

  @Override
  public long getProgramRunCount(ProgramId programId) throws NotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = getAppMetadataStore(context);
      ApplicationSpecification appSpec = getApplicationSpec(appMetadataStore, programId.getParent());
      // app not found
      if (appSpec == null) {
        throw new NotFoundException(programId.getParent());
      }
      ProgramSpecification programSpec = getExistingAppProgramSpecification(appSpec, programId);
      // program not found
      if (programSpec == null) {
        throw new NotFoundException(programId);
      }
      return appMetadataStore.getProgramRunCount(programId);
    }, NotFoundException.class);
  }

  @Override
  public List<RunCountResult> getProgramRunCounts(Collection<ProgramId> programIds) {
    return TransactionRunners.run(transactionRunner, context -> {
      List<RunCountResult> result = new ArrayList<>();
      AppMetadataStore appMetadataStore = getAppMetadataStore(context);

      Map<ProgramId, Long> runCounts = appMetadataStore.getProgramRunCounts(
        appMetadataStore.filterProgramsExistence(programIds));

      for (ProgramId programId : programIds) {
        Long count = runCounts.get(programId);
        if (count == null) {
          result.add(new RunCountResult(programId, null, new NotFoundException(programId)));
        } else {
          result.add(new RunCountResult(programId, count, null));
        }
      }
      return result;
    });
  }

  @Override
  public List<ProgramHistory> getRuns(Collection<ProgramId> programs, ProgramRunStatus status,
                                      long startTime, long endTime, int limitPerProgram) {
    return TransactionRunners.run(transactionRunner, context -> {
      List<ProgramHistory> result = new ArrayList<>(programs.size());
      AppMetadataStore appMetadataStore = getAppMetadataStore(context);

      Set<ProgramId> existingPrograms = appMetadataStore.filterProgramsExistence(programs);

      for (ProgramId programId : programs) {
        if (!existingPrograms.contains(programId)) {
          result.add(new ProgramHistory(programId, Collections.emptyList(), new ProgramNotFoundException(programId)));
          continue;
        }

        List<RunRecord> runs = appMetadataStore.getRuns(programId, status, startTime, endTime,
                                                        limitPerProgram, null)
          .values().stream()
          .map(record -> RunRecord.builder(record).build()).collect(Collectors.toList());
        result.add(new ProgramHistory(programId, runs, null));
      }

      return result;
    });
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program}.
   * @param appSpec the {@link ApplicationSpecification} of the existing application
   * @param programId the {@link ProgramId program} for which the {@link ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}
   */
  @Nullable
  private ProgramSpecification getExistingAppProgramSpecification(ApplicationSpecification appSpec,
                                                                  ProgramId programId) {
    String programName = programId.getProgram();
    ProgramType type = programId.getType();
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

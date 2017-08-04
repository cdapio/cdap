/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.messaging.TopicMessageIdStore;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.tephra.TxConstants;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

import static com.google.common.base.Predicates.and;

/**
 * Store for application metadata
 */
public class AppMetadataStore extends MetadataStoreDataset implements TopicMessageIdStore {
  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStore.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type BYTE_TYPE = new TypeToken<byte[]>() { }.getType();
  private static final String TYPE_APP_META = "appMeta";
  private static final String TYPE_STREAM = "stream";
  private static final String TYPE_RUN_RECORD_STARTING = "runRecordStarting";
  private static final String TYPE_RUN_RECORD_STARTED = "runRecordStarted";
  private static final String TYPE_RUN_RECORD_SUSPENDED = "runRecordSuspended";
  private static final String TYPE_RUN_RECORD_COMPLETED = "runRecordCompleted";
  private static final String TYPE_WORKFLOW_NODE_STATE = "wns";
  private static final String TYPE_WORKFLOW_TOKEN = "wft";
  private static final String TYPE_NAMESPACE = "namespace";
  private static final String TYPE_MESSAGE = "msg";

  private final CConfiguration cConf;
  private final AtomicBoolean upgradeComplete;

  private static final Function<RunRecordMeta, RunId> RUN_RECORD_META_TO_RUN_ID_FUNCTION =
    new Function<RunRecordMeta, RunId>() {
      @Override
      public RunId apply(RunRecordMeta runRecordMeta) {
        return RunIds.fromString(runRecordMeta.getPid());
      }
    };

  public AppMetadataStore(Table table, CConfiguration cConf, AtomicBoolean upgradeComplete) {
    super(table);
    this.cConf = cConf;
    this.upgradeComplete = upgradeComplete;
  }

  @Override
  protected <T> byte[] serialize(T value) {
    return Bytes.toBytes(GSON.toJson(value));
  }

  @Override
  protected <T> T deserialize(byte[] serialized, Type typeOfT) {
    return GSON.fromJson(Bytes.toString(serialized), typeOfT);
  }

  @Nullable
  public ApplicationMeta getApplication(String namespaceId, String appId, String versionId) {
    ApplicationMeta appMeta = getFirst(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId, versionId).build(),
                                       ApplicationMeta.class);

    if (appMeta != null) {
      return appMeta;
    }

    if (!upgradeComplete.get() && versionId.equals(ApplicationId.DEFAULT_VERSION)) {
      appMeta = get(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId).build(), ApplicationMeta.class);
    }
    return appMeta;
  }

  public List<ApplicationMeta> getAllApplications(String namespaceId) {
    return list(new MDSKey.Builder().add(TYPE_APP_META, namespaceId).build(), ApplicationMeta.class);
  }

  public List<ApplicationMeta> getAllAppVersions(String namespaceId, String appId) {
    return list(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId).build(), ApplicationMeta.class);
  }

  public List<ApplicationId> getAllAppVersionsAppIds(String namespaceId, String appId) {
    List<ApplicationId> appIds = new ArrayList<>();
    for (MDSKey key : listKV(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId).build(),
                             ApplicationMeta.class).keySet()) {
      MDSKey.Splitter splitter = key.split();
      splitter.skipBytes(); // skip recordType
      splitter.skipBytes(); // skip namespaceId
      splitter.skipBytes(); // skip appId
      String versionId = splitter.hasRemaining() ? splitter.getString() : ApplicationId.DEFAULT_VERSION;
      appIds.add(new NamespaceId(namespaceId).app(appId, versionId));
    }
    return appIds;
  }

  public void writeApplication(String namespaceId, String appId, String versionId, ApplicationSpecification spec) {
    if (!upgradeComplete.get() && versionId.equals(ApplicationId.DEFAULT_VERSION)) {
      MDSKey mdsKey = new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId).build();
      ApplicationMeta appMeta = get(mdsKey, ApplicationMeta.class);
      // If app meta exists for the application without a version, delete that key.
      if (appMeta != null) {
        delete(mdsKey);
      }
    }
    write(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId, versionId).build(),
          new ApplicationMeta(appId, spec));
  }

  public void deleteApplication(String namespaceId, String appId, String versionId) {
    if (!upgradeComplete.get() && versionId.equals(ApplicationId.DEFAULT_VERSION)) {
      MDSKey mdsKey = new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId).build();
      ApplicationMeta appMeta = get(mdsKey, ApplicationMeta.class);
      // If app meta exists for the application without a version, delete only that key.
      if (appMeta != null) {
        delete(mdsKey);
      }
    }
    deleteAll(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId, versionId).build());
  }

  public void deleteApplications(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_APP_META, namespaceId).build());
  }

  // todo: do we need appId? may be use from appSpec?
  public void updateAppSpec(String namespaceId, String appId, String versionId, ApplicationSpecification spec) {
    LOG.trace("App spec to be updated: id: {}: spec: {}", appId, GSON.toJson(spec));
    MDSKey key = new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId, versionId).build();
    MDSKey versionLessKey = null;
    ApplicationMeta existing = getFirst(key, ApplicationMeta.class);
    ApplicationMeta updated;

    // Check again without the version to account for old data format if might not have been upgraded yet
    if (!upgradeComplete.get() && existing == null && (versionId.equals(ApplicationId.DEFAULT_VERSION))) {
      versionLessKey = new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId).build();
      existing = get(versionLessKey, ApplicationMeta.class);
    }

    if (existing == null) {
      String msg = String.format("No meta for namespace %s app %s exists", namespaceId, appId);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    updated = ApplicationMeta.updateSpec(existing, spec);
    LOG.trace("Application exists in mds: id: {}, spec: {}", existing);

    // Delete the old spec since the old spec has been replaced with this one.
    if (versionLessKey != null) {
      delete(versionLessKey);
    }
    write(key, updated);
  }

  /**
   * Return the {@link List} of {@link WorkflowNodeStateDetail} for a given Workflow run.
   */
  public List<WorkflowNodeStateDetail> getWorkflowNodeStates(ProgramRunId workflowRunId) {
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId.getParent())
      .add(workflowRunId.getRun()).build();

    List<WorkflowNodeStateDetail> nodeStateDetails = list(key, WorkflowNodeStateDetail.class);

    // Check again without the version to account for old data format since they might not have been updated yet
    // Since all the programs needs to be stopped before upgrade tool is run, either we will have node state details for
    // one specific run-id either in the old format or in the new format.
    if (!upgradeComplete.get() && nodeStateDetails.isEmpty() &&
      workflowRunId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      key = getVersionLessProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId.getParent())
        .add(workflowRunId.getRun()).build();
      nodeStateDetails = list(key, WorkflowNodeStateDetail.class);
    }
    return nodeStateDetails;
  }

  /**
   * This method is called to associate node state of custom action with the Workflow run.
   *
   * @param workflowRunId the run for which node state is to be added
   * @param nodeStateDetail node state details to be added
   */
  public void addWorkflowNodeState(ProgramRunId workflowRunId, WorkflowNodeStateDetail nodeStateDetail) {
    // Node states will be stored with following key:
    // workflowNodeState.namespace.app.WORKFLOW.workflowName.workflowRun.workflowNodeId
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId.getParent())
      .add(workflowRunId.getRun()).add(nodeStateDetail.getNodeId()).build();

    write(key, nodeStateDetail);
  }

  private void addWorkflowNodeState(ProgramId programId, String pid, Map<String, String> systemArgs,
                                    ProgramRunStatus status, @Nullable BasicThrowable failureCause) {
    String workflowNodeId = systemArgs.get(ProgramOptionConstants.WORKFLOW_NODE_ID);
    String workflowName = systemArgs.get(ProgramOptionConstants.WORKFLOW_NAME);
    String workflowRun = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);

    ApplicationId appId = Ids.namespace(programId.getNamespace()).app(programId.getApplication());
    ProgramRunId workflowRunId = appId.workflow(workflowName).run(workflowRun);

    // Node states will be stored with following key:
    // workflowNodeState.namespace.app.WORKFLOW.workflowName.workflowRun.workflowNodeId
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId.getParent())
      .add(workflowRun).add(workflowNodeId).build();

    WorkflowNodeStateDetail nodeStateDetail = new WorkflowNodeStateDetail(workflowNodeId,
                                                                          ProgramRunStatus.toNodeStatus(status),
                                                                          pid, failureCause);

    write(key, nodeStateDetail);

    // Get the run record of the Workflow which started this program
    key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, workflowRunId.getParent())
            .add(workflowRunId.getRun()).build();

    RunRecordMeta record = get(key, RunRecordMeta.class);
    if (record != null) {
      // Update the parent Workflow run record by adding node id and program run id in the properties
      Map<String, String> properties = record.getProperties();
      properties.put(workflowNodeId, pid);
      write(key, new RunRecordMeta(record, properties));
    }
  }

  public void recordProgramStart(ProgramId programId, String pid, long startTs, String twillRunId,
                                 Map<String, String> runtimeArgs, Map<String, String> systemArgs) {
    MDSKey.Builder builder = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTING, programId);
    recordProgramStart(programId, pid, startTs, twillRunId, runtimeArgs, systemArgs, builder);
  }

  public void recordProgramRunning(ProgramId programId, String pid, long stateChangeTime, String twillRunId) {
    MDSKey.Builder builder = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, programId);
    recordProgramRunning(programId, pid, stateChangeTime, twillRunId, builder);
  }

  @VisibleForTesting
  void recordProgramRunningOldFormat(ProgramId programId, String pid, long stateChangeTime, String twillRunId) {
    MDSKey.Builder builder = getVersionLessProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, programId);
    recordProgramRunning(programId, pid, stateChangeTime, twillRunId, builder);
  }

  private void recordProgramStart(ProgramId programId, String pid, long startTs, String twillRunId,
                                  Map<String, String> runtimeArgs, Map<String, String> systemArgs,
                                  MDSKey.Builder keyBuilder) {
    String workflowRunId = null;
    if (systemArgs != null && systemArgs.containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      // Program is started by Workflow. Add row corresponding to its node state.
      addWorkflowNodeState(programId, pid, systemArgs, ProgramRunStatus.STARTING, null);
      workflowRunId = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);
    }

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("runtimeArgs", GSON.toJson(runtimeArgs, MAP_STRING_STRING_TYPE));
    if (workflowRunId != null) {
      builder.put("workflowrunid", workflowRunId);
    }

    MDSKey key = keyBuilder.add(pid).build();
    RunRecordMeta meta =
      new RunRecordMeta(pid, startTs, null, null, ProgramRunStatus.STARTING, builder.build(), systemArgs, twillRunId);
    write(key, meta);
  }

  private void recordProgramRunning(ProgramId programId, String pid, long runTs, String twillRunId,
                                    MDSKey.Builder keyBuilder) {
    MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTING, programId).add(pid).build();
    RunRecordMeta existing = get(key, RunRecordMeta.class);

    if (existing == null) {
      // No starting record, but program could have just resumed, so it should already have a running record
      key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, programId).add(pid).build();
      existing = get(key, RunRecordMeta.class);

      if (existing == null) {
        // No started or running record exists, so throw an error
        String msg = String.format("No meta for run record for namespace %s app %s program type %s " +
                                   "program %s pid %s exists",
                                   programId.getNamespace(), programId.getApplication(), programId.getType().name(),
                                   programId.getProgram(), pid);
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
    }

    Map<String, String> systemArgs = existing.getSystemArgs();
    if (systemArgs != null && systemArgs.containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      // Program was started by Workflow. Add row corresponding to its node state.
      addWorkflowNodeState(programId, pid, systemArgs, ProgramRunStatus.RUNNING, null);
    }

    // Delete the old run record
    deleteAll(key);

    // Build the key for TYPE_RUN_RECORD_STARTED
    key = keyBuilder.add(pid).build();
    // The existing record's properties already contains the workflowRunId
    RunRecordMeta meta = new RunRecordMeta(pid, existing.getStartTs(), runTs, null, ProgramRunStatus.RUNNING,
                                           existing.getProperties(), systemArgs, twillRunId);
    write(key, meta);
  }

  public void recordProgramSuspend(ProgramId program, String pid) {
    recordProgramSuspendResume(program, pid, "suspend");
  }

  public void recordProgramResumed(ProgramId program, String pid) {
    recordProgramSuspendResume(program, pid, "resume");
  }

  private void recordProgramSuspendResume(ProgramId programId, String pid, String action) {
    String fromType = TYPE_RUN_RECORD_STARTED;
    String toType = TYPE_RUN_RECORD_SUSPENDED;
    ProgramRunStatus toStatus = ProgramRunStatus.SUSPENDED;

    if (action.equals("resume")) {
      fromType = TYPE_RUN_RECORD_SUSPENDED;
      toType = TYPE_RUN_RECORD_STARTED;
      toStatus = ProgramRunStatus.RUNNING;
    }

    MDSKey key = getProgramKeyBuilder(fromType, programId)
      .add(pid)
      .build();
    RunRecordMeta record = get(key, RunRecordMeta.class);

    // Check without the version string only for default version
    if (!upgradeComplete.get() && record == null && (programId.getVersion().equals(ApplicationId.DEFAULT_VERSION))) {
      key = getVersionLessProgramKeyBuilder(fromType, programId)
        .add(pid)
        .build();
      record = get(key, RunRecordMeta.class);
    }

    // We can also suspend a workflow that is in the starting state
    if (record == null && fromType.equals(TYPE_RUN_RECORD_STARTED)) {
      key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTING, programId)
        .add(pid)
        .build();
      record = get(key, RunRecordMeta.class);
    }

    if (record == null) {
      String msg = String.format("No meta for %s run record for namespace %s app %s program type %s " +
                                 "program %s pid %s exists", action.equals("suspend") ? "running" : "suspended",
                                 programId.getNamespace(), programId.getApplication(), programId.getType().name(),
                                 programId.getProgram(), pid);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    // Since the key contains the RunId/PID in addition to the programId, it is ok to deleteAll.
    deleteAll(key);

    key = getProgramKeyBuilder(toType, programId)
      .add(pid)
      .build();
    write(key, new RunRecordMeta(record, null, toStatus));
  }

  public void recordProgramStop(ProgramId programId, String pid, long stopTs, ProgramRunStatus runStatus,
                                @Nullable BasicThrowable failureCause) {
    MDSKey.Builder builder = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId);
    recordProgramStop(programId, pid, stopTs, runStatus, failureCause, builder);
  }

  @VisibleForTesting
  void recordProgramStopOldFormat(ProgramId programId, String pid, long stopTs, ProgramRunStatus runStatus,
                                @Nullable BasicThrowable failureCause) {
    MDSKey.Builder builder = getVersionLessProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId);
    recordProgramStop(programId, pid, stopTs, runStatus, failureCause, builder);
  }

  private void recordProgramStop(ProgramId programId, String pid, long stopTs, ProgramRunStatus runStatus,
                                 @Nullable BasicThrowable failureCause, MDSKey.Builder builder) {
    MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, programId)
      .add(pid)
      .build();
    RunRecordMeta existing = getFirst(key, RunRecordMeta.class);

    // Check without the version string only for default version
    if (!upgradeComplete.get() && existing == null && (programId.getVersion().equals(ApplicationId.DEFAULT_VERSION))) {
      key = getVersionLessProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, programId)
        .add(pid)
        .build();
      existing = getFirst(key, RunRecordMeta.class);
    }

    if (existing == null) {
      // No running record, so program could have started and been killed / encountered an error before running
      key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTING, programId)
        .add(pid)
        .build();
      existing = getFirst(key, RunRecordMeta.class);
    }

    if (existing == null) {
      // No running or started record, but a program can be suspended
      key = getProgramKeyBuilder(TYPE_RUN_RECORD_SUSPENDED, programId)
        .add(pid)
        .build();
      existing = getFirst(key, RunRecordMeta.class);
    }

    if (existing == null) {
      // No started or running or suspended record exists, so throw an error
      String msg = String.format("No meta for run record for namespace %s app %s version %s program type %s " +
                                 "program %s pid %s exists",
                                 programId.getNamespace(), programId.getApplication(), programId.getVersion(),
                                 programId.getType().name(), programId.getProgram(), pid);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    deleteAll(key);

    // Record in the workflow
    Map<String, String> systemArgs = existing.getSystemArgs();
    if (systemArgs != null && systemArgs.containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      addWorkflowNodeState(programId, pid, systemArgs, runStatus, failureCause);
    }

    key = builder.add(getInvertedTsKeyPart(existing.getStartTs())).add(pid).build();
    write(key, new RunRecordMeta(existing, stopTs, runStatus));
  }

  public Map<ProgramRunId, RunRecordMeta> getRuns(ProgramRunStatus status, Predicate<RunRecordMeta> filter) {
    return getRuns(null, status, 0L, Long.MAX_VALUE, Integer.MAX_VALUE, filter);
  }

  public Map<ProgramRunId, RunRecordMeta> getRuns(Set<ProgramRunId> programRunIds) {
    return getRuns(programRunIds, Integer.MAX_VALUE);
  }

  private Map<ProgramRunId, RunRecordMeta> getRuns(Set<ProgramRunId> programRunIds, int limit) {
    Map<ProgramRunId, RunRecordMeta> resultMap = new LinkedHashMap<>();
    resultMap.putAll(getActiveRuns(programRunIds, limit));
    resultMap.putAll(getSuspendedRuns(programRunIds, limit - resultMap.size()));
    resultMap.putAll(getHistoricalRuns(programRunIds, limit - resultMap.size()));
    return resultMap;
  }

  public Map<ProgramRunId, RunRecordMeta> getRuns(@Nullable ProgramId programId, final ProgramRunStatus status,
                                                  long startTime, long endTime, int limit,
                                                  @Nullable Predicate<RunRecordMeta> filter) {
    switch(status) {
      case ALL:
        Map<ProgramRunId, RunRecordMeta> runRecords =  new LinkedHashMap<>();
        runRecords.putAll(getActiveRuns(programId, startTime, endTime, limit, filter));
        runRecords.putAll(getSuspendedRuns(programId, startTime, endTime, limit - runRecords.size(), filter));
        runRecords.putAll(getHistoricalRuns(programId, status, startTime, endTime, limit - runRecords.size(), filter));
        return runRecords;
      case STARTING:
        return getNonCompleteRuns(programId, TYPE_RUN_RECORD_STARTING, startTime, endTime, limit, filter);
      case RUNNING:
        return getNonCompleteRuns(programId, TYPE_RUN_RECORD_STARTED, startTime, endTime, limit, filter);
      case SUSPENDED:
        return getSuspendedRuns(programId, startTime, endTime, limit, filter);
      default:
        return getHistoricalRuns(programId, status, startTime, endTime, limit, filter);
    }
  }

  // TODO: getRun is duplicated in cdap-watchdog AppMetadataStore class.
  // Any changes made here will have to be made over there too.
  // JIRA https://issues.cask.co/browse/CDAP-2172
  public RunRecordMeta getRun(ProgramId program, final String runid) {
    // Query active run record first
    RunRecordMeta running = getUnfinishedRun(program, TYPE_RUN_RECORD_STARTED, runid);
    // If program is running, this will be non-null
    if (running != null) {
      return running;
    }

    // Then query for started run record
    RunRecordMeta starting = getUnfinishedRun(program, TYPE_RUN_RECORD_STARTING, runid);
    if (starting != null) {
      return starting;
    }

    // If program is not running, query completed run records
    RunRecordMeta complete = getCompletedRun(program, runid);
    if (complete != null) {
      return complete;
    }

    // Else query suspended run records
    return getUnfinishedRun(program, TYPE_RUN_RECORD_SUSPENDED, runid);
  }

  /**
   * @return run records for runs that do not have start time in mds key for the run record.
   */
  private RunRecordMeta getUnfinishedRun(ProgramId programId, String recordType, String runid) {
    MDSKey runningKey = getProgramKeyBuilder(recordType, programId)
      .add(runid)
      .build();

    RunRecordMeta runRecordMeta = get(runningKey, RunRecordMeta.class);

    if (!upgradeComplete.get() && runRecordMeta == null &&
      programId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      runningKey = getVersionLessProgramKeyBuilder(recordType, programId).add(runid).build();
      return get(runningKey, RunRecordMeta.class);
    }

    return runRecordMeta;
  }

  private RunRecordMeta getCompletedRun(ProgramId programId, final String runid) {
    MDSKey completedKey = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId).build();
    RunRecordMeta runRecordMeta = getCompletedRun(completedKey, runid);

    if (!upgradeComplete.get() && runRecordMeta == null &&
      programId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      completedKey = getVersionLessProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId).build();
      return getCompletedRun(completedKey, runid);
    }

    return runRecordMeta;
  }

  private RunRecordMeta getCompletedRun(MDSKey completedKey, final String runid) {
    // Get start time from RunId
    long programStartSecs = RunIds.getTime(RunIds.fromString(runid), TimeUnit.SECONDS);
    if (programStartSecs > -1) {
      // If start time is found, run a get
      MDSKey key = new MDSKey.Builder(completedKey)
        .add(getInvertedTsKeyPart(programStartSecs))
        .add(runid)
        .build();

      return get(key, RunRecordMeta.class);
    } else {
      // If start time is not found, scan the table (backwards compatibility when run ids were random UUIDs)
      MDSKey startKey = new MDSKey.Builder(completedKey).add(getInvertedTsScanKeyPart(Long.MAX_VALUE)).build();
      MDSKey stopKey = new MDSKey.Builder(completedKey).add(getInvertedTsScanKeyPart(0)).build();
      List<RunRecordMeta> runRecords =
        list(startKey, stopKey, RunRecordMeta.class, 1,  // Should have only one record for this runid
             new Predicate<RunRecordMeta>() {
               @Override
               public boolean apply(RunRecordMeta input) {
                 return input.getPid().equals(runid);
               }
             });
      return Iterables.getFirst(runRecords, null);
    }
  }

  private Map<ProgramRunId, RunRecordMeta> getSuspendedRuns(@Nullable ProgramId programId, long startTime, long endTime,
                                                             int limit, @Nullable Predicate<RunRecordMeta> filter) {
    return getNonCompleteRuns(programId, TYPE_RUN_RECORD_SUSPENDED, startTime, endTime, limit, filter);
  }

  private Map<ProgramRunId, RunRecordMeta> getActiveRuns(@Nullable ProgramId programId, final long startTime,
                                                         final long endTime, int limit,
                                                         @Nullable Predicate<RunRecordMeta> filter) {
    Map<ProgramRunId, RunRecordMeta> activeRunRecords =
      getNonCompleteRuns(programId, TYPE_RUN_RECORD_STARTING, startTime, endTime, limit, filter);
    activeRunRecords.putAll(getNonCompleteRuns(programId, TYPE_RUN_RECORD_STARTED, startTime, endTime, limit, filter));
    return activeRunRecords;
  }

  private Map<ProgramRunId, RunRecordMeta> getNonCompleteRuns(@Nullable ProgramId programId, String recordType,
                                                              final long startTime, final long endTime, int limit,
                                                              Predicate<RunRecordMeta> filter) {
    Predicate<RunRecordMeta> valuePredicate = andPredicate(new Predicate<RunRecordMeta>() {

      @Override
      public boolean apply(RunRecordMeta input) {
        return input.getStartTs() >= startTime && input.getStartTs() < endTime;
      }
    }, filter);

    if (programId == null || !programId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      MDSKey key = getProgramKeyBuilder(recordType, programId).build();
      return getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, limit, valuePredicate));
    }

    Predicate<MDSKey> keyPredicate = new AppVersionPredicate(ApplicationId.DEFAULT_VERSION);
    MDSKey key = getProgramKeyBuilder(recordType, programId).build();
    Map<MDSKey, RunRecordMeta> newRecords = listKV(key, null, RunRecordMeta.class, limit, keyPredicate, valuePredicate);

    int remaining = limit - newRecords.size();
    if (remaining > 0 && !upgradeComplete.get()) {
      // We need to scan twice since the scan key is modified based on whether we include the app version or not.
      key = getVersionLessProgramKeyBuilder(recordType, programId).build();
      Map<MDSKey, RunRecordMeta> oldRecords = listKV(key, null, RunRecordMeta.class, remaining, keyPredicate,
                                                     valuePredicate);
      newRecords.putAll(oldRecords);
    }
    return getProgramRunIdMap(newRecords);
  }

  private Map<ProgramRunId, RunRecordMeta> getSuspendedRuns(Set<ProgramRunId> programRunIds, int limit) {
    return getRunsForRunIds(programRunIds, TYPE_RUN_RECORD_SUSPENDED, limit);
  }

  private Map<ProgramRunId, RunRecordMeta> getActiveRuns(Set<ProgramRunId> programRunIds, int limit) {
    Map<ProgramRunId, RunRecordMeta> activeRunRecords =
        getRunsForRunIds(programRunIds, TYPE_RUN_RECORD_STARTING, limit);
    activeRunRecords.putAll(getRunsForRunIds(programRunIds, TYPE_RUN_RECORD_STARTED, limit));
    return activeRunRecords;
  }

  private Map<ProgramRunId, RunRecordMeta> getHistoricalRuns(Set<ProgramRunId> programRunIds, int limit) {
    return getRunsForRunIds(programRunIds, TYPE_RUN_RECORD_COMPLETED, limit);
  }

  private Map<ProgramRunId, RunRecordMeta> getRunsForRunIds(final Set<ProgramRunId> runIds, String recordType,
                                                            int limit) {
    Set<MDSKey> keySet = new HashSet<>();
    boolean includeVersionLessKeys = !upgradeComplete.get();
    for (ProgramRunId programRunId : runIds) {
      keySet.add(getProgramKeyBuilder(recordType, programRunId).build());
      if (includeVersionLessKeys && programRunId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
        keySet.add(getVersionLessProgramKeyBuilder(recordType, programRunId).build());
      }
    }

    Predicate<KeyValue<RunRecordMeta>> combinedFilter = new Predicate<KeyValue<RunRecordMeta>>() {
      @Override
      public boolean apply(KeyValue<RunRecordMeta> input) {
        ProgramId programId = getProgramID(input.getKey());
        RunRecordMeta meta = input.getValue();
        ProgramRunId programRunId = programId.run(meta.getPid());
        return runIds.contains(programRunId);
      }
    };

    Map<MDSKey, RunRecordMeta> returnMap = listKV(keySet, RunRecordMeta.class, limit, combinedFilter);
    return getProgramRunIdMap(returnMap);
  }

  /** Converts MDSkeys in the map to ProgramIDs
   *
   * @param keymap map with keys as MDSkeys
   * @return map with keys as program IDs
   */
  private Map<ProgramRunId, RunRecordMeta> getProgramRunIdMap(Map<MDSKey, RunRecordMeta> keymap) {
    Map<ProgramRunId, RunRecordMeta> programRunIdMap = new LinkedHashMap<>();
    for (Map.Entry<MDSKey, RunRecordMeta> entry : keymap.entrySet()) {
      ProgramId programId = getProgramID(entry.getKey());
      programRunIdMap.put(programId.run(entry.getValue().getPid()), entry.getValue());
    }
    return programRunIdMap;
  }

  private Map<ProgramRunId, RunRecordMeta> getHistoricalRuns(@Nullable ProgramId programId, ProgramRunStatus status,
                                                             final long startTime, final long endTime, int limit,
                                                             @Nullable Predicate<RunRecordMeta> filter) {
    if (programId == null || !programId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId).build();
      return getHistoricalRuns(key, status, startTime, endTime, limit, null, filter);
    }

    Predicate<MDSKey> keyPredicate = new AppVersionPredicate(ApplicationId.DEFAULT_VERSION);
    MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId).build();
    Map<ProgramRunId, RunRecordMeta> newRecords = getHistoricalRuns(
      key, status, startTime, endTime, limit, keyPredicate, filter);

    int remaining = limit - newRecords.size();
    if (remaining > 0 && !upgradeComplete.get()) {
      // We need to scan twice since the key is modified again in getHistoricalRuns since we want to use the
      // endTime and startTime to reduce the scan range
      key = getVersionLessProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId).build();
      Map<ProgramRunId, RunRecordMeta> oldRecords = getHistoricalRuns(
        key, status, startTime, endTime, remaining, keyPredicate, filter);
      newRecords.putAll(oldRecords);
    }
    return newRecords;
  }

  private Map<ProgramRunId, RunRecordMeta> getHistoricalRuns(MDSKey historyKey, ProgramRunStatus status,
                                                             final long startTime, final long endTime, int limit,
                                                             @Nullable Predicate<MDSKey> keyFiter,
                                                             @Nullable Predicate<RunRecordMeta> valueFilter) {
    MDSKey start = new MDSKey.Builder(historyKey).add(getInvertedTsScanKeyPart(endTime)).build();
    MDSKey stop = new MDSKey.Builder(historyKey).add(getInvertedTsScanKeyPart(startTime)).build();
    if (status.equals(ProgramRunStatus.ALL)) {
      //return all records (successful and failed)
      return getProgramRunIdMap(listKV(start, stop, RunRecordMeta.class, limit, keyFiter,
                                       valueFilter == null ? Predicates.<RunRecordMeta>alwaysTrue() : valueFilter));
    }

    if (status.equals(ProgramRunStatus.COMPLETED)) {
      return getProgramRunIdMap(listKV(start, stop, RunRecordMeta.class, limit, keyFiter,
                                       andPredicate(getPredicate(ProgramController.State.COMPLETED), valueFilter)));
    }
    if (status.equals(ProgramRunStatus.KILLED)) {
      return getProgramRunIdMap(listKV(start, stop, RunRecordMeta.class, limit, keyFiter,
                                       andPredicate(getPredicate(ProgramController.State.KILLED), valueFilter)));
    }
    return getProgramRunIdMap(listKV(start, stop, RunRecordMeta.class, limit, keyFiter,
                                     andPredicate(getPredicate(ProgramController.State.ERROR), valueFilter)));
  }

  private Predicate<RunRecordMeta> getPredicate(final ProgramController.State state) {
    return new Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(RunRecordMeta record) {
        return record.getStatus().equals(state.getRunStatus());
      }
    };
  }

  private Predicate<RunRecordMeta> andPredicate(Predicate<RunRecordMeta> first,
                                                @Nullable Predicate<RunRecordMeta> second) {
    if (second != null) {
      return and(first, second);
    }
    return first;
  }

  private long getInvertedTsKeyPart(long endTime) {
    return Long.MAX_VALUE - endTime;
  }

  /**
   * Returns inverted scan key for given time. The scan key needs to be adjusted to maintain the property that
   * start key is inclusive and end key is exclusive on a scan. Since when you invert start key, it becomes end key and
   * vice-versa.
   */
  private long getInvertedTsScanKeyPart(long time) {
    long invertedTsKey = getInvertedTsKeyPart(time);
    return invertedTsKey < Long.MAX_VALUE ? invertedTsKey + 1 : invertedTsKey;
  }

  public void writeStream(String namespaceId, StreamSpecification spec) {
    write(new MDSKey.Builder().add(TYPE_STREAM, namespaceId, spec.getName()).build(), spec);
  }

  public StreamSpecification getStream(String namespaceId, String name) {
    return getFirst(new MDSKey.Builder().add(TYPE_STREAM, namespaceId, name).build(), StreamSpecification.class);
  }

  public List<StreamSpecification> getAllStreams(String namespaceId) {
    return list(new MDSKey.Builder().add(TYPE_STREAM, namespaceId).build(), StreamSpecification.class);
  }

  public void deleteAllStreams(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_STREAM, namespaceId).build());
  }

  public void deleteStream(String namespaceId, String name) {
    deleteAll(new MDSKey.Builder().add(TYPE_STREAM, namespaceId, name).build());
  }

  public void deleteProgramHistory(String namespaceId, String appId, String versionId) {
    if (!upgradeComplete.get() && versionId.equals(ApplicationId.DEFAULT_VERSION)) {
      Predicate<MDSKey> keyPredicate = new AppVersionPredicate(ApplicationId.DEFAULT_VERSION);
      deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTING, namespaceId, appId).build(), keyPredicate);
      deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId).build(), keyPredicate);
      deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId, appId).build(), keyPredicate);
      deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_SUSPENDED, namespaceId, appId).build(), keyPredicate);
    } else {
      deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTING, namespaceId, appId, versionId).build());
      deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId, versionId).build());
      deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId, appId, versionId).build());
      deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_SUSPENDED, namespaceId, appId, versionId).build());
    }
  }

  public void deleteProgramHistory(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTING, namespaceId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_SUSPENDED, namespaceId).build());
  }

  public void createNamespace(NamespaceMeta metadata) {
    write(getNamespaceKey(metadata.getName()), metadata);
  }

  public NamespaceMeta getNamespace(Id.Namespace id) {
    return getFirst(getNamespaceKey(id.getId()), NamespaceMeta.class);
  }

  public void deleteNamespace(Id.Namespace id) {
    deleteAll(getNamespaceKey(id.getId()));
  }

  public List<NamespaceMeta> listNamespaces() {
    return list(getNamespaceKey(null), NamespaceMeta.class);
  }

  private MDSKey getNamespaceKey(@Nullable String name) {
    MDSKey.Builder builder = new MDSKey.Builder().add(TYPE_NAMESPACE);
    if (null != name) {
      builder.add(name);
    }
    return builder.build();
  }

  public void updateWorkflowToken(ProgramRunId workflowRunId, WorkflowToken workflowToken) {
    // Workflow token will be stored with following key:
    // [wft][namespace][app][WORKFLOW][workflowName][workflowRun]
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_TOKEN, workflowRunId.getParent())
      .add(workflowRunId.getRun()).build();

    write(key, workflowToken);
  }

  public WorkflowToken getWorkflowToken(ProgramId workflowId, String workflowRunId) {
    Preconditions.checkArgument(ProgramType.WORKFLOW == workflowId.getType());
    // Workflow token is stored with following key:
    // [wft][namespace][app][version][WORKFLOW][workflowName][workflowRun]
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_TOKEN, workflowId).add(workflowRunId).build();

    BasicWorkflowToken workflowToken = get(key, BasicWorkflowToken.class);

    // Check without the version string only for default version
    if (!upgradeComplete.get() && workflowToken == null &&
      workflowId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      key = getVersionLessProgramKeyBuilder(TYPE_WORKFLOW_TOKEN, workflowId).add(workflowRunId).build();
      workflowToken = get(key, BasicWorkflowToken.class);
    }

    if (workflowToken == null) {
      LOG.debug("No workflow token available for workflow: {}, runId: {}", workflowId, workflowRunId);
      // Its ok to not allow any updates by returning a 0 size token.
      return new BasicWorkflowToken(0);
    }

    return workflowToken;
  }

  /**
   * @return programs that were running between given start and end time
   */
  public Set<RunId> getRunningInRange(long startTimeInSecs, long endTimeInSecs) {
    // We have scan timeout to be half of transaction timeout to eliminate transaction timeouts during large scans.
    long scanTimeoutMills = TimeUnit.SECONDS.toMillis(cConf.getLong(TxConstants.Manager.CFG_TX_TIMEOUT)) / 2;
    LOG.trace("Scan timeout = {}ms", scanTimeoutMills);

    Set<RunId> runIds = new HashSet<>();
    Iterables.addAll(runIds, getRunningInRangeForStatus(TYPE_RUN_RECORD_COMPLETED, startTimeInSecs, endTimeInSecs,
                                                        scanTimeoutMills));
    Iterables.addAll(runIds, getRunningInRangeForStatus(TYPE_RUN_RECORD_SUSPENDED, startTimeInSecs, endTimeInSecs,
                                                        scanTimeoutMills));
    Iterables.addAll(runIds, getRunningInRangeForStatus(TYPE_RUN_RECORD_STARTED, startTimeInSecs, endTimeInSecs,
                                                        scanTimeoutMills));
    Iterables.addAll(runIds, getRunningInRangeForStatus(TYPE_RUN_RECORD_STARTING, startTimeInSecs, endTimeInSecs,
                                                        scanTimeoutMills));
    return runIds;
  }

  /**
   * @return true if the row key is value is greater or than or equal to the expected version
   */
  public boolean isUpgradeComplete(byte[] key) {
    MDSKey.Builder keyBuilder = new MDSKey.Builder();
    keyBuilder.add(key);
    String version = get(keyBuilder.build(), String.class);
    if (version == null) {
      return false;
    }
    ProjectInfo.Version actual = new ProjectInfo.Version(version);
    return actual.compareTo(ProjectInfo.getVersion()) >= 0;
  }

  /**
   * Mark the table that the upgrade is complete.
   */
  public void setUpgradeComplete(byte[] key) {
    MDSKey.Builder keyBuilder = new MDSKey.Builder();
    keyBuilder.add(key);
    write(keyBuilder.build(), ProjectInfo.getVersion().toString());
  }

  @Nullable
  @Override
  public String retrieveSubscriberState(String topic) {
    MDSKey.Builder keyBuilder = new MDSKey.Builder().add(TYPE_MESSAGE)
      .add(topic);
    byte[] rawBytes = get(keyBuilder.build(), BYTE_TYPE);
    return (rawBytes == null) ? null : Bytes.toString(rawBytes);
  }

  @Override
  public void persistSubscriberState(String topic, String messageId) {
    MDSKey.Builder keyBuilder = new MDSKey.Builder().add(TYPE_MESSAGE)
      .add(topic);
    write(keyBuilder.build(), Bytes.toBytes(messageId));
  }

  private Iterable<RunId> getRunningInRangeForStatus(String statusKey, final long startTimeInSecs,
                                                     final long endTimeInSecs, long maxScanTimeMillis) {
    List<Iterable<RunId>> batches = getRunningInRangeForStatus(statusKey, startTimeInSecs, endTimeInSecs,
                                                               maxScanTimeMillis, Ticker.systemTicker());
    return Iterables.concat(batches);
  }

  @VisibleForTesting
  List<Iterable<RunId>> getRunningInRangeForStatus(String statusKey, final long startTimeInSecs,
                                                   final long endTimeInSecs, long maxScanTimeMillis, Ticker ticker) {
    // Create time filter to get running programs between start and end time
    Predicate<RunRecordMeta> timeFilter = new Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(RunRecordMeta runRecordMeta) {
        // Program is running in range [startTime, endTime) if the program started before endTime
        // or program's stop time was after startTime
        return runRecordMeta.getStartTs() < endTimeInSecs &&
          (runRecordMeta.getStopTs() == null || runRecordMeta.getStopTs() >= startTimeInSecs);
      }
    };

    // Break up scans into smaller batches to prevent transaction timeout
    List<Iterable<RunId>> batches = new ArrayList<>();
    MDSKey startKey = new MDSKey.Builder().add(statusKey).build();
    MDSKey endKey = new MDSKey(Bytes.stopKeyForPrefix(startKey.getKey()));
    while (true) {
      ScanFunction scanFunction = new ScanFunction(timeFilter, ticker, maxScanTimeMillis);
      scanFunction.start();
      scan(startKey, endKey, RunRecordMeta.class, scanFunction);
      // stop when scan returns zero elements
      if (scanFunction.getNumProcessed() == 0) {
        break;
      }
      batches.add(Iterables.transform(scanFunction.getValues(), RUN_RECORD_META_TO_RUN_ID_FUNCTION));
      // key for next scan is the last key + 1 from the previous scan
      startKey = new MDSKey(Bytes.stopKeyForPrefix(scanFunction.getLastKey().getKey()));
    }
    return batches;
  }

  /**
   * Upgrades the keys in table to include version if it is not already present.
   */
  boolean upgradeVersionKeys(int maxRows) {
    boolean upgradeDone = upgradeVersionKeys(TYPE_APP_META, ApplicationMeta.class, maxRows);
    upgradeDone &= upgradeVersionKeys(TYPE_RUN_RECORD_COMPLETED, RunRecordMeta.class, maxRows);
    upgradeDone &= upgradeVersionKeys(TYPE_WORKFLOW_NODE_STATE, WorkflowNodeStateDetail.class, maxRows);
    upgradeDone &= upgradeVersionKeys(TYPE_WORKFLOW_TOKEN, BasicWorkflowToken.class, maxRows);
    return upgradeDone;
  }

  /**
   * Upgrades the rowkeys for the given record type.
   *
   * @param recordType type of the record
   * @param typeOfT    content type of the record
   * @param <T>        type param
   * @param maxRows    maximum number of rows to be upgraded in this call.
   * @return true if no rows required an upgrade
   */
  private <T> boolean upgradeVersionKeys(String recordType, Type typeOfT, int maxRows) {
    LOG.info("Upgrading {}", recordType);
    MDSKey startKey = new MDSKey.Builder().add(recordType).build();
    Map<MDSKey, T> oldMap = listKV(startKey, typeOfT);
    Map<MDSKey, T> newMap = new HashMap<>();
    Set<MDSKey> deleteKeys = new HashSet<>();

    for (Map.Entry<MDSKey, T> oldEntry : oldMap.entrySet()) {
      MDSKey oldKey = oldEntry.getKey();
      MDSKey newKey = appendDefaultVersion(recordType, typeOfT, oldKey);
      // If the key has been modified, only then add it to the map.
      if (!newKey.equals(oldKey)) {
        deleteKeys.add(oldKey);

        // If a row with the new key doesn't exists, only then upgrade the old key otherwise just delete the old key.
        Object valueOfNewKey = get(newKey, typeOfT);
        if (valueOfNewKey == null) {
          newMap.put(newKey, oldEntry.getValue());
        }

        // We want to modify only certain number of rows
        if (deleteKeys.size() >= maxRows) {
          break;
        }
      }
    }

    // No rows needs to be modified
    if (deleteKeys.size() == 0) {
      return true;
    }

    // Delete old keys
    for (MDSKey oldKey : deleteKeys) {
      delete(oldKey);
    }

    // Write new rows
    for (Map.Entry<MDSKey, T> newEntry : newMap.entrySet()) {
      write(newEntry.getKey(), newEntry.getValue());
    }
    return false;
  }

  private static MDSKey getUpgradedAppMetaKey(MDSKey originalKey) {
    // Key format after upgrade: appMeta.namespace.appName.appVersion
    MDSKey.Splitter splitter = originalKey.split();
    String recordType = splitter.getString();
    String namespace = splitter.getString();
    String appName = splitter.getString();
    String appVersion;
    try {
      appVersion = splitter.getString();
    } catch (BufferUnderflowException e) {
      appVersion = ApplicationId.DEFAULT_VERSION;
    }
    LOG.trace("Upgrade AppMeta key to {}.{}.{}.{}", recordType, namespace, appName, appVersion);
    return new MDSKey.Builder()
      .add(recordType)
      .add(namespace)
      .add(appName)
      .add(appVersion)
      .build();
  }

  private static MDSKey getUpgradedCompletedRunRecordKey(MDSKey originalKey) {
    // Key format after upgrade:
    // runRecordCompleted.namespace.appName.appVersion.programType.programName.invertedTs.runId
    MDSKey.Splitter splitter = originalKey.split();
    String recordType = splitter.getString();
    String namespace = splitter.getString();
    String appName = splitter.getString();
    String programType = splitter.getString();
    // If nextValue is programType then we need to upgrade the key
    try {
      ProgramType.valueOf(programType);
    } catch (IllegalArgumentException e) {
      // Version is already part of the key. So return the original key
      LOG.trace("No need to upgrade completed RunRecord key starting with {}.{}.{}.{}.{}",
                recordType, namespace, appName, ApplicationId.DEFAULT_VERSION, programType);
      return originalKey;
    }
    String programName = splitter.getString();
    long invertedTs = splitter.getLong();
    String runId = splitter.getString();
    LOG.trace("Upgrade completed RunRecord key to {}.{}.{}.{}.{}.{}.{}.{}", recordType, namespace, appName,
              ApplicationId.DEFAULT_VERSION, programType, programName, invertedTs, runId);
    return new MDSKey.Builder()
      .add(recordType)
      .add(namespace)
      .add(appName)
      .add(ApplicationId.DEFAULT_VERSION)
      .add(programType)
      .add(programName)
      .add(invertedTs)
      .add(runId)
      .build();
  }

  private static MDSKey.Builder getUpgradedWorkflowRunKey(MDSKey.Splitter splitter) {
    // Key format after upgrade: recordType.namespace.appName.appVersion.WORKFLOW.workflowName.workflowRun
    String recordType = splitter.getString();
    String namespace = splitter.getString();
    String appName = splitter.getString();
    String nextString = splitter.getString();
    String appVersion;
    String programType;
    if ("WORKFLOW".equals(nextString)) {
      appVersion = ApplicationId.DEFAULT_VERSION;
      programType = nextString;
    } else {
      // App version is already present in the key.
      LOG.trace("App version exists in workflow run id starting with {}.{}.{}.{}", recordType, namespace,
                appName, nextString);
      appVersion = nextString;
      programType = splitter.getString();
    }
    String workflowName = splitter.getString();
    String workflowRun = splitter.getString();
    LOG.trace("Upgrade workflow run id to {}.{}.{}.{}.{}.{}.{}", recordType, namespace, appName,
              ApplicationId.DEFAULT_VERSION, programType, workflowName, workflowRun);
    return new MDSKey.Builder()
      .add(recordType)
      .add(namespace)
      .add(appName)
      .add(appVersion)
      .add(programType)
      .add(workflowName)
      .add(workflowRun);
  }

  private static MDSKey getUpgradedWorkflowNodeStateRecordKey(MDSKey originalKey) {
    // Key format after upgrade: wns.namespace.appName.appVersion.WORKFLOW.workflowName.workflowRun.workflowNodeId
    MDSKey.Splitter splitter = originalKey.split();
    MDSKey.Builder builder = getUpgradedWorkflowRunKey(splitter);
    String workflowNodeId = splitter.getString();
    LOG.trace("Upgrade workflow node state record with WorkflowNodeId {}", workflowNodeId);
    return builder.add(workflowNodeId)
      .build();
  }

  private static MDSKey getUpgradedWorkflowTokenRecordKey(MDSKey originalKey) {
    // Key format after upgrade: wft.namespace.appName.appVersion.WORKFLOW.workflowName,workflowRun
    MDSKey.Splitter splitter = originalKey.split();
    return getUpgradedWorkflowRunKey(splitter).build();
  }

  // Append version only if it doesn't have version in the key
  private static MDSKey appendDefaultVersion(String recordType, Type typeOfT, MDSKey originalKey) {
    switch (recordType) {
      case TYPE_APP_META:
        return getUpgradedAppMetaKey(originalKey);
      case TYPE_RUN_RECORD_COMPLETED:
        return getUpgradedCompletedRunRecordKey(originalKey);
      case TYPE_WORKFLOW_NODE_STATE:
        return getUpgradedWorkflowNodeStateRecordKey(originalKey);
      case TYPE_WORKFLOW_TOKEN:
        return getUpgradedWorkflowTokenRecordKey(originalKey);
      default:
        throw new IllegalArgumentException(String.format("Invalid row key type '%s'", recordType));
    }
  }

  /**
   * Returns a ProgramId given the MDS key
   *
   * @param key the MDS key to be used
   * @return ProgramId created from the MDS key
   */
  private static ProgramId getProgramID(MDSKey key) {
    MDSKey.Splitter splitter = key.split();

    // Format : recordType, ns, app, <version>, type, program, <ts>, runid
    // <version> -> might or might not be present based on whether the upgrade thread has changed the record
    // <ts> -> will be present if the record type is runRecordComplete

    String recordType = splitter.getString();
    String namespace = splitter.getString();
    String application = splitter.getString();
    String appVersion = ApplicationId.DEFAULT_VERSION;
    String type;
    String program;

    List<String> splits = new ArrayList<>();
    while (true) {
      try {
        splits.add(splitter.getString());
      } catch (BufferUnderflowException ex) {
        break;
      }
    }

    // If the record type is runRecordCompleted, then it will have a <ts> field between program and runid fields.
    // So trying to split
    int versionLessSplitsSize = recordType.equals(TYPE_RUN_RECORD_COMPLETED) ? 2 : 3;
    if (splits.size() == versionLessSplitsSize) {
      // old format: [recordType, ns, app] type, program, <ts, runid>
      type = splits.get(0);
      program = splits.get(1);
    } else {
      // new format: [recordType, ns, app] version, type, program, <ts, runid>
      appVersion = splits.get(0);
      type = splits.get(1);
      program = splits.get(2);
    }
    return (new ApplicationId(namespace, application, appVersion).program(ProgramType.valueOf(type), program));
  }

  private static class AppVersionPredicate implements Predicate<MDSKey> {
    private final String version;

    AppVersionPredicate(String version) {
      this.version = version;
    }

    @Override
    public boolean apply(MDSKey input) {
      ProgramId programId = getProgramID(input);
      return programId.getVersion().equals(version);
    }
  }

  private static class ScanFunction implements Function<MetadataStoreDataset.KeyValue<RunRecordMeta>, Boolean> {
    private final Predicate<RunRecordMeta> filter;
    private final Stopwatch stopwatch;
    private final long maxScanTimeMillis;
    private final List<RunRecordMeta> values = new ArrayList<>();
    private int numProcessed = 0;
    private MDSKey lastKey;

    ScanFunction(Predicate<RunRecordMeta> filter, Ticker ticker, long maxScanTimeMillis) {
      this.filter = filter;
      this.maxScanTimeMillis = maxScanTimeMillis;
      this.stopwatch = new Stopwatch(ticker);
    }

    public void start() {
      stopwatch.start();
    }

    public List<RunRecordMeta> getValues() {
      return Collections.unmodifiableList(values);
    }

    public int getNumProcessed() {
      return numProcessed;
    }

    public MDSKey getLastKey() {
      return lastKey;
    }

    @Override
    public Boolean apply(MetadataStoreDataset.KeyValue<RunRecordMeta> input) {
      long elapsedMillis = stopwatch.elapsedMillis();
      if (elapsedMillis > maxScanTimeMillis) {
        return false;
      }

      ++numProcessed;
      lastKey = input.getKey();
      if (filter.apply(input.getValue())) {
        values.add(input.getValue());
      }
      return true;
    }
  }
}

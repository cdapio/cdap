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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
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
import javax.annotation.Nullable;

import static com.google.common.base.Predicates.and;

/**
 * Store for application metadata
 */
public class AppMetadataStore extends MetadataStoreDataset {
  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStore.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final String TYPE_APP_META = "appMeta";
  private static final String TYPE_STREAM = "stream";
  private static final String TYPE_RUN_RECORD_STARTED = "runRecordStarted";
  private static final String TYPE_RUN_RECORD_SUSPENDED = "runRecordSuspended";
  private static final String TYPE_RUN_RECORD_COMPLETED = "runRecordCompleted";
  private static final String TYPE_WORKFLOW_NODE_STATE = "wns";
  private static final String TYPE_WORKFLOW_TOKEN = "wft";
  private static final String TYPE_NAMESPACE = "namespace";

  private final CConfiguration cConf;

  private static final Function<RunRecordMeta, RunId> RUN_RECORD_META_TO_RUN_ID_FUNCTION =
    new Function<RunRecordMeta, RunId>() {
      @Override
      public RunId apply(RunRecordMeta runRecordMeta) {
        return RunIds.fromString(runRecordMeta.getPid());
      }
    };

  public AppMetadataStore(Table table, CConfiguration cConf) {
    super(table);
    this.cConf = cConf;
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
    return getFirst(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId, versionId).build(),
                    ApplicationMeta.class);
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
      splitter.getBytes(); // skip recordType
      splitter.getBytes(); // skip namespaceId
      splitter.getBytes(); // skip appId
      String versionId = splitter.getString();
      appIds.add(new NamespaceId(namespaceId).app(appId, versionId));
    }
    return appIds;
  }

  public void writeApplication(String namespaceId, String appId, String versionId, ApplicationSpecification spec) {
    write(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId, versionId).build(),
          new ApplicationMeta(appId, spec));
  }

  public void deleteApplication(String namespaceId, String appId, String versionId) {
    deleteAll(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId, versionId).build());
  }

  public void deleteApplications(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_APP_META, namespaceId).build());
  }

  // todo: do we need appId? may be use from appSpec?
  public void updateAppSpec(String namespaceId, String appId, String versionId, ApplicationSpecification spec) {
    LOG.trace("App spec to be updated: id: {}: spec: {}", appId, GSON.toJson(spec));
    MDSKey key = new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId, versionId).build();
    ApplicationMeta existing = getFirst(key, ApplicationMeta.class);
    if (existing == null) {
      String msg = String.format("No meta for namespace %s app %s exists", namespaceId, appId);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    LOG.trace("Application exists in mds: id: {}, spec: {}", existing);
    ApplicationMeta updated = ApplicationMeta.updateSpec(existing, spec);
    write(key, updated);
  }

  /**
   * Return the {@link List} of {@link WorkflowNodeStateDetail} for a given Workflow run.
   */
  public List<WorkflowNodeStateDetail> getWorkflowNodeStates(ProgramRunId workflowRunId) {
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId.getParent())
      .add(workflowRunId.getRun()).build();

    return list(key, WorkflowNodeStateDetail.class);
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
    key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, workflowRunId.getParent()).add(workflowRunId.getRun()).build();

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

    String workflowrunId = null;
    if (systemArgs != null && systemArgs.containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      // Program is started by Workflow. Add row corresponding to its node state.
      addWorkflowNodeState(programId, pid, systemArgs, ProgramRunStatus.RUNNING, null);
      workflowrunId = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);
    }

    MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, programId).add(pid).build();

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("runtimeArgs", GSON.toJson(runtimeArgs, MAP_STRING_STRING_TYPE));
    if (workflowrunId != null) {
      builder.put("workflowrunid", workflowrunId);
    }

    RunRecordMeta meta =
      new RunRecordMeta(pid, startTs, null, ProgramRunStatus.RUNNING, builder.build(), systemArgs, twillRunId);
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
    if (record == null) {
      String msg = String.format("No meta for %s run record for namespace %s app %s program type %s " +
                                   "program %s pid %s exists", action.equals("suspend") ? "started" : "suspended",
                                 programId.getNamespace(), programId.getApplication(), programId.getType().name(),
                                 programId.getProgram(), pid);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    deleteAll(key);

    key = getProgramKeyBuilder(toType, programId)
      .add(pid)
      .build();
    write(key, new RunRecordMeta(record, null, toStatus));
  }

  public void recordProgramStop(ProgramId programId, String pid, long stopTs, ProgramRunStatus runStatus,
                                @Nullable BasicThrowable failureCause) {
    MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, programId)
      .add(pid)
      .build();
    RunRecordMeta started = getFirst(key, RunRecordMeta.class);
    if (started == null) {
      String msg = String.format("No meta for started run record for namespace %s app %s program type %s " +
                                 "program %s pid %s exists",
                                 programId.getNamespace(), programId.getApplication(), programId.getType().name(),
                                 programId.getProgram(), pid);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    if (started.getSystemArgs() != null && started.getSystemArgs().containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      addWorkflowNodeState(programId, pid, started.getSystemArgs(), runStatus, failureCause);
    }

    deleteAll(key);

    key = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId)
      .add(getInvertedTsKeyPart(started.getStartTs()))
      .add(pid).build();

    write(key, new RunRecordMeta(started, stopTs, runStatus));
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

  public  Map<ProgramRunId, RunRecordMeta> getRuns(@Nullable ProgramId programId, ProgramRunStatus status,
                                     long startTime, long endTime, int limit,
                                     @Nullable Predicate<RunRecordMeta> filter) {
    if (status.equals(ProgramRunStatus.ALL)) {
      Map<ProgramRunId, RunRecordMeta> resultmap =  new LinkedHashMap<>();
      resultmap.putAll(getActiveRuns(programId, startTime, endTime, limit, filter));
      resultmap.putAll(getSuspendedRuns(programId, startTime, endTime, limit - resultmap.size(), filter));
      resultmap.putAll(getProgramRunIdMap(getHistoricalRuns(programId, status, startTime, endTime,
                                             limit - resultmap.size(), filter)));
      return resultmap;
    } else if (status.equals(ProgramRunStatus.RUNNING)) {
      return getActiveRuns(programId, startTime, endTime, limit, filter);
    } else if (status.equals(ProgramRunStatus.SUSPENDED)) {
      return getSuspendedRuns(programId, startTime, endTime, limit, filter);
    } else {
      return getProgramRunIdMap(getHistoricalRuns(programId, status, startTime, endTime, limit, filter));
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

    return get(runningKey, RunRecordMeta.class);
  }

  private RunRecordMeta getCompletedRun(ProgramId programId, final String runid) {
    MDSKey completedKey = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId).build();

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

  private Map<ProgramRunId, RunRecordMeta> getSuspendedRuns(ProgramId programId, long startTime, long endTime,
                                                             int limit, @Nullable Predicate<RunRecordMeta> filter) {
    return getNonCompleteRuns(programId, TYPE_RUN_RECORD_SUSPENDED, startTime, endTime, limit, filter);
  }

  private Map<ProgramRunId, RunRecordMeta> getActiveRuns(ProgramId programId, final long startTime, final long endTime,
                                                          int limit, @Nullable Predicate<RunRecordMeta> filter) {
    return getNonCompleteRuns(programId, TYPE_RUN_RECORD_STARTED, startTime, endTime, limit, filter);
  }

  private Map<ProgramRunId, RunRecordMeta> getNonCompleteRuns(ProgramId programId, String recordType,
                                                 final long startTime, final long endTime, int limit,
                                                 Predicate<RunRecordMeta> filter) {
    MDSKey activeKey = getProgramKeyBuilder(recordType, programId).build();

    Map<MDSKey, RunRecordMeta> runRecordMap = listKV(activeKey, null, RunRecordMeta.class, limit,
                                                     andPredicate(new Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(RunRecordMeta input) {
        return input.getStartTs() >= startTime && input.getStartTs() < endTime;
      }
    }, filter));

    return getProgramRunIdMap(runRecordMap);
  }

  private Map<ProgramRunId, RunRecordMeta> getSuspendedRuns(Set<ProgramRunId> programRunIds, int limit) {
    return getNonCompleteRuns(programRunIds, TYPE_RUN_RECORD_SUSPENDED, limit);
  }

  private Map<ProgramRunId, RunRecordMeta> getActiveRuns(Set<ProgramRunId> programRunIds, int limit) {
    return getNonCompleteRuns(programRunIds, TYPE_RUN_RECORD_STARTED, limit);
  }

  private Map<ProgramRunId, RunRecordMeta> getNonCompleteRuns(Set<ProgramRunId> programRunIds, String recordType,
                                                              int limit) {
    Set<MDSKey> keySet = new HashSet<>();
    for (ProgramRunId programRunId : programRunIds) {
      keySet.add(getProgramKeyBuilder(recordType, programRunId).build());
    }
    Map<MDSKey, RunRecordMeta> returnMap = listKV(keySet, RunRecordMeta.class, limit);
    return getProgramRunIdMap(returnMap);
  }

  private Map<ProgramRunId, RunRecordMeta> getHistoricalRuns(Set<ProgramRunId> programRunIds, int limit) {
    Set<MDSKey> keySet = new HashSet<>();

    for (ProgramRunId programRunId : programRunIds) {
      keySet.add(getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programRunId).build());
    }
    Map<MDSKey, RunRecordMeta> returnMap = listKV(keySet, RunRecordMeta.class, limit);
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
      programRunIdMap.put(new ProgramRunId(programId.getNamespace(), programId.getApplication(),
                                           programId.getType(), programId.getProgram(), entry.getValue().getPid()),
                          entry.getValue());
    }
    return programRunIdMap;
  }

  private  Map<MDSKey, RunRecordMeta> getHistoricalRuns(ProgramId programId, ProgramRunStatus status,
                                                final long startTime, final long endTime, int limit,
                                                @Nullable Predicate<RunRecordMeta> filter) {
    MDSKey historyKey = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programId).build();

    MDSKey start = new MDSKey.Builder(historyKey).add(getInvertedTsScanKeyPart(endTime)).build();
    MDSKey stop = new MDSKey.Builder(historyKey).add(getInvertedTsScanKeyPart(startTime)).build();
    if (status.equals(ProgramRunStatus.ALL)) {
      //return all records (successful and failed)
      return listKV(start, stop, RunRecordMeta.class, limit,
                  filter == null ? Predicates.<RunRecordMeta>alwaysTrue() : filter);
    }

    if (status.equals(ProgramRunStatus.COMPLETED)) {
      return listKV(start, stop, RunRecordMeta.class, limit,
                  andPredicate(getPredicate(ProgramController.State.COMPLETED), filter));
    }
    if (status.equals(ProgramRunStatus.KILLED)) {
      return listKV(start, stop, RunRecordMeta.class, limit,
                  andPredicate(getPredicate(ProgramController.State.KILLED), filter));
    }
    return listKV(start, stop, RunRecordMeta.class, limit,
                andPredicate(getPredicate(ProgramController.State.ERROR), filter));
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

  public void deleteProgramHistory(String namespaceId, String appId) {
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId, appId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_SUSPENDED, namespaceId, appId).build());
  }

  public void deleteProgramHistory(String namespaceId) {
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
    // [wft][namespace][app][WORKFLOW][workflowName][workflowRun]
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_TOKEN, workflowId).add(workflowRunId).build();

    BasicWorkflowToken workflowToken = get(key, BasicWorkflowToken.class);
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
    return runIds;
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

  public void upgradeVersionKeys() {
    MDSKey startKey = new MDSKey.Builder().add(TYPE_APP_META).build();
    Map<MDSKey, ApplicationMeta> appMetas = listKV(startKey, ApplicationMeta.class);
    Map<MDSKey, ApplicationMeta> newAppMetas = new HashMap<>();
    for (Map.Entry<MDSKey, ApplicationMeta> oldKey : appMetas.entrySet()) {
      MDSKey newKey = appendDefaultVersion(oldKey.getKey());
      newAppMetas.put(newKey, oldKey.getValue());
    }

    //Delete old rows
    deleteAll(startKey);

    // Write new rows
    for (Map.Entry<MDSKey, ApplicationMeta> newMeta : newAppMetas.entrySet()) {
      write(newMeta.getKey(), newMeta.getValue());
    }
  }

  // Append version only if it doesn't have version at the end
  private static MDSKey appendDefaultVersion(MDSKey oldKey) {
    MDSKey.Splitter splitter = oldKey.split();
    int keyParts = 0;
    try {
      // max parts = 4 => appMeta.ns.app.version
      for (int i = 0; i < 4; i++) {
        splitter.skipString();
        keyParts++;
      }
    } catch (BufferUnderflowException ex) {
      // expected when the version is not part of the key
    }

    if (keyParts == 4) {
      // Key already is versioned, then return old key
      return oldKey;
    }
    // Otherwise, append the key with default version.
    return new MDSKey.Builder(oldKey).add(ApplicationId.DEFAULT_VERSION).build();
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

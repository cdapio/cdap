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
import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.WorkflowNodeThrowable;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.tephra.TxConstants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
  public ApplicationMeta getApplication(String namespaceId, String appId) {
    return getFirst(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId).build(), ApplicationMeta.class);
  }

  public List<ApplicationMeta> getAllApplications(String namespaceId) {
    return list(new MDSKey.Builder().add(TYPE_APP_META, namespaceId).build(), ApplicationMeta.class);
  }

  public void writeApplication(String namespaceId, String appId, ApplicationSpecification spec,
                               String archiveLocation) {
    // NOTE: we use Gson underneath to do serde, as it doesn't serialize inner classes (which we use everywhere for
    //       specs - see forwarding specs), we want to wrap spec with DefaultApplicationSpecification
    spec = DefaultApplicationSpecification.from(spec);
    write(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId).build(),
          new ApplicationMeta(appId, spec, archiveLocation));
  }

  public void deleteApplication(String namespaceId, String appId) {
    deleteAll(new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId).build());
  }

  public void deleteApplications(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_APP_META, namespaceId).build());
  }

  // todo: do we need appId? may be use from appSpec?
  public void updateAppSpec(String namespaceId, String appId, ApplicationSpecification spec) {
    // NOTE: we use Gson underneath to do serde, as it doesn't serialize inner classes (which we use everywhere for
    //       specs - see forwarding specs), we want to wrap spec with DefaultApplicationSpecification
    spec = DefaultApplicationSpecification.from(spec);
    LOG.trace("App spec to be updated: id: {}: spec: {}", appId, GSON.toJson(spec));
    MDSKey key = new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId).build();
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
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId.getParent().toId())
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
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId.getParent().toId())
      .add(workflowRunId.getRun()).add(nodeStateDetail.getNodeId()).build();

    write(key, nodeStateDetail);
  }

  private void addWorkflowNodeState(ProgramId programId, String pid, Map<String, String> systemArgs,
                                    ProgramRunStatus status, @Nullable Throwable failureCause) {
    String workflowNodeId = systemArgs.get(ProgramOptionConstants.WORKFLOW_NODE_ID);
    String workflowName = systemArgs.get(ProgramOptionConstants.WORKFLOW_NAME);
    String workflowRun = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);

    ApplicationId appId = Ids.namespace(programId.getNamespace()).app(programId.getApplication());
    ProgramRunId workflowRunId = appId.workflow(workflowName).run(workflowRun);

    // Node states will be stored with following key:
    // workflowNodeState.namespace.app.WORKFLOW.workflowName.workflowRun.workflowNodeId
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId.getParent().toId())
      .add(workflowRun).add(workflowNodeId).build();

    WorkflowNodeThrowable defaultThrowable = failureCause == null ? null : new WorkflowNodeThrowable(failureCause);
    WorkflowNodeStateDetail nodeStateDetail = new WorkflowNodeStateDetail(workflowNodeId,
                                                                          ProgramRunStatus.toNodeStatus(status),
                                                                          pid, defaultThrowable);

    write(key, nodeStateDetail);

    // Get the run record of the Workflow which started this program
    key = getWorkflowRunRecordKey(workflowRunId.getParent().toId(), workflowRunId.getRun());

    RunRecordMeta record = get(key, RunRecordMeta.class);
    if (record != null) {
      // Update the parent Workflow run record by adding node id and program run id in the properties
      Map<String, String> properties = record.getProperties();
      properties.put(workflowNodeId, pid);
      write(key, new RunRecordMeta(record, properties));
    }
  }

  public void recordProgramStart(Id.Program program, String pid, long startTs, String twillRunId,
                                 Map<String, String> runtimeArgs, Map<String, String> systemArgs) {

    String workflowrunId = null;
    if (systemArgs != null && systemArgs.containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      // Program is started by Workflow. Add row corresponding to its node state.
      ProgramId programId = program.toEntityId();
      addWorkflowNodeState(programId, pid, systemArgs, ProgramRunStatus.RUNNING, null);
      workflowrunId = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);
    }

    MDSKey key = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_STARTED)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(pid)
      .build();

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("runtimeArgs", GSON.toJson(runtimeArgs, MAP_STRING_STRING_TYPE));
    if (workflowrunId != null) {
      builder.put("workflowrunid", workflowrunId);
    }

    RunRecordMeta meta = new RunRecordMeta(pid, startTs, null, ProgramRunStatus.RUNNING, builder.build(), systemArgs,
                                           twillRunId);
    write(key, meta);
  }

  public void recordProgramSuspend(Id.Program program, String pid) {
    recordProgramSuspendResume(program, pid, "suspend");
  }

  public void recordProgramResumed(Id.Program program, String pid) {
    recordProgramSuspendResume(program, pid, "resume");
  }

  private void recordProgramSuspendResume(Id.Program program, String pid, String action) {
    String fromType = TYPE_RUN_RECORD_STARTED;
    String toType = TYPE_RUN_RECORD_SUSPENDED;
    ProgramRunStatus toStatus = ProgramRunStatus.SUSPENDED;

    if (action.equals("resume")) {
      fromType = TYPE_RUN_RECORD_SUSPENDED;
      toType = TYPE_RUN_RECORD_STARTED;
      toStatus = ProgramRunStatus.RUNNING;
    }

    MDSKey key = new MDSKey.Builder()
      .add(fromType)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(pid)
      .build();
    RunRecordMeta record = get(key, RunRecordMeta.class);
    if (record == null) {
      String msg = String.format("No meta for %s run record for namespace %s app %s program type %s " +
                                   "program %s pid %s exists", action.equals("suspend") ? "started" : "suspended",
                                 program.getNamespaceId(), program.getApplicationId(), program.getType().name(),
                                 program.getId(), pid);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    deleteAll(key);

    key = new MDSKey.Builder()
      .add(toType)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(pid)
      .build();
    write(key, new RunRecordMeta(record, null, toStatus));
  }

  public void recordProgramStop(Id.Program program, String pid, long stopTs, ProgramRunStatus runStatus,
                                @Nullable Throwable failureCause) {
    MDSKey key = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_STARTED)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(pid)
      .build();
    RunRecordMeta started = getFirst(key, RunRecordMeta.class);
    if (started == null) {
      String msg = String.format("No meta for started run record for namespace %s app %s program type %s " +
                                 "program %s pid %s exists",
                                 program.getNamespaceId(), program.getApplicationId(), program.getType().name(),
                                 program.getId(), pid);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    if (started.getSystemArgs() != null && started.getSystemArgs().containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      ProgramId programId = program.toEntityId();
      addWorkflowNodeState(programId, pid, started.getSystemArgs(), runStatus, failureCause);
    }

    deleteAll(key);

    key = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_COMPLETED)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(getInvertedTsKeyPart(started.getStartTs()))
      .add(pid).build();

    write(key, new RunRecordMeta(started, stopTs, runStatus));
  }

  public List<RunRecordMeta> getRuns(ProgramRunStatus status, Predicate<RunRecordMeta> filter) {
    return getRuns(null, status, Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE, filter);
  }

  private MDSKey.Builder getProgramKeyBuilder(String recordType, @Nullable Id.Program program) {
    MDSKey.Builder builder = new MDSKey.Builder().add(recordType);
    if (program != null) {
      builder.add(program.getNamespaceId());
      builder.add(program.getApplicationId());
      builder.add(program.getType().name());
      builder.add(program.getId());
    }
    return builder;
  }

  public List<RunRecordMeta> getRuns(@Nullable Id.Program program, ProgramRunStatus status,
                                     long startTime, long endTime, int limit,
                                     @Nullable Predicate<RunRecordMeta> filter) {
    if (status.equals(ProgramRunStatus.ALL)) {
      List<RunRecordMeta> resultRecords = Lists.newArrayList();
      resultRecords.addAll(getActiveRuns(program, startTime, endTime, limit, filter));
      resultRecords.addAll(getSuspendedRuns(program, startTime, endTime, limit - resultRecords.size(), filter));
      resultRecords.addAll(getHistoricalRuns(program, status, startTime, endTime,
                                             limit - resultRecords.size(), filter));
      return resultRecords;
    } else if (status.equals(ProgramRunStatus.RUNNING)) {
      return getActiveRuns(program, startTime, endTime, limit, filter);
    } else if (status.equals(ProgramRunStatus.SUSPENDED)) {
      return getSuspendedRuns(program, startTime, endTime, limit, filter);
    } else {
      return getHistoricalRuns(program, status, startTime, endTime, limit, filter);
    }
  }

  // TODO: getRun is duplicated in cdap-watchdog AppMetadataStore class.
  // Any changes made here will have to be made over there too.
  // JIRA https://issues.cask.co/browse/CDAP-2172
  public RunRecordMeta getRun(Id.Program program, final String runid) {
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
  private RunRecordMeta getUnfinishedRun(Id.Program program, String recordType, String runid) {
    MDSKey runningKey = new MDSKey.Builder()
      .add(recordType)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(runid)
      .build();

    return get(runningKey, RunRecordMeta.class);
  }

  private RunRecordMeta getCompletedRun(Id.Program program, final String runid) {
    MDSKey completedKey = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_COMPLETED)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .build();

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

  private List<RunRecordMeta> getSuspendedRuns(Id.Program program, long startTime, long endTime, int limit,
                                               @Nullable Predicate<RunRecordMeta> filter) {
    return getNonCompleteRuns(program, TYPE_RUN_RECORD_SUSPENDED, startTime, endTime, limit, filter);
  }

  private List<RunRecordMeta> getActiveRuns(Id.Program program, final long startTime, final long endTime, int limit,
                                            @Nullable Predicate<RunRecordMeta> filter) {
    return getNonCompleteRuns(program, TYPE_RUN_RECORD_STARTED, startTime, endTime, limit, filter);
    }

  private List<RunRecordMeta> getNonCompleteRuns(Id.Program program, String recordType,
                                                 final long startTime, final long endTime, int limit,
                                                 Predicate<RunRecordMeta> filter) {
    MDSKey activeKey = getProgramKeyBuilder(recordType, program).build();

    return list(activeKey, null, RunRecordMeta.class, limit, andPredicate(new Predicate<RunRecordMeta>() {
                  @Override
                  public boolean apply(RunRecordMeta input) {
                    return input.getStartTs() >= startTime && input.getStartTs() < endTime;
                  }
                }, filter));
  }

  private List<RunRecordMeta> getHistoricalRuns(Id.Program program, ProgramRunStatus status,
                                                final long startTime, final long endTime, int limit,
                                                @Nullable Predicate<RunRecordMeta> filter) {
    MDSKey historyKey = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, program).build();

    MDSKey start = new MDSKey.Builder(historyKey).add(getInvertedTsScanKeyPart(endTime)).build();
    MDSKey stop = new MDSKey.Builder(historyKey).add(getInvertedTsScanKeyPart(startTime)).build();
    if (status.equals(ProgramRunStatus.ALL)) {
      //return all records (successful and failed)
      return list(start, stop, RunRecordMeta.class, limit,
                  filter == null ? Predicates.<RunRecordMeta>alwaysTrue() : filter);
    }

    if (status.equals(ProgramRunStatus.COMPLETED)) {
      return list(start, stop, RunRecordMeta.class, limit,
                  andPredicate(getPredicate(ProgramController.State.COMPLETED), filter));
    }
    if (status.equals(ProgramRunStatus.KILLED)) {
      return list(start, stop, RunRecordMeta.class, limit,
                  andPredicate(getPredicate(ProgramController.State.KILLED), filter));
    }
    return list(start, stop, RunRecordMeta.class, limit,
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
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_TOKEN, workflowRunId.getParent().toId())
      .add(workflowRunId.getRun()).build();

    write(key, workflowToken);
  }

  public WorkflowToken getWorkflowToken(Id.Workflow workflowId, String workflowRunId) {
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

  private MDSKey getWorkflowRunRecordKey(Id.Program workflowId, String workflowRunId) {
    return new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_STARTED)
      .add(workflowId.getNamespaceId())
      .add(workflowId.getApplicationId())
      .add(ProgramType.WORKFLOW.name())
      .add(workflowId.getId())
      .add(workflowRunId)
      .build();
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

  private static class ScanFunction implements Function<MetadataStoreDataset.KeyValue<RunRecordMeta>, Boolean> {
    private final Predicate<RunRecordMeta> filter;
    private final Stopwatch stopwatch;
    private final long maxScanTimeMillis;
    private final List<RunRecordMeta> values = new ArrayList<>();
    private int numProcessed = 0;
    private MDSKey lastKey;

    public ScanFunction(Predicate<RunRecordMeta> filter, Ticker ticker, long maxScanTimeMillis) {
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

  /**
   * Upgrade the Workflow run records. This method iterate over all Workflow run records
   * and create new records for Workflow token and Workflow node states based on the properties.
   */
  public void upgradeWorkflowRunRecords() {
    final String workflowTokenPropertyName = "workflowToken";
    String runtimeArgsPropertyName = "runtimeArgs";
    MDSKey startKey = new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED).build();
    MDSKey endKey = new MDSKey(Bytes.stopKeyForPrefix(startKey.getKey()));
    Predicate<RunRecordMeta> predicate = new Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(@Nullable RunRecordMeta input) {
        return input != null && input.getProperties().containsKey(workflowTokenPropertyName);
      }
    };

    Map<MDSKey, RunRecordMeta> wfRunRecords = listKV(startKey, endKey, RunRecordMeta.class, Integer.MAX_VALUE,
                                                     predicate);

    for (Map.Entry<MDSKey, RunRecordMeta> wfRunRecord : wfRunRecords.entrySet()) {
      String runId = wfRunRecord.getValue().getPid();
      ProgramRunId workflowRunId = getProgramIdFromRunRecordKey(wfRunRecord.getKey()).run(runId);

      Map<String, String> runRecordProperties = wfRunRecord.getValue().getProperties();

      String workflowToken = runRecordProperties.get(workflowTokenPropertyName);
      updateWorkflowToken(workflowRunId, GSON.fromJson(workflowToken, BasicWorkflowToken.class));

      for (Map.Entry<String, String> property : runRecordProperties.entrySet()) {
        if (property.getKey().equals(workflowTokenPropertyName) || property.getKey().equals(runtimeArgsPropertyName)) {
          // property is for workflow token or runtime argument
          continue;
        }

        // Property is of type - <program name, program run id>
        String programName = property.getKey();
        String programRunId = property.getValue();
        ProgramId programId = Ids.namespace(workflowRunId.getNamespace()).app(workflowRunId.getApplication())
          .mr(programName);
        // Check if the current property is MapReduce program
        RunRecordMeta completedRun = getCompletedRun(programId.toId(), programRunId);
        if (completedRun == null) {
          // Check if current property is for Spark program
          programId = Ids.namespace(workflowRunId.getNamespace()).app(workflowRunId.getApplication())
            .spark(programName);
          completedRun = getCompletedRun(programId.toId(), programRunId);
        }

        if (completedRun == null) {
          continue;
        }

        NodeStatus nodeStatus = ProgramRunStatus.toNodeStatus(completedRun.getStatus());
        WorkflowNodeStateDetail nodeStateDetail = new WorkflowNodeStateDetail(programName, nodeStatus, programRunId,
                                                                              null);
        addWorkflowNodeState(workflowRunId, nodeStateDetail);
      }
    }
  }

  private ProgramId getProgramIdFromRunRecordKey(MDSKey key) {
    MDSKey.Splitter splitter = key.split();
    // Skip the RunRecord type.
    splitter.skipString();
    // Namespace id is the next part.
    String namespaceId = splitter.getString();
    // Application id is the next part.
    String applicationId = splitter.getString();
    // Program type is the next part.
    String programType = splitter.getString();
    // Program id is the next part.
    String programId = splitter.getString();

    return Ids.namespace(namespaceId).app(applicationId).program(ProgramType.valueOf(programType), programId);
  }
}

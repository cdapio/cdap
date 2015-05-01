/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.proto.AdapterStatus;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.templates.AdapterDefinition;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static com.google.common.base.Predicates.and;

/**
 * Store for application metadata
 */
public class AppMetadataStore extends MetadataStoreDataset {
  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStore.class);

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();

  public static final String TYPE_APP_META = "appMeta";
  public static final String TYPE_STREAM = "stream";
  public static final String TYPE_RUN_RECORD_STARTED = "runRecordStarted";
  public static final String TYPE_RUN_RECORD_SUSPENDED = "runRecordSuspended";
  public static final String TYPE_RUN_RECORD_COMPLETED = "runRecordCompleted";
  public static final String TYPE_PROGRAM_ARGS = "programArgs";
  private static final String TYPE_NAMESPACE = "namespace";
  private static final String TYPE_ADAPTER = "adapter";

  public AppMetadataStore(Table table) {
    super(table);
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

  public void recordProgramStart(Id.Program program, String pid, long startTs, String adapter, String twillRunId) {
    MDSKey key = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_STARTED)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(pid)
      .build();

    write(key, new RunRecord(pid, startTs, null, ProgramRunStatus.RUNNING, adapter, twillRunId));
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
    RunRecord record = get(key, RunRecord.class);
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
    write(key, new RunRecord(record, null, toStatus));
  }

  public void recordProgramStop(Id.Program program, String pid, long stopTs, ProgramRunStatus runStatus) {
    MDSKey key = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_STARTED)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(pid)
      .build();
    RunRecord started = getFirst(key, RunRecord.class);
    if (started == null) {
      String msg = String.format("No meta for started run record for namespace %s app %s program type %s " +
                                 "program %s pid %s exists",
                                 program.getNamespaceId(), program.getApplicationId(), program.getType().name(),
                                 program.getId(), pid);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
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
    write(key, new RunRecord(started, stopTs, runStatus));
  }

  public List<RunRecord> getRuns(ProgramRunStatus status, Predicate<RunRecord> filter) {
    return getRuns(null, status, Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE, null, filter);
  }

  private MDSKey.Builder getProgramKeyBuilder(String searchType, @Nullable Id.Program program) {
    MDSKey.Builder builder = new MDSKey.Builder().add(searchType);
    if (program != null) {
      builder.add(program.getNamespaceId());
      builder.add(program.getApplicationId());
      builder.add(program.getType().name());
      builder.add(program.getId());
    }
    return builder;
  }

  public List<RunRecord> getRuns(@Nullable Id.Program program, ProgramRunStatus status,
                                 long startTime, long endTime, int limit, String adapter,
                                 @Nullable Predicate<RunRecord> filter) {
    if (status.equals(ProgramRunStatus.ALL)) {
      List<RunRecord> resultRecords = Lists.newArrayList();
      resultRecords.addAll(getSuspendedRuns(program, startTime, endTime, limit, adapter, filter));
      resultRecords.addAll(getActiveRuns(program, startTime, endTime, limit, adapter, filter));
      resultRecords.addAll(getHistoricalRuns(program, status, startTime, endTime, limit, adapter, filter));
      return resultRecords;
    } else if (status.equals(ProgramRunStatus.RUNNING)) {
      return getActiveRuns(program, startTime, endTime, limit, adapter, filter);
    } else if (status.equals(ProgramRunStatus.SUSPENDED)) {
      return getSuspendedRuns(program, startTime, endTime, limit, adapter, filter);
    } else {
      return getHistoricalRuns(program, status, startTime, endTime, limit, adapter, filter);
    }
  }

  public List<RunRecord> getRuns(@Nullable Id.Program program, ProgramRunStatus status,
                                 long startTime, long endTime, int limit, String adapter) {
    return getRuns(program, status, startTime, endTime, limit, adapter, null);
  }

  // TODO: getRun is duplicated in cdap-watchdog AppMetadataStore class.
  // Any changes made here will have to be made over there too.
  // JIRA https://issues.cask.co/browse/CDAP-2172
  public RunRecord getRun(Id.Program program, final String runid) {
    // Query active run record first
    RunRecord running = getUnfinishedRun(program, TYPE_RUN_RECORD_STARTED, runid);
    // If program is running, this will be non-null
    if (running != null) {
      return running;
    }

    // If program is not running, query completed run records
    RunRecord complete = getCompletedRun(program, runid);
    if (complete != null) {
      return complete;
    }

    // Else query suspended run records
    return getUnfinishedRun(program, TYPE_RUN_RECORD_SUSPENDED, runid);
  }

  /**
   * @return run records for runs that do not have start time in mds key for the run record.
   */
  private RunRecord getUnfinishedRun(Id.Program program, String recordType, String runid) {
    MDSKey runningKey = new MDSKey.Builder()
      .add(recordType)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(runid)
      .build();

    return get(runningKey, RunRecord.class);
  }

  private RunRecord getCompletedRun(Id.Program program, final String runid) {
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

      return get(key, RunRecord.class);
    } else {
      // If start time is not found, scan the table (backwards compatibility when run ids were random UUIDs)
      MDSKey startKey = new MDSKey.Builder(completedKey).add(getInvertedTsScanKeyPart(Long.MAX_VALUE)).build();
      MDSKey stopKey = new MDSKey.Builder(completedKey).add(getInvertedTsScanKeyPart(0)).build();
      List<RunRecord> runRecords =
        list(startKey, stopKey, RunRecord.class, 1,  // Should have only one record for this runid
             new Predicate<RunRecord>() {
               @Override
               public boolean apply(RunRecord input) {
                 return input.getPid().equals(runid);
               }
             });
      return Iterables.getFirst(runRecords, null);
    }
  }

  private List<RunRecord> getSuspendedRuns(Id.Program program, long startTime, long endTime, int limit,
                                           final String adapter, @Nullable Predicate<RunRecord> filter) {
    return getNonCompleteRuns(program, TYPE_RUN_RECORD_SUSPENDED, startTime, endTime, limit, adapter, filter);
  }

  private List<RunRecord> getActiveRuns(Id.Program program, final long startTime, final long endTime, int limit,
                                        final String adapter, @Nullable Predicate<RunRecord> filter) {
    return getNonCompleteRuns(program, TYPE_RUN_RECORD_STARTED, startTime, endTime, limit, adapter, filter);
    }

  private List<RunRecord> getNonCompleteRuns(Id.Program program, String recordType,
                                             final long startTime, final long endTime, int limit,
                                             final String adapter, Predicate<RunRecord> filter) {
    MDSKey activeKey = getProgramKeyBuilder(recordType, program).build();

    return list(activeKey, null, RunRecord.class, limit, andPredicate(new Predicate<RunRecord>() {
                  @Override
                  public boolean apply(RunRecord input) {
                    boolean normalCheck = input.getStartTs() >= startTime && input.getStartTs() < endTime;
                    // Check if RunRecord matches with passed in adapter name
                    if (normalCheck && adapter != null) {
                      normalCheck = adapter.equals(input.getAdapterName());
                    }
                    return normalCheck;
                  }
                }, filter));
  }

  private List<RunRecord> getHistoricalRuns(Id.Program program, ProgramRunStatus status,
                                            final long startTime, final long endTime, int limit, final String adapter,
                                            @Nullable Predicate<RunRecord> filter) {
    MDSKey historyKey = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, program).build();

    MDSKey start = new MDSKey.Builder(historyKey).add(getInvertedTsScanKeyPart(endTime)).build();
    MDSKey stop = new MDSKey.Builder(historyKey).add(getInvertedTsScanKeyPart(startTime)).build();
    if (status.equals(ProgramRunStatus.ALL)) {
      //return all records (successful and failed)
      return list(start, stop, RunRecord.class, limit, andPredicate(new Predicate<RunRecord>() {
        @Override
        public boolean apply(@Nullable RunRecord record) {
          // Check if RunRecord matches with passed in adapter name
          return (adapter == null) || (record != null && adapter.equals(record.getAdapterName()));
        }
      }, filter));
    }

    if (status.equals(ProgramRunStatus.COMPLETED)) {
      return list(start, stop, RunRecord.class, limit,
                  andPredicate(getPredicate(ProgramController.State.COMPLETED, adapter), filter));
    }
    if (status.equals(ProgramRunStatus.KILLED)) {
      return list(start, stop, RunRecord.class, limit,
                  andPredicate(getPredicate(ProgramController.State.KILLED, adapter), filter));
    }
    return list(start, stop, RunRecord.class, limit,
                andPredicate(getPredicate(ProgramController.State.ERROR, adapter), filter));
  }

  private Predicate<RunRecord> getPredicate(final ProgramController.State state, final String adapter) {
    return new Predicate<RunRecord>() {
      @Override
      public boolean apply(RunRecord record) {
        boolean normalCheck = record.getStatus().equals(state.getRunStatus());
        // Check if RunRecord matches with passed in adapter name
        if (normalCheck && adapter != null) {
          normalCheck = adapter.equals(record.getAdapterName());
        }
        return normalCheck;
      }
    };
  }

  private Predicate<RunRecord> andPredicate(Predicate<RunRecord> first, @Nullable Predicate<RunRecord> second) {
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

  public void writeProgramArgs(Id.Program program, Map<String, String> args) {
    write(new MDSKey.Builder()
            .add(TYPE_PROGRAM_ARGS)
            .add(program.getNamespaceId())
            .add(program.getApplicationId())
            .add(program.getType().name())
            .add(program.getId())
            .build(), new ProgramArgs(args));
  }

  public ProgramArgs getProgramArgs(Id.Program program) {
    return getFirst(new MDSKey.Builder()
                      .add(TYPE_PROGRAM_ARGS)
                      .add(program.getNamespaceId())
                      .add(program.getApplicationId())
                      .add(program.getType().name())
                      .add(program.getId())
                      .build(), ProgramArgs.class);
  }

  public void deleteProgramArgs(Id.Program program) {
    deleteAll(new MDSKey.Builder().add(TYPE_PROGRAM_ARGS)
                .add(program.getNamespaceId())
                .add(program.getApplicationId())
                .add(program.getType().name())
                .add(program.getId())
                .build());
  }

  public void deleteProgramArgs(String namespaceId, String appId) {
    deleteAll(new MDSKey.Builder().add(TYPE_PROGRAM_ARGS, namespaceId, appId).build());
  }

  public void deleteProgramArgs(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_PROGRAM_ARGS, namespaceId).build());
  }

  public void deleteProgramHistory(String namespaceId, String appId) {
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId, appId).build());
  }

  public void deleteProgramHistory(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId).build());
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

  public void writeAdapter(Id.Namespace id, AdapterDefinition adapterSpec,
                           AdapterStatus adapterStatus) {
    write(new MDSKey.Builder().add(TYPE_ADAPTER, id.getId(), adapterSpec.getName()).build(),
          new AdapterMeta(adapterSpec, adapterStatus));
  }

  @Nullable
  public AdapterDefinition getAdapter(Id.Namespace id, String name) {
    AdapterMeta adapterMeta = getAdapterMeta(id, name);
    return adapterMeta == null ?  null : adapterMeta.getSpec();
  }

  @Nullable
  public AdapterStatus getAdapterStatus(Id.Namespace id, String name) {
    AdapterMeta adapterMeta = getAdapterMeta(id, name);
    return adapterMeta == null ?  null : adapterMeta.getStatus();
  }

  @Nullable
  public AdapterStatus setAdapterStatus(Id.Namespace id, String name, AdapterStatus status) {
    AdapterMeta adapterMeta = getAdapterMeta(id, name);
    if (adapterMeta == null) {
      return null;
    }
    AdapterStatus previousStatus = adapterMeta.getStatus();
    writeAdapter(id, adapterMeta.getSpec(), status);
    return previousStatus;
  }

  @SuppressWarnings("unchecked")
  private AdapterMeta getAdapterMeta(Id.Namespace id, String name) {
    return getFirst(new MDSKey.Builder().add(TYPE_ADAPTER, id.getId(), name).build(), AdapterMeta.class);
  }

  public List<AdapterDefinition> getAllAdapters(Id.Namespace id) {
    List<AdapterDefinition> adapterSpecs = Lists.newArrayList();
    List<AdapterMeta> adapterMetas = list(new MDSKey.Builder().add(TYPE_ADAPTER, id.getId()).build(),
                                          AdapterMeta.class);
    for (AdapterMeta adapterMeta : adapterMetas) {
      adapterSpecs.add(adapterMeta.getSpec());
    }
    return adapterSpecs;
  }

  public void deleteAdapter(Id.Namespace id, String name) {
    deleteAll(new MDSKey.Builder().add(TYPE_ADAPTER, id.getId(), name).build());
  }

  public void deleteAllAdapters(Id.Namespace id) {
    deleteAll(new MDSKey.Builder().add(TYPE_ADAPTER, id.getId()).build());
  }

  private MDSKey getNamespaceKey(@Nullable String name) {
    MDSKey.Builder builder = new MDSKey.Builder().add(TYPE_NAMESPACE);
    if (null != name) {
      builder.add(name);
    }
    return builder.build();
  }

  public void recordWorkflowProgramStart(Id.Program program, String programRunId, String workflow,
                                         String workflowRunId, String workflowNodeId, long startTimeInSeconds,
                                         String adapter, String twillRunId) {
    // Get the run record of the Workflow which started this program
    MDSKey key = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_STARTED)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(ProgramType.WORKFLOW.name())
      .add(workflow)
      .add(workflowRunId)
      .build();

    RunRecord record = get(key, RunRecord.class);
    if (record == null) {
      String msg = String.format("No meta found for associated Workflow %s run record %s, while recording run for the" +
                                   " namespace %s app %s type %s program %s runid %s", workflow, workflowRunId,
                                 program.getNamespaceId(), program.getApplicationId(), program.getType().name(),
                                 program.getId(), programRunId);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    // Update the parent Workflow run record by adding node id and program run id in the properties
    Map<String, String> properties = record.getProperties();
    properties.put(workflowNodeId, programRunId);

    write(key, new RunRecord(record.getPid(), record.getStartTs(), null, ProgramRunStatus.RUNNING,
                             record.getAdapterName(), record.getTwillRunId(), properties));

    // Record the program start
    key = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_STARTED)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(programRunId)
      .build();

    write(key, new RunRecord(programRunId, startTimeInSeconds, null, ProgramRunStatus.RUNNING, adapter, twillRunId,
                             ImmutableMap.of("workflowrunid", workflowRunId)));
  }
}

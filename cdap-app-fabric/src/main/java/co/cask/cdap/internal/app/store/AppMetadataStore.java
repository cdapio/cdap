/*
 * Copyright © 2014-2018 Cask Data, Inc.
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
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunCluster;
import co.cask.cdap.proto.ProgramRunClusterStatus;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Store for application metadata.
 *
 * This class is mostly responsible for reading and storing run records. Each program run will have several run
 * records corresponding to state changes that occur during the program run. The rowkeys are of the form:
 *
 * runRecordActive|namespace|app|version|programtype|program|inverted start time|runid
 * runRecordCompleted|namespace|app|version|programtype|program|inverted start time|runid
 *
 * The run count will have the row key of format:
 * runRecordCount|namespace|app|version|programtype|program
 *
 * These rows get deleted whenever state changes, with a new record written on top. In addition, workflow node state
 * is stored as:
 *
 * wns|namespace|app|version|programtype|program|runid|nodeid
 *
 * Workflow node state is updated whenever program state is updated
 * and we notice that the program belongs to a workflow.
 */
public class AppMetadataStore extends MetadataStoreDataset {

  public static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);

  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStore.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type BYTE_TYPE = new TypeToken<byte[]>() { }.getType();
  private static final byte[] APP_VERSION_UPGRADE_KEY = Bytes.toBytes("version.default.store");

  private static final String TYPE_APP_META = "appMeta";
  private static final String TYPE_STREAM = "stream";

  private static final String TYPE_RUN_RECORD_ACTIVE = "runRecordActive";

  // we will keep these since these will be needed during upgrade
  private static final String TYPE_RUN_RECORD_STARTING = "runRecordStarting";
  private static final String TYPE_RUN_RECORD_STARTED = "runRecordStarted";
  private static final String TYPE_RUN_RECORD_SUSPENDED = "runRecordSuspended";
  private static final String TYPE_RUN_RECORD_COMPLETED = "runRecordCompleted";
  private static final String TYPE_WORKFLOW_NODE_STATE = "wns";
  private static final String TYPE_WORKFLOW_TOKEN = "wft";
  private static final String TYPE_NAMESPACE = "namespace";
  private static final String TYPE_MESSAGE = "msg";
  private static final String TYPE_COUNT = "runRecordCount";
  private static final Map<ProgramRunStatus, String> STATUS_TYPE_MAP = ImmutableMap.<ProgramRunStatus, String>builder()
    .put(ProgramRunStatus.PENDING, TYPE_RUN_RECORD_ACTIVE)
    .put(ProgramRunStatus.STARTING, TYPE_RUN_RECORD_ACTIVE)
    .put(ProgramRunStatus.RUNNING, TYPE_RUN_RECORD_ACTIVE)
    .put(ProgramRunStatus.SUSPENDED, TYPE_RUN_RECORD_ACTIVE)
    .put(ProgramRunStatus.COMPLETED, TYPE_RUN_RECORD_COMPLETED)
    .put(ProgramRunStatus.KILLED, TYPE_RUN_RECORD_COMPLETED)
    .put(ProgramRunStatus.FAILED, TYPE_RUN_RECORD_COMPLETED)
    .build();

  // These are for caching the upgraded state to avoid reading from Table again after upgrade is completed
  // The interval is to avoid frequent reading from Table before upgrade is completed
  // The upgrade is done outside of this class asynchronously.
  // One upgrade is completed, the upgradeCompleted() method will be called from the upgrade thread
  // to clear the lastUpgradeCompletedCheck.
  private static final long UPGRADE_COMPLETED_CHECK_INTERVAL = TimeUnit.MINUTES.toMillis(1L);
  private static volatile boolean upgradeCompleted;
  private static long lastUpgradeCompletedCheck;

  private final CConfiguration cConf;

  /**
   * Static method for creating an instance of {@link AppMetadataStore}.
   */
  public static AppMetadataStore create(CConfiguration cConf,
                                        DatasetContext datasetContext,
                                        DatasetFramework datasetFramework) {
    try {
      Table table = DatasetsUtil.getOrCreateDataset(datasetContext, datasetFramework, APP_META_INSTANCE_ID,
                                                    Table.class.getName(), DatasetProperties.EMPTY);
      return new AppMetadataStore(table, cConf);
    } catch (DatasetManagementException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public AppMetadataStore(Table table, CConfiguration cConf) {
    super(table);
    this.cConf = cConf;
  }

  @Override
  protected <T> byte[] serialize(T value) {
    return Bytes.toBytes(GSON.toJson(value));
  }

  @Override
  protected <T> T deserialize(MDSKey key, byte[] serialized, Type typeOfT) {
    if (RunRecordMeta.class.equals(typeOfT)) {
      RunRecordMeta meta = GSON.fromJson(Bytes.toString(serialized), RunRecordMeta.class);
      meta = RunRecordMeta.builder(meta)
        .setProgramRunId(getProgramID(key).run(meta.getPid()))
        .build();
      //noinspection unchecked
      return (T) meta;
    } else {
      return GSON.fromJson(Bytes.toString(serialized), typeOfT);
    }
  }

  @Nullable
  public ApplicationMeta getApplication(ApplicationId appId) {
    return getFirst(new MDSKey.Builder().add(TYPE_APP_META,
                                             appId.getNamespace(),
                                             appId.getApplication(),
                                             appId.getVersion()).build(),
                    ApplicationMeta.class);
  }

  @Nullable
  public ApplicationMeta getApplication(String namespaceId, String appId, String versionId) {
    return getApplication(new ApplicationId(namespaceId, appId, versionId));
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

  public Map<ApplicationId, ApplicationMeta> getApplicationsForAppIds(Collection<ApplicationId> appIds) {
    Map<MDSKey, ApplicationId> keysAppMap = new HashMap<>();
    for (ApplicationId appId: appIds) {
      keysAppMap.put(getApplicationKeyBuilder(TYPE_APP_META, appId).build(), appId);
    }

    Map<MDSKey, ApplicationMeta> metas = getKV(keysAppMap.keySet(), ApplicationMeta.class);
    Map<ApplicationId, ApplicationMeta> result = new HashMap<>();
    for (MDSKey key : metas.keySet()) {
      result.put(keysAppMap.get(key), metas.get(key));
    }
    return result;
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
    ApplicationMeta updated;

    if (existing == null) {
      String msg = String.format("No meta for namespace %s app %s exists", namespaceId, appId);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    updated = ApplicationMeta.updateSpec(existing, spec);
    LOG.trace("Application exists in mds: id: {}, spec: {}", existing);
    write(key, updated);
  }

  /**
   * Return the {@link List} of {@link WorkflowNodeStateDetail} for a given Workflow run.
   */
  public List<WorkflowNodeStateDetail> getWorkflowNodeStates(ProgramRunId workflowRunId) {
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId).build();
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
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId).add(nodeStateDetail.getNodeId()).build();

    write(key, nodeStateDetail);
  }

  private void addWorkflowNodeState(ProgramRunId programRunId, Map<String, String> systemArgs,
                                    ProgramRunStatus status, @Nullable BasicThrowable failureCause, byte[] sourceId) {
    String workflowNodeId = systemArgs.get(ProgramOptionConstants.WORKFLOW_NODE_ID);
    String workflowName = systemArgs.get(ProgramOptionConstants.WORKFLOW_NAME);
    String workflowRun = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);

    ApplicationId appId = programRunId.getParent().getParent();
    ProgramRunId workflowRunId = appId.workflow(workflowName).run(workflowRun);

    // Node states will be stored with following key:
    // workflowNodeState.namespace.app.WORKFLOW.workflowName.workflowRun.workflowNodeId
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId).add(workflowNodeId).build();

    WorkflowNodeStateDetail nodeStateDetail = new WorkflowNodeStateDetail(workflowNodeId,
                                                                          ProgramRunStatus.toNodeStatus(status),
                                                                          programRunId.getRun(), failureCause);

    write(key, nodeStateDetail);

    // Get the run record of the Workflow which started this program
    key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, workflowRunId,
                                       RunIds.getTime(workflowRun, TimeUnit.SECONDS));

    RunRecordMeta record = get(key, RunRecordMeta.class);
    if (record != null) {
      // Update the parent Workflow run record by adding node id and program run id in the properties
      Map<String, String> properties = new HashMap<>(record.getProperties());
      properties.put(workflowNodeId, programRunId.getRun());
      write(key, RunRecordMeta.builder(record).setProperties(properties).setSourceId(sourceId).build());
    }
  }

  /**
   * Record that the program run is provisioning compute resources for the run. If the current status has
   * a higher source id, this call will be ignored.
   *
   * @param programRunId program run
   * @param runtimeArgs runtime arguments
   * @param systemArgs system arguments
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @param artifactId artifact id of the program's application -
   *                   its null only for older messages that were not processed before upgrading to 5.0
   * @return {@link ProgramRunClusterStatus#PROVISIONING} if it is successfully persisted, {@code null} otherwise.
   */
  @Nullable
  public RunRecordMeta recordProgramProvisioning(ProgramRunId programRunId, Map<String, String> runtimeArgs,
                                                 Map<String, String> systemArgs, byte[] sourceId,
                                                 @Nullable ArtifactId artifactId) {
    long startTs = RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS);
    MDSKey key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, startTs);
    if (startTs == -1L) {
      LOG.error("Ignoring unexpected request to record provisioning state for program run {} that does not have " +
                  "a timestamp in the run id.");
      return null;
    }

    RunRecordMeta existing = getRun(programRunId);
    // for some reason, there is an existing run record.
    if (existing != null) {
      LOG.error("Ignoring unexpected request to record provisioning state for program run {} that has an existing "
                  + "run record in run state {} and cluster state {}.",
                programRunId, existing.getStatus(), existing.getCluster().getStatus());
      return null;
    }

    Optional<ProfileId> profileId = SystemArguments.getProfileIdFromArgs(programRunId.getNamespaceId(), systemArgs);
    if (!profileId.isPresent()) {
      LOG.error("Ignoring unexpected request to record provisioning state for program run {} that does not have "
                  + "a profile assigned to it.", programRunId);
      return null;
    }

    ProgramRunCluster cluster = new ProgramRunCluster(ProgramRunClusterStatus.PROVISIONING, null, null);
    RunRecordMeta meta = RunRecordMeta.builder()
      .setProgramRunId(programRunId)
      .setStartTime(startTs)
      .setStatus(ProgramRunStatus.PENDING)
      .setProperties(getRecordProperties(systemArgs, runtimeArgs))
      .setSystemArgs(systemArgs)
      .setCluster(cluster)
      .setProfileId(profileId.get())
      .setSourceId(sourceId)
      .setArtifactId(artifactId)
      .setPrincipal(systemArgs.get(ProgramOptionConstants.PRINCIPAL))
      .build();
    write(key, meta);
    MDSKey countKey = getProgramKeyBuilder(TYPE_COUNT, programRunId.getParent()).build();
    increment(countKey, 1L);
    LOG.trace("Recorded {} for program {}", ProgramRunClusterStatus.PROVISIONING, programRunId);
    return meta;
  }

  // return the property map to set in the RunRecordMeta
  private Map<String, String> getRecordProperties(Map<String, String> systemArgs, Map<String, String> runtimeArgs) {
    String workflowRunId = null;
    if (systemArgs != null && systemArgs.containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      workflowRunId = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);
    }

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("runtimeArgs", GSON.toJson(runtimeArgs, MAP_STRING_STRING_TYPE));
    if (workflowRunId != null) {
      builder.put("workflowrunid", workflowRunId);
    }
    return builder.build();
  }

  /**
   * Record that the program run has completed provisioning compute resources for the run. If the current status has
   * a higher source id, this call will be ignored.
   *
   * @param programRunId program run
   * @param numNodes number of cluster nodes provisioned
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @return {@link RunRecordMeta} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordMeta recordProgramProvisioned(ProgramRunId programRunId, int numNodes, byte[] sourceId) {
    RunRecordMeta existing = getRun(programRunId);

    if (existing == null) {
      LOG.warn("Ignoring unexpected request to transition program run {} from non-existent state to cluster state {}.",
                programRunId, ProgramRunClusterStatus.PROVISIONED);
      return null;
    }
    if (!isValid(existing, existing.getStatus(), ProgramRunClusterStatus.PROVISIONED, sourceId)) {
      return null;
    }

    MDSKey key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, existing.getStartTs());
    ProgramRunCluster cluster = new ProgramRunCluster(ProgramRunClusterStatus.PROVISIONED, null, numNodes);
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    write(key, meta);
    LOG.trace("Recorded {} for program {}", ProgramRunClusterStatus.PROVISIONED, programRunId);
    return meta;
  }

  /**
   * Record that the program run has started de-provisioning compute resources for the run. If the current status has
   * a higher source id, this call will be ignored.
   *
   * @param programRunId program run
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @return {@link RunRecordMeta} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordMeta recordProgramDeprovisioning(ProgramRunId programRunId, byte[] sourceId) {

    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.debug("Ignoring unexpected transition of program run {} to cluster state {} with no existing run record.",
                programRunId, ProgramRunClusterStatus.DEPROVISIONING);
      return null;
    }
    if (!isValid(existing, existing.getStatus(), ProgramRunClusterStatus.DEPROVISIONING, sourceId)) {
      return null;
    }

    delete(existing);

    MDSKey key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_COMPLETED, programRunId, existing.getStartTs());

    ProgramRunCluster cluster = new ProgramRunCluster(ProgramRunClusterStatus.DEPROVISIONING, null,
                                                      existing.getCluster().getNumNodes());
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    write(key, meta);
    LOG.trace("Recorded {} for program {}", ProgramRunClusterStatus.DEPROVISIONING, programRunId);
    return meta;
  }

  /**
   * Record that the program run has deprovisioned compute resources for the run. If the current status has
   * a higher source id, this call will be ignored.
   *
   * @param programRunId program run
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @param endTs timestamp in seconds for when the cluster was deprovisioned. This is null if the program is run
   *              as part of a workflow
   * @return {@link RunRecordMeta} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordMeta recordProgramDeprovisioned(ProgramRunId programRunId, @Nullable Long endTs, byte[] sourceId) {
    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.debug("Ignoring unexpected transition of program run {} to cluster state {} with no existing run record.",
                programRunId, ProgramRunClusterStatus.DEPROVISIONED);
      return null;
    }
    if (!isValid(existing, existing.getStatus(), ProgramRunClusterStatus.DEPROVISIONED, sourceId)) {
      return null;
    }

    delete(existing);
    MDSKey key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_COMPLETED, programRunId, existing.getStartTs());

    ProgramRunCluster cluster = new ProgramRunCluster(ProgramRunClusterStatus.DEPROVISIONED, endTs,
                                                      existing.getCluster().getNumNodes());
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    write(key, meta);
    LOG.trace("Recorded {} for program {}", ProgramRunClusterStatus.DEPROVISIONED, programRunId);
    return meta;
  }

  /**
   * Record that the program run has been orphaned. If the current status has a higher source id,
   * this call will be ignored.
   *
   * @param programRunId program run
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @param endTs timestamp in seconds for when the cluster was orphaned
   * @return {@link RunRecordMeta} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordMeta recordProgramOrphaned(ProgramRunId programRunId, long endTs, byte[] sourceId) {
    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.debug("Ignoring unexpected transition of program run {} to cluster state {} with no existing run record.",
                programRunId, ProgramRunClusterStatus.DEPROVISIONED);
      return null;
    }
    if (!isValid(existing, existing.getStatus(), ProgramRunClusterStatus.ORPHANED, sourceId)) {
      return null;
    }

    delete(existing);
    MDSKey key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_COMPLETED, programRunId, existing.getStartTs());

    ProgramRunCluster cluster = new ProgramRunCluster(ProgramRunClusterStatus.ORPHANED, endTs,
                                                      existing.getCluster().getNumNodes());
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    write(key, meta);
    LOG.trace("Recorded {} for program {}", ProgramRunClusterStatus.ORPHANED, programRunId);
    return meta;
  }

  /**
   * Logs initialization of program run and persists program status to {@link ProgramRunStatus#STARTING}.
   * @param programRunId run id of the program
   * @param twillRunId Twill run id
   * @param systemArgs the system arguments for this program run
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @return {@link RunRecordMeta} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordMeta recordProgramStart(ProgramRunId programRunId, @Nullable String twillRunId,
                                          Map<String, String> systemArgs, byte[] sourceId) {
    RunRecordMeta existing = getRun(programRunId);
    RunRecordMeta meta;

    if (systemArgs.containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      addWorkflowNodeState(programRunId, systemArgs, ProgramRunStatus.STARTING, null, sourceId);
    }

    if (existing == null) {
      LOG.warn("Ignoring unexpected transition of program run {} to program state {} with no existing run record.",
               programRunId, ProgramRunStatus.STARTING);
      return null;
    }
    if (!isValid(existing, ProgramRunStatus.STARTING, existing.getCluster().getStatus(), sourceId)) {
      return null;
    }

    // Delete the old run record
    delete(existing);
    MDSKey key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, existing.getStartTs());
    meta = RunRecordMeta.builder(existing)
      .setStatus(ProgramRunStatus.STARTING)
      .setTwillRunId(twillRunId)
      .setSourceId(sourceId)
      .build();
    write(key, meta);
    LOG.trace("Recorded {} for program {}", ProgramRunStatus.STARTING, programRunId);
    return meta;
  }

  /**
   * Logs start of program run and persists program status to {@link ProgramRunStatus#RUNNING}.
   * @param programRunId run id of the program
   * @param stateChangeTime start timestamp in seconds
   * @param twillRunId Twill run id
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @return {@link RunRecordMeta} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordMeta recordProgramRunning(ProgramRunId programRunId, long stateChangeTime, String twillRunId,
                                            byte[] sourceId) {
    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.warn("Ignoring unexpected transition of program run {} to program state {} with no existing run record.",
               programRunId, ProgramRunStatus.RUNNING);
      return null;
    }
    if (!isValid(existing, ProgramRunStatus.RUNNING, existing.getCluster().getStatus(), sourceId)) {
      // Skip recording running if the existing records are not valid
      return null;
    }
    Map<String, String> systemArgs = existing.getSystemArgs();
    if (systemArgs != null && systemArgs.containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      // Program was started by Workflow. Add row corresponding to its node state.
      addWorkflowNodeState(programRunId, systemArgs, ProgramRunStatus.RUNNING, null, sourceId);
    }

    // Delete the old run record
    delete(existing);
    MDSKey key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, existing.getStartTs());

    // The existing record's properties already contains the workflowRunId
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setRunTime(stateChangeTime)
      .setStatus(ProgramRunStatus.RUNNING)
      .setTwillRunId(twillRunId)
      .setSourceId(sourceId)
      .build();
    write(key, meta);
    LOG.trace("Recorded {} for program {}", ProgramRunStatus.RUNNING, programRunId);
    return meta;
  }

  /**
   * Logs suspend of a program run and sets the run status to {@link ProgramRunStatus#SUSPENDED}.
   * @param programRunId run id of the program
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @return {@link RunRecordMeta} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordMeta recordProgramSuspend(ProgramRunId programRunId, byte[] sourceId, long timestamp) {
    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.warn("Ignoring unexpected transition of program run {} to program state {} with no existing run record.",
               programRunId, ProgramRunStatus.SUSPENDED);
      return null;
    }
    if (!isValid(existing, ProgramRunStatus.SUSPENDED, existing.getCluster().getStatus(), sourceId)) {
      // Skip recording suspend if the existing record is not valid
      return null;
    }
    return recordProgramSuspendResume(programRunId, sourceId, existing, "suspend", timestamp);
  }

  /**
   * Logs resume of a program run and sets the run status to {@link ProgramRunStatus#RUNNING}.
   * @param programRunId run id of the program
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @return {@link RunRecordMeta} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordMeta recordProgramResumed(ProgramRunId programRunId, byte[] sourceId, long timestamp) {
    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.warn("Ignoring unexpected transition of program run {} to program state {} with no existing run record.",
               programRunId, ProgramRunStatus.RUNNING);
      return null;
    }
    if (!isValid(existing, ProgramRunStatus.RUNNING, existing.getCluster().getStatus(), sourceId)) {
      // Skip recording resumed if the existing records are not valid
      return null;
    }
    return recordProgramSuspendResume(programRunId, sourceId, existing, "resume", timestamp);
  }

  private RunRecordMeta recordProgramSuspendResume(ProgramRunId programRunId, byte[] sourceId,
                                                   RunRecordMeta existing, String action, long timestamp) {
    ProgramRunStatus toStatus = ProgramRunStatus.SUSPENDED;

    if (action.equals("resume")) {
      toStatus = ProgramRunStatus.RUNNING;
    }
    // Delete the old run record
    delete(existing);
    MDSKey key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, existing.getStartTs());
    RunRecordMeta.Builder builder = RunRecordMeta.builder(existing).setStatus(toStatus).setSourceId(sourceId);
    if (timestamp != -1) {
      if (action.equals("resume")) {
        builder.setResumeTime(timestamp);
      } else {
        builder.setSuspendTime(timestamp);
      }
    }
    RunRecordMeta meta = builder.build();
    write(key, meta);
    LOG.trace("Recorded {} for program {}", toStatus, programRunId);
    return meta;
  }

  /**
   * Logs end of program run and sets the run status to the given run status with a failure cause.
   * @param programRunId run id of the program
   * @param stopTs stop timestamp in seconds
   * @param runStatus {@link ProgramRunStatus} of program run
   * @param failureCause failure cause if the program failed to execute
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @return {@link RunRecordMeta} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordMeta recordProgramStop(ProgramRunId programRunId, long stopTs, ProgramRunStatus runStatus,
                                         @Nullable BasicThrowable failureCause, byte[] sourceId) {
    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.warn("Ignoring unexpected transition of program run {} to program state {} with no existing run record.",
               programRunId, runStatus);
      return null;
    }
    if (!isValid(existing, runStatus, existing.getCluster().getStatus(), sourceId)) {
      // Skip recording stop if the existing records are not valid
      return null;
    }
    // Delete the old run record
    delete(existing);

    // Record in the workflow
    Map<String, String> systemArgs = existing.getSystemArgs();
    if (systemArgs != null && systemArgs.containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      addWorkflowNodeState(programRunId, systemArgs, runStatus, failureCause, sourceId);
    }

    MDSKey key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_COMPLETED, programRunId, existing.getStartTs());
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setStopTime(stopTs)
      .setStatus(runStatus)
      .setSourceId(sourceId)
      .build();
    write(key, meta);
    LOG.trace("Recorded {} for program {}", runStatus, programRunId);
    return meta;
  }

  /**
   * Checks whether the existing run record meta of a given program run are in a state for
   * the program run to transition into the given run status.
   * This is required because program states are not guaranteed to be written in order.
   * For example, starting can be written from a twill AM, while running may be written from a twill runnable.
   * If the running state is written before the starting state, we don't want to record the state as starting
   * once it is already running.
   *
   * @param existing the existing run record meta of the given program run
   * @param nextProgramState the program state to transition to
   * @param nextClusterState the cluster state to transition to
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @return {@code true} if the program run is allowed to persist the given status, {@code false} otherwise
   */
  private boolean isValid(RunRecordMeta existing, ProgramRunStatus nextProgramState,
                          ProgramRunClusterStatus nextClusterState, byte[] sourceId) {
    byte[] existingSourceId = existing.getSourceId();
    if (existingSourceId != null && Bytes.compareTo(sourceId, existingSourceId) < 0) {
      LOG.debug("Current source id '{}' is not larger than the existing source id '{}' in the existing " +
                  "run record meta '{}'. Skip recording state transition to program state {} and cluster state {}.",
                Bytes.toHexString(sourceId), Bytes.toHexString(existingSourceId), existing,
                nextProgramState, nextClusterState);
      return false;
    }
    // sometimes we expect duplicate messages. For example, multiple KILLED messages are sent, one by the CDAP master
    // and one by the program. In these cases, we don't need to write, but we don't want to log a warning
    if (existing.getStatus() == nextProgramState && existing.getCluster().getStatus() == nextClusterState) {
      return false;
    }
    if (!existing.getStatus().canTransitionTo(nextProgramState)) {
      LOG.warn("Ignoring unexpected transition of program run {} from run state {} to {}.",
               existing.getProgramRunId(), existing.getStatus(), nextProgramState);
      return false;
    }
    if (!existing.getCluster().getStatus().canTransitionTo(nextClusterState)) {
      LOG.warn("Ignoring unexpected transition of program run {} from cluster state {} to {}.",
               existing.getProgramRunId(), existing.getCluster().getStatus(), nextClusterState);
      return false;
    }
    return true;
  }

  public Map<ProgramRunId, RunRecordMeta> getRuns(Set<ProgramRunId> programRunIds) {
    return getRuns(programRunIds, Integer.MAX_VALUE);
  }

  /**
   * Get active runs in the given set of namespaces that satisfies a filter, active runs means program run with status
   * STARTING, PENDING, RUNNING or SUSPENDED.
   *
   * @param namespaces set of namespaces
   * @param filter filter to filter run record
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(Set<NamespaceId> namespaces, Predicate<RunRecordMeta> filter) {
    return namespaces.stream().flatMap(namespaceId -> {
      MDSKey key = getNamespaceKeyBuilder(TYPE_RUN_RECORD_ACTIVE, namespaceId).build();
      Map<ProgramRunId, RunRecordMeta> activeRuns = getProgramRunIdMap(listKV(key, null, RunRecordMeta.class,
                                                                              Integer.MAX_VALUE, filter));
      return activeRuns.entrySet().stream();
    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Get active runs in all namespaces with a filter, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param filter filter to filter run record
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(Predicate<RunRecordMeta> filter) {
    MDSKey key = getNamespaceKeyBuilder(TYPE_RUN_RECORD_ACTIVE, null).build();
    return getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, Integer.MAX_VALUE, filter));
  }

  /**
   * Get active runs in the given namespace, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param namespaceId given namespace
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(NamespaceId namespaceId) {
    // TODO CDAP-12361 should consolidate these methods and get rid of duplicate / unnecessary methods.
    Predicate<RunRecordMeta> timePredicate = getTimeRangePredicate(0, Long.MAX_VALUE);
    MDSKey key = getNamespaceKeyBuilder(TYPE_RUN_RECORD_ACTIVE, namespaceId).build();
    return getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, Integer.MAX_VALUE, timePredicate));
  }

  /**
   * Get active runs in the given application, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param applicationId given app
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(ApplicationId applicationId) {
    Predicate<RunRecordMeta> timePredicate = getTimeRangePredicate(0, Long.MAX_VALUE);
    MDSKey key = getApplicationKeyBuilder(TYPE_RUN_RECORD_ACTIVE, applicationId).build();
    return getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, Integer.MAX_VALUE, timePredicate));
  }

  /**
   * Get active runs in the given program, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param programId given program
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(ProgramId programId) {
    Predicate<RunRecordMeta> timePredicate = getTimeRangePredicate(0, Long.MAX_VALUE);
    MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_ACTIVE, programId).build();

    return getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, Integer.MAX_VALUE, timePredicate));
  }

  private Map<ProgramRunId, RunRecordMeta> getRuns(Set<ProgramRunId> programRunIds, int limit) {
    Map<ProgramRunId, RunRecordMeta> resultMap = new LinkedHashMap<>();
    for (String type : Arrays.asList(TYPE_RUN_RECORD_ACTIVE, TYPE_RUN_RECORD_COMPLETED)) {
      resultMap.putAll(getRunsForRunIds(programRunIds, type, limit - resultMap.size()));
    }
    return resultMap;
  }

  public Map<ProgramRunId, RunRecordMeta> getRuns(@Nullable ProgramId programId, final ProgramRunStatus status,
                                                  long startTime, long endTime, int limit,
                                                  @Nullable Predicate<RunRecordMeta> filter) {
    switch (status) {
      case ALL:
        Map<ProgramRunId, RunRecordMeta> runRecords = new LinkedHashMap<>();
        runRecords.putAll(getNonCompleteRuns(programId, TYPE_RUN_RECORD_ACTIVE, startTime, endTime,
                                             limit - runRecords.size(), filter));
        runRecords.putAll(getHistoricalRuns(programId, status, startTime, endTime, limit - runRecords.size(), filter));
        return runRecords;
      case PENDING:
      case STARTING:
      case RUNNING:
      case SUSPENDED:
        Predicate<RunRecordMeta> stateFilter = record -> record.getStatus() == status;
        if (filter != null) {
          stateFilter = stateFilter.and(filter);
        }
        return getNonCompleteRuns(programId, TYPE_RUN_RECORD_ACTIVE, startTime, endTime, limit, stateFilter);
      default:
        return getHistoricalRuns(programId, status, startTime, endTime, limit, filter);
    }
  }

  // TODO: getRun is duplicated in cdap-watchdog AppMetadataStore class.
  // Any changes made here will have to be made over there too.
  // JIRA https://issues.cask.co/browse/CDAP-2172
  @Nullable
  public RunRecordMeta getRun(ProgramRunId programRun) {
    // Query active run record first
    RunRecordMeta running = getUnfinishedRun(programRun);
    // If program is running, this will be non-null
    if (running != null) {
      return running;
    }
    // If program is not running, query completed run records
    return getCompletedRun(programRun);
  }

  private void delete(RunRecordMeta record) {
    ProgramRunId programRunId = record.getProgramRunId();
    MDSKey key = getProgramRunInvertedTimeKey(STATUS_TYPE_MAP.get(record.getStatus()), programRunId,
                                              record.getStartTs());
    deleteAll(key);
  }

  /**
   * @return run records for unfinished run
   */
  private RunRecordMeta getUnfinishedRun(ProgramRunId programRunId) {
    MDSKey runningKey = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId,
                                                     RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS));
    return get(runningKey, RunRecordMeta.class);
  }

  private RunRecordMeta getCompletedRun(ProgramRunId programRunId) {
    MDSKey completedKey = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programRunId.getParent()).build();
    return getCompletedRun(completedKey, programRunId.getRun());
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
             (input) -> input.getPid().equals(runid));
      return Iterables.getFirst(runRecords, null);
    }
  }

  private Map<ProgramRunId, RunRecordMeta> getNonCompleteRuns(@Nullable ProgramId programId, String recordType,
                                                              final long startTime, final long endTime, int limit,
                                                              Predicate<RunRecordMeta> filter) {
    Predicate<RunRecordMeta> valuePredicate = andPredicate(getTimeRangePredicate(startTime, endTime), filter);

    if (programId == null || !programId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      MDSKey key = getProgramKeyBuilder(recordType, programId).build();
      return getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, limit, valuePredicate));
    }

    Predicate<MDSKey> keyPredicate = new AppVersionPredicate(ApplicationId.DEFAULT_VERSION);
    MDSKey key = getProgramKeyBuilder(recordType, programId).build();
    Map<MDSKey, RunRecordMeta> newRecords = listKV(key, null, RunRecordMeta.class, limit, keyPredicate, valuePredicate);
    return getProgramRunIdMap(newRecords);
  }

  private Map<ProgramRunId, RunRecordMeta> getRunsForRunIds(final Set<ProgramRunId> runIds, String recordType,
                                                            int limit) {
    Set<MDSKey> keySet = new HashSet<>();
    for (ProgramRunId programRunId : runIds) {
      keySet.add(getProgramKeyBuilder(recordType, programRunId.getParent()).build());
    }
    
    Predicate<KeyValue<RunRecordMeta>> combinedFilter = input -> {
      ProgramId programId = getProgramID(input.getKey());
      RunRecordMeta meta = input.getValue();
      ProgramRunId programRunId = programId.run(meta.getPid());
      return runIds.contains(programRunId);
    };

    Map<MDSKey, RunRecordMeta> returnMap = listKV(keySet, RunRecordMeta.class, limit, combinedFilter);
    return getProgramRunIdMap(returnMap);
  }

  /**
   * Converts MDSkeys in the map to ProgramIDs
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
    return getHistoricalRuns(key, status, startTime, endTime, limit, keyPredicate, filter);
  }

  /**
   * Fetches the historical (i.e COMPLETED or FAILED or KILLED) run records from a given set of namespaces.
   *
   * @param namespaces fetch run history that is belonged to one of these namespaces
   * @param earliestStopTime fetch run history that has stopped at or after the earliestStopTime in seconds
   * @param latestStartTime fetch run history that has started before the latestStartTime in seconds
   * @param limit max number of entries to fetch for this history call
   * @return map of logged runs
   */
  public Map<ProgramRunId, RunRecordMeta> getHistoricalRuns(final Set<NamespaceId> namespaces,
                                                            final long earliestStopTime, final long latestStartTime,
                                                            final int limit) {
    MDSKey keyPrefix = new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED).build();
    //return all records in each namespace
    return namespaces.stream()
      .flatMap(ns -> getProgramRunIdMap(
        listKV(new MDSKey.Builder(keyPrefix).add(ns.getNamespace()).build(), null,
               RunRecordMeta.class, limit, key -> true,
               // get active runs in a time window with range [earliestStopTime, latestStartTime),
               // which excludes program run records that stopped before earliestStopTime and
               // program run records that started after latestStartTime, all remaining records are active
               // at some point within the time window and will be returned
               meta -> meta.getStopTs() != null && meta.getStopTs() >= earliestStopTime
                 && meta.getStartTs() < latestStartTime)).entrySet().stream())
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
                                       valueFilter == null ? x -> true : valueFilter));
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
    return (record) -> record.getStatus().equals(state.getRunStatus());
  }

  private Predicate<RunRecordMeta> getTimeRangePredicate(final long startTime, final long endTime) {
    return (record) -> record.getStartTs() >= startTime && record.getStartTs() < endTime;
  }

  private Predicate<RunRecordMeta> andPredicate(Predicate<RunRecordMeta> first,
                                                @Nullable Predicate<RunRecordMeta> second) {
    if (second != null) {
      return first.and(second);
    }
    return first;
  }

  private long getInvertedTsKeyPart(long time) {
    return Long.MAX_VALUE - time;
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

  public void deleteProgramHistory(String namespaceId, String appId, String versionId) {
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_ACTIVE, namespaceId, appId, versionId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId, appId, versionId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_COUNT, namespaceId, appId, versionId).build());
  }

  public void deleteProgramHistory(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_ACTIVE, namespaceId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_COUNT, namespaceId).build());
  }

  public void createNamespace(NamespaceMeta metadata) {
    write(getNamespaceKey(metadata.getName()), metadata);
  }

  private MDSKey getNamespaceKey(@Nullable String name) {
    MDSKey.Builder builder = new MDSKey.Builder().add(TYPE_NAMESPACE);
    if (null != name) {
      builder.add(name);
    }
    return builder.build();
  }

  /**
   * Sets the {@link WorkflowToken} for the given workflow run.
   *
   * @param workflowRunId the {@link ProgramRunId} representing the workflow run
   * @param workflowToken the {@link WorkflowToken} to set to
   */
  public void setWorkflowToken(ProgramRunId workflowRunId, WorkflowToken workflowToken) {
    if (workflowRunId.getType() != ProgramType.WORKFLOW) {
      throw new IllegalArgumentException("WorkflowToken can only be set for workflow execution: " + workflowRunId);
    }

    // Workflow token will be stored with following key:
    // [wft][namespace][app][WORKFLOW][workflowName][workflowRun]
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_TOKEN, workflowRunId).build();

    write(key, workflowToken);
  }

  public WorkflowToken getWorkflowToken(ProgramId workflowId, String workflowRunId) {
    Preconditions.checkArgument(ProgramType.WORKFLOW == workflowId.getType());
    // Workflow token is stored with following key:
    // [wft][namespace][app][version][WORKFLOW][workflowName][workflowRun]
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_TOKEN, workflowId.run(workflowRunId)).build();

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
    Iterables.addAll(runIds, getRunningInRangeForStatus(TYPE_RUN_RECORD_ACTIVE, startTimeInSecs, endTimeInSecs,
                                                        scanTimeoutMills));
    return runIds;
  }

  /**
   * Get the run count of the given program.
   *
   * @param programId the program to get the count
   * @return the number of run count
   */
  public long getProgramRunCount(ProgramId programId) {
    MDSKey key = getProgramKeyBuilder(TYPE_COUNT, programId).build();
    byte[] count = getValue(key);
    return count == null ? 0 : Bytes.toLong(count);
  }

  /**
   * Get the run counts of the given program collections.
   *
   * @param programIds the collection of program ids to get the program
   * @return the map of the program id to its run count
   */
  public Map<ProgramId, Long> getProgramRunCounts(Collection<ProgramId> programIds) throws BadRequestException {
    Map<MDSKey, ProgramId> mdsKeyProgramIdMap = new HashMap<>();
    if (programIds.size() > 100) {
      throw new BadRequestException(String.format("%d programs found, the maximum number supported is 100",
                                                  programIds.size()));
    }
    for (ProgramId programId : programIds) {
      MDSKey key = getProgramKeyBuilder(TYPE_COUNT, programId).build();
      mdsKeyProgramIdMap.put(key, programId);
    }
    Map<MDSKey, byte[]> counts = getKV(mdsKeyProgramIdMap.keySet());
    Map<ProgramId, Long> result = new LinkedHashMap<>();
    for (MDSKey mdsKey : mdsKeyProgramIdMap.keySet()) {
      result.put(mdsKeyProgramIdMap.get(mdsKey), counts.containsKey(mdsKey) ? Bytes.toLong(counts.get(mdsKey)) : 0L);
    }
    return result;
  }

  /**
   * @return true if the upgrade of the app meta store is complete
   * TODO: CDAP-14114 change the logic to run count upgrade
   */
  private boolean hasUpgraded() {
    boolean upgraded = upgradeCompleted;
    if (upgraded) {
      return true;
    }

    // See if need to check the Table or not
    long now = System.currentTimeMillis();
    if (now - lastUpgradeCompletedCheck < UPGRADE_COMPLETED_CHECK_INTERVAL) {
      return false;
    }

    synchronized (AppMetadataStore.class) {
      // Check again, as it can be checked by other thread
      upgraded = upgradeCompleted;
      if (upgraded) {
        return true;
      }

      lastUpgradeCompletedCheck = now;

      MDSKey.Builder keyBuilder = new MDSKey.Builder();
      keyBuilder.add(APP_VERSION_UPGRADE_KEY);
      String version = get(keyBuilder.build(), String.class);
      if (version == null) {
        return false;
      }
      ProjectInfo.Version actual = new ProjectInfo.Version(version);
      upgradeCompleted = upgraded = actual.compareTo(ProjectInfo.getVersion()) >= 0;
    }
    return upgraded;
  }

  /**
   * Mark the table that the upgrade is complete.
   * TODO: CDAP-14114 change the logic to run count upgrade
   */
  public void upgradeCompleted() {
    MDSKey.Builder keyBuilder = new MDSKey.Builder();
    keyBuilder.add(APP_VERSION_UPGRADE_KEY);
    write(keyBuilder.build(), ProjectInfo.getVersion().toString());

    // Reset the lastUpgradeCompletedCheck to 0L to force checking from Table next time when hasUpgraded is called
    synchronized (AppMetadataStore.class) {
      lastUpgradeCompletedCheck = 0L;
    }
  }

  /**
   * Gets the id of the last fetched message that was set for a subscriber of the given TMS topic
   *
   * @param topic the topic to lookup the last message id
   * @param subscriber the subscriber name
   * @return the id of the last fetched message for this subscriber on this topic,
   *         or {@code null} if no message id was stored before
   */
  @Nullable
  public String retrieveSubscriberState(String topic, String subscriber) {
    MDSKey.Builder keyBuilder = new MDSKey.Builder().add(TYPE_MESSAGE).add(topic);

    // For backward compatibility, skip the subscriber part if it is empty or null
    if (subscriber != null && !subscriber.isEmpty()) {
      keyBuilder.add(subscriber);
    }

    byte[] rawBytes = get(keyBuilder.build(), BYTE_TYPE);
    return (rawBytes == null) ? null : Bytes.toString(rawBytes);
  }

  /**
   * Updates the given topic's last fetched message id with the given message id for the given subscriber.
   *
   * @param topic the topic to persist the message id
   * @param subscriber the subscriber name
   * @param messageId the most recently processed message id
   */
  public void persistSubscriberState(String topic, String subscriber, String messageId) {
    MDSKey.Builder keyBuilder = new MDSKey.Builder().add(TYPE_MESSAGE).add(topic);

    // For backward compatibility, skip the subscriber part if it is empty or null
    if (subscriber != null && !subscriber.isEmpty()) {
      keyBuilder.add(subscriber);
    }

    write(keyBuilder.build(), Bytes.toBytes(messageId));
  }

  /**
   * Deletes the given topic's last fetched message id with the given message id for the given subscriber.
   *
   * @param topic the topic to persist the message id
   * @param subscriber the subscriber name
   */
  public void deleteSubscriberState(String topic, String subscriber) {
    MDSKey.Builder keyBuilder = new MDSKey.Builder().add(TYPE_MESSAGE).add(topic);
    keyBuilder.add(subscriber);
    delete(keyBuilder.build());
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
    Predicate<RunRecordMeta> timeFilter = (runRecordMeta) ->
      runRecordMeta.getStartTs() < endTimeInSecs &&
        (runRecordMeta.getStopTs() == null || runRecordMeta.getStopTs() >= startTimeInSecs);

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
      batches.add(Iterables.transform(scanFunction.getValues(),
                                      runRecordMeta -> RunIds.fromString(runRecordMeta.getPid())));
      // key for next scan is the last key + 1 from the previous scan
      startKey = new MDSKey(Bytes.stopKeyForPrefix(scanFunction.getLastKey().getKey()));
    }
    return batches;
  }

  /**
   * Returns a ProgramId given the MDS key
   *
   * @param key the MDS key to be used
   * @return ProgramId created from the MDS key
   */
  private static ProgramId getProgramID(MDSKey key) {
    MDSKey.Splitter splitter = key.split();

    // Format : recordType, ns, app, version, type, program, ts, runid

    // record type
    splitter.getString();
    String namespace = splitter.getString();
    String application = splitter.getString();
    String appVersion = splitter.getString();
    String type = splitter.getString();
    String program = splitter.getString();

    return (new ApplicationId(namespace, application, appVersion).program(ProgramType.valueOf(type), program));
  }

  private MDSKey getProgramRunInvertedTimeKey(String recordType, ProgramRunId runId, long startTs) {
    return getProgramKeyBuilder(recordType, runId.getParent())
      .add(getInvertedTsKeyPart(startTs))
      .add(runId.getRun())
      .build();
  }

  private static class AppVersionPredicate implements Predicate<MDSKey> {
    private final String version;

    AppVersionPredicate(String version) {
      this.version = version;
    }

    @Override
    public boolean test(MDSKey input) {
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
      if (filter.test(input.getValue())) {
        values.add(input.getValue());
      }
      return true;
    }
  }
}

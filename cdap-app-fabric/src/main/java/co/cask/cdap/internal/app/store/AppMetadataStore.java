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
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunCluster;
import co.cask.cdap.proto.ProgramRunClusterStatus;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
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

import java.lang.reflect.Type;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Store for application metadata.
 *
 * This class is mostly responsible for reading and storing run records. Each program run will have several run
 * records corresponding to state changes that occur during the program run. The rowkeys are of the form:
 *
 * runRecordStarting|namespace|app|version|programtype|program|runid
 * runRecordStarted|namespace|app|version|programtype|program|runid
 * runRecordSuspended|namespace|app|version|programtype|program|runid
 * runRecordCompleted|namespace|app|version|programtype|program|inverted start time|runid
 *
 * These rows get deleted whenever state changes, with a new record written on top. In addition, workflow node state
 * is stored as:
 *
 * wns|namespace|app|version|programtype|program|runid|nodeid
 *
 * Workflow node state is updated whenever program state is updated
 * and we notice that the program belongs to a workflow.
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
  private static final Map<ProgramRunStatus, String> STATUS_TYPE_MAP = ImmutableMap.<ProgramRunStatus, String>builder()
    .put(ProgramRunStatus.PENDING, TYPE_RUN_RECORD_STARTING)
    .put(ProgramRunStatus.STARTING, TYPE_RUN_RECORD_STARTING)
    .put(ProgramRunStatus.RUNNING, TYPE_RUN_RECORD_STARTED)
    .put(ProgramRunStatus.SUSPENDED, TYPE_RUN_RECORD_SUSPENDED)
    .put(ProgramRunStatus.COMPLETED, TYPE_RUN_RECORD_COMPLETED)
    .put(ProgramRunStatus.KILLED, TYPE_RUN_RECORD_COMPLETED)
    .put(ProgramRunStatus.FAILED, TYPE_RUN_RECORD_COMPLETED)
    .build();

  private final CConfiguration cConf;
  private final AtomicBoolean upgradeComplete;

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
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId).build();

    List<WorkflowNodeStateDetail> nodeStateDetails = list(key, WorkflowNodeStateDetail.class);

    // Check again without the version to account for old data format since they might not have been updated yet
    // Since all the programs needs to be stopped before upgrade tool is run, either we will have node state details for
    // one specific run-id either in the old format or in the new format.
    if (!upgradeComplete.get() && nodeStateDetails.isEmpty() &&
      workflowRunId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      key = getVersionLessProgramKeyBuilder(TYPE_WORKFLOW_NODE_STATE, workflowRunId).build();
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
    key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, workflowRunId).build();

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
    MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTING, programRunId).build();
    long startTs = RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS);
    if (startTs == -1L) {
      LOG.error("Ignoring unexpected request to record provisioning state for program run {} that does not have " +
                  "a timestamp in the run id.");
      return null;
    }

    RunRecordMeta existing = getRun(programRunId);
    // for some reason, there is an existing run record.
    if (existing != null) {
      // TODO:  Don't expect this to happen, but update the start time,
      // runtime args, and system args of the existing run record.
      if (!isValid(existing, sourceId, "provisioning")) {
        return null;
      }
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
      .setSourceId(sourceId)
      .setArtifactId(artifactId)
      .setPrincipal(systemArgs.get(ProgramOptionConstants.PRINCIPAL))
      .build();
    write(key, meta);
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
    MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTING, programRunId).build();

    RunRecordMeta existing = getRun(programRunId);

    if (existing == null) {
      LOG.warn("Ignoring unexpected request to transition program run {} from non-existent state to cluster state {}.",
                programRunId, ProgramRunClusterStatus.PROVISIONED);
      return null;
    } else if (!isValid(existing, sourceId, "provisioned")) {
      return null;
    }

    ProgramRunClusterStatus clusterState = existing.getCluster().getStatus();
    if (clusterState != ProgramRunClusterStatus.PROVISIONING) {
      LOG.warn("Ignoring unexpected request to transition program run {} from cluster state {} to cluster state {}.",
               programRunId, existing.getCluster().getStatus(),
               ProgramRunClusterStatus.PROVISIONED);
      return null;
    }

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
    MDSKey.Builder key = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programRunId.getParent());

    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.debug("Ignoring unexpected transition of program run {} to cluster state {} with no existing run record.",
                programRunId, ProgramRunClusterStatus.DEPROVISIONING);
      return null;
    } else if (!isValid(existing, sourceId, "deprovisioning")) {
      return null;
    }

    ProgramRunClusterStatus clusterStatus = existing.getCluster().getStatus();
    // can come from provisioning if there was an error while provisioning,
    // such as some resources could be provisioned but other could not
    // for example, if 3 out of 5 nodes were provisioned, but the other 2 could not due to quota restrictions
    if (clusterStatus != ProgramRunClusterStatus.PROVISIONED && clusterStatus != ProgramRunClusterStatus.PROVISIONING) {
      LOG.warn("Ignoring unexpected request to transition program run {} from cluster state {} to cluster state {}.",
               programRunId, clusterStatus, ProgramRunClusterStatus.DEPROVISIONING);
      return null;
    }
    if (clusterStatus == ProgramRunClusterStatus.PROVISIONED && !existing.getStatus().isEndState()) {
      LOG.warn("Ignoring unexpected request to transition program run {} from program state {} to cluster state {}.",
               programRunId, existing.getStatus(), ProgramRunClusterStatus.DEPROVISIONING);
      return null;
    }

    delete(existing);
    key.add(getInvertedTsKeyPart(existing.getStartTs())).add(programRunId.getRun()).build();

    // if the previous state was provisioning, that means we've transitioned here from a failure
    ProgramRunStatus newStatus = clusterStatus == ProgramRunClusterStatus.PROVISIONING ?
      ProgramRunStatus.FAILED : existing.getStatus();
    ProgramRunCluster cluster = new ProgramRunCluster(ProgramRunClusterStatus.DEPROVISIONING,
                                                      null, existing.getCluster().getNumNodes());
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .setStatus(newStatus)
      .build();
    write(key.build(), meta);
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
   * @return {@link RunRecordMeta} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordMeta recordProgramDeprovisioned(ProgramRunId programRunId, byte[] sourceId) {
    MDSKey.Builder key = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programRunId.getParent());

    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.debug("Ignoring unexpected transition of program run {} to cluster state {} with no existing run record.",
                programRunId, ProgramRunClusterStatus.DEPROVISIONED);
      return null;
    } else if (!isValid(existing, sourceId, "deprovisioned")) {
      return null;
    }

    ProgramRunClusterStatus clusterStatus = existing.getCluster().getStatus();
    // existing cluster status must be provisioning or deprovisioning.
    // can come from provisioning if there was an error during the provisioning state,
    // before the provisioner could be called
    if (clusterStatus != ProgramRunClusterStatus.DEPROVISIONING &&
      clusterStatus != ProgramRunClusterStatus.PROVISIONING) {
      LOG.warn("Ignoring unexpected request to transition program run {} from cluster state {} to cluster state {}.",
               programRunId, clusterStatus, ProgramRunClusterStatus.DEPROVISIONED);
      return null;
    }
    if (clusterStatus == ProgramRunClusterStatus.DEPROVISIONING && !existing.getStatus().isEndState()) {
      LOG.warn("Ignoring unexpected request to transition program run {} from program state {} to cluster state {}.",
               programRunId, existing.getStatus(), ProgramRunClusterStatus.DEPROVISIONED);
      return null;
    }

    delete(existing);
    key.add(getInvertedTsKeyPart(existing.getStartTs())).add(programRunId.getRun()).build();

    // if the previous state was provisioning, that means we've transitioned here from a failure
    ProgramRunStatus newStatus = clusterStatus == ProgramRunClusterStatus.PROVISIONING ?
      ProgramRunStatus.FAILED : existing.getStatus();
    ProgramRunCluster cluster = new ProgramRunCluster(ProgramRunClusterStatus.DEPROVISIONED,
                                                      null, existing.getCluster().getNumNodes());
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .setStatus(newStatus)
      .build();
    write(key.build(), meta);
    LOG.trace("Recorded {} for program {}", ProgramRunClusterStatus.DEPROVISIONED, programRunId);
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
    MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTING, programRunId).build();
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
    if (!isValid(existing, sourceId, "start")) {
      return null;
    }
    if (existing.getStatus() != ProgramRunStatus.STARTING) {
      LOG.warn("Ignoring unexpected transition of program run {} from program state {} to program state {}.",
               programRunId, existing.getStatus(), ProgramRunStatus.STARTING);
      return null;
    }

    // Delete the old run record
    delete(existing);
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
  public RunRecordMeta recordProgramRunning(ProgramRunId programRunId, long stateChangeTime, String twillRunId,
                                            byte[] sourceId) {
    MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, programRunId).build();
    return recordProgramRunning(programRunId, stateChangeTime, twillRunId, key, sourceId);
  }

  /**
   * Logs start of program run and persists program status to {@link ProgramRunStatus#STARTING} with version-less key.
   * See {@link #recordProgramRunning(ProgramRunId, long, String, byte[])}
   */
  @VisibleForTesting
  void recordProgramRunningOldFormat(ProgramRunId programRunId, long stateChangeTime, String twillRunId,
                                     byte[] sourceId) {
    MDSKey key = getVersionLessProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, programRunId).build();
    recordProgramRunning(programRunId, stateChangeTime, twillRunId, key, sourceId);
  }

  @Nullable
  private RunRecordMeta recordProgramRunning(ProgramRunId programRunId, long runTs, String twillRunId,
                                             MDSKey key, byte[] sourceId) {
    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.warn("Ignoring unexpected transition of program run {} to program state {} with no existing run record.",
               programRunId, ProgramRunStatus.RUNNING);
      return null;
    }
    if (!isValid(existing, sourceId, "running")) {
      // Skip recording running if the existing records are not valid
      return null;
    }
    ProgramRunStatus status = existing.getStatus();
    if (status == null || status.isEndState()) {
      LOG.warn("Ignoring unexpected transition of program run {} from program state {} to program state {}.",
               programRunId, status, ProgramRunStatus.RUNNING);
      return null;
    }
    Map<String, String> systemArgs = existing.getSystemArgs();
    if (systemArgs != null && systemArgs.containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      // Program was started by Workflow. Add row corresponding to its node state.
      addWorkflowNodeState(programRunId, systemArgs, ProgramRunStatus.RUNNING, null, sourceId);
    }

    // Delete the old run record
    delete(existing);

    // The existing record's properties already contains the workflowRunId
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setRunTime(runTs)
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
  public RunRecordMeta recordProgramSuspend(ProgramRunId programRunId, byte[] sourceId) {
    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.warn("Ignoring unexpected transition of program run {} to program state {} with no existing run record.",
               programRunId, ProgramRunStatus.SUSPENDED);
      return null;
    }
    if (!isValid(existing, sourceId, "suspend")) {
      // Skip recording suspend if the existing record is not valid
      return null;
    }
    return recordProgramSuspendResume(programRunId, sourceId, existing, "suspend");
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
  public RunRecordMeta recordProgramResumed(ProgramRunId programRunId, byte[] sourceId) {
    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.warn("Ignoring unexpected transition of program run {} to program state {} with no existing run record.",
               programRunId, ProgramRunStatus.RUNNING);
      return null;
    }
    if (!isValid(existing, sourceId, "resume")) {
      // Skip recording resumed if the existing records are not valid
      return null;
    }
    // Only if existingRecords is empty & upgrade is not complete & the version is default version,
    // also try to get the record without the version string
    if (!upgradeComplete.get() && programRunId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      MDSKey key = getVersionLessProgramKeyBuilder(TYPE_RUN_RECORD_SUSPENDED, programRunId.getParent())
        .add(programRunId.getRun())
        .build();
      existing = get(key, RunRecordMeta.class);
    }
    if (existing == null) {
      LOG.error("No run record meta for program '{}' pid '{}' exists. Skip recording program suspend.",
                programRunId.getParent(), programRunId.getRun());
      return null;
    }
    return recordProgramSuspendResume(programRunId, sourceId, existing, "resume");
  }

  private RunRecordMeta recordProgramSuspendResume(ProgramRunId programRunId, byte[] sourceId,
                                                   RunRecordMeta existing, String action) {
    String toType = TYPE_RUN_RECORD_SUSPENDED;
    ProgramRunStatus toStatus = ProgramRunStatus.SUSPENDED;

    if (action.equals("resume")) {
      toType = TYPE_RUN_RECORD_STARTED;
      toStatus = ProgramRunStatus.RUNNING;
    }
    // Delete the old run record
    delete(existing);
    MDSKey key = getProgramKeyBuilder(toType, programRunId).build();
    RunRecordMeta meta = RunRecordMeta.builder(existing).setStatus(toStatus).setSourceId(sourceId).build();
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
    MDSKey.Builder builder = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programRunId.getParent());
    return recordProgramStop(programRunId, stopTs, runStatus, failureCause, builder, sourceId);
  }

  @VisibleForTesting
  void recordProgramStopOldFormat(ProgramRunId programRunId, long stopTs, ProgramRunStatus runStatus,
                                  @Nullable BasicThrowable failureCause, byte[] sourceId) {
    MDSKey.Builder builder = getVersionLessProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programRunId.getParent());
    recordProgramStop(programRunId, stopTs, runStatus, failureCause, builder, sourceId);
  }

  @Nullable
  private RunRecordMeta recordProgramStop(ProgramRunId programRunId, long stopTs, ProgramRunStatus runStatus,
                                          @Nullable BasicThrowable failureCause, MDSKey.Builder builder,
                                          byte[] sourceId) {
    RunRecordMeta existing = getRun(programRunId);
    if (existing == null) {
      LOG.warn("Ignoring unexpected transition of program run {} to program state {} with no existing run record.",
               programRunId, runStatus);
      return null;
    }
    if (!isValid(existing, sourceId, runStatus.name().toLowerCase())) {
      // Skip recording stop if the existing records are not valid
      return null;
    }
    if (existing.getStatus().isEndState()) {
      LOG.warn("Ignoring unexpected transition of program run {} from program state {} to program state {}.",
               programRunId, existing.getStatus(), runStatus);
      return null;
    }
    // Delete the old run record
    delete(existing);

    // Record in the workflow
    Map<String, String> systemArgs = existing.getSystemArgs();
    if (systemArgs != null && systemArgs.containsKey(ProgramOptionConstants.WORKFLOW_NAME)) {
      addWorkflowNodeState(programRunId, systemArgs, runStatus, failureCause, sourceId);
    }

    MDSKey key = builder.add(getInvertedTsKeyPart(existing.getStartTs())).add(programRunId.getRun()).build();
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
   * @param sourceId unique id representing the source of program run status, such as the message id of the program
   *                 run status notification in TMS. The source id must increase as the recording time of the program
   *                 run status increases, so that the attempt to persist program run status older than the existing
   *                 program run status will be ignored
   * @param recordType the type of record corresponding to the current status
   * @return {@code true} if the program run is allowed to persist the given status, {@code false} otherwise
   */
  private boolean isValid(RunRecordMeta existing, byte[] sourceId, String recordType) {
    byte[] existingSourceId = existing.getSourceId();
    if (existingSourceId != null && Bytes.compareTo(sourceId, existingSourceId) < 0) {
      LOG.debug("Current source id '{}' is not larger than the existing source id '{}' in the existing " +
                  "run record meta '{}'. Skip recording program {}.",
                Bytes.toHexString(sourceId), Bytes.toHexString(existingSourceId), existing, recordType);
      return false;
    }
    return true;
  }

  public Map<ProgramRunId, RunRecordMeta> getRuns(Set<ProgramRunId> programRunIds) {
    return getRuns(programRunIds, Integer.MAX_VALUE);
  }

  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(NamespaceId namespaceId) {
    // TODO CDAP-12361 should consolidate these methods and get rid of duplicate / unnecessary methods.
    Predicate<RunRecordMeta> timePredicate = getTimeRangePredicate(0, Long.MAX_VALUE);
    MDSKey key = getNamespaceKeyBuilder(TYPE_RUN_RECORD_STARTING, namespaceId).build();
    Map<ProgramRunId, RunRecordMeta> activeRuns = getProgramRunIdMap(listKV(key, null, RunRecordMeta.class,
                                                                            Integer.MAX_VALUE, timePredicate));

    key = getNamespaceKeyBuilder(TYPE_RUN_RECORD_STARTED, namespaceId).build();
    activeRuns.putAll(getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, Integer.MAX_VALUE, timePredicate)));
    key = getNamespaceKeyBuilder(TYPE_RUN_RECORD_SUSPENDED, namespaceId).build();
    activeRuns.putAll(getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, Integer.MAX_VALUE, timePredicate)));
    return activeRuns;
  }

  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(ApplicationId applicationId) {
    Predicate<RunRecordMeta> timePredicate = getTimeRangePredicate(0, Long.MAX_VALUE);
    MDSKey key = getApplicationKeyBuilder(TYPE_RUN_RECORD_STARTING, applicationId).build();
    Map<ProgramRunId, RunRecordMeta> activeRuns = getProgramRunIdMap(listKV(key, null, RunRecordMeta.class,
                                                                            Integer.MAX_VALUE, timePredicate));

    key = getApplicationKeyBuilder(TYPE_RUN_RECORD_STARTED, applicationId).build();
    activeRuns.putAll(getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, Integer.MAX_VALUE, timePredicate)));
    key = getApplicationKeyBuilder(TYPE_RUN_RECORD_SUSPENDED, applicationId).build();
    activeRuns.putAll(getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, Integer.MAX_VALUE, timePredicate)));
    return activeRuns;
  }

  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(ProgramId programId) {
    Predicate<RunRecordMeta> timePredicate = getTimeRangePredicate(0, Long.MAX_VALUE);
    MDSKey key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTING, programId).build();
    Map<ProgramRunId, RunRecordMeta> activeRuns = getProgramRunIdMap(listKV(key, null, RunRecordMeta.class,
                                                                            Integer.MAX_VALUE, timePredicate));

    key = getProgramKeyBuilder(TYPE_RUN_RECORD_STARTED, programId).build();
    activeRuns.putAll(getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, Integer.MAX_VALUE, timePredicate)));
    key = getProgramKeyBuilder(TYPE_RUN_RECORD_SUSPENDED, programId).build();
    activeRuns.putAll(getProgramRunIdMap(listKV(key, null, RunRecordMeta.class, Integer.MAX_VALUE, timePredicate)));
    return activeRuns;
  }

  private Map<ProgramRunId, RunRecordMeta> getRuns(Set<ProgramRunId> programRunIds, int limit) {
    Map<ProgramRunId, RunRecordMeta> resultMap = new LinkedHashMap<>();
    for (String type : Arrays.asList(TYPE_RUN_RECORD_STARTING, TYPE_RUN_RECORD_STARTED,
                                     TYPE_RUN_RECORD_SUSPENDED, TYPE_RUN_RECORD_COMPLETED)) {
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
        for (String type : Arrays.asList(TYPE_RUN_RECORD_STARTING,
                                         TYPE_RUN_RECORD_STARTED, TYPE_RUN_RECORD_SUSPENDED)) {
          runRecords.putAll(getNonCompleteRuns(programId, type, startTime, endTime, limit - runRecords.size(), filter));
        }
        runRecords.putAll(getHistoricalRuns(programId, status, startTime, endTime, limit - runRecords.size(), filter));
        return runRecords;
      case PENDING:
      case STARTING:
        Predicate<RunRecordMeta> stateFilter = record -> record.getStatus() == status;
        if (filter != null) {
          stateFilter = stateFilter.and(filter);
        }
        return getNonCompleteRuns(programId, TYPE_RUN_RECORD_STARTING, startTime, endTime, limit, stateFilter);
      case RUNNING:
        return getNonCompleteRuns(programId, TYPE_RUN_RECORD_STARTED, startTime, endTime, limit, filter);
      case SUSPENDED:
        return getNonCompleteRuns(programId, TYPE_RUN_RECORD_SUSPENDED, startTime, endTime, limit, filter);
      default:
        return getHistoricalRuns(programId, status, startTime, endTime, limit, filter);
    }
  }

  // TODO: getRun is duplicated in cdap-watchdog AppMetadataStore class.
  // Any changes made here will have to be made over there too.
  // JIRA https://issues.cask.co/browse/CDAP-2172
  public RunRecordMeta getRun(ProgramRunId programRun) {
    // Query active run record first
    RunRecordMeta running = getUnfinishedRun(programRun, TYPE_RUN_RECORD_STARTED);
    // If program is running, this will be non-null
    if (running != null) {
      return running;
    }

    // Then query for starting run record
    RunRecordMeta starting = getUnfinishedRun(programRun, TYPE_RUN_RECORD_STARTING);
    if (starting != null) {
      return starting;
    }

    // If program is not running, query completed run records
    RunRecordMeta complete = getCompletedRun(programRun);
    if (complete != null) {
      return complete;
    }

    // Else query suspended run records
    return getUnfinishedRun(programRun, TYPE_RUN_RECORD_SUSPENDED);
  }

  private void delete(RunRecordMeta record) {
    ProgramRunId programRunId = record.getProgramRunId();
    MDSKey.Builder key = getProgramKeyBuilder(STATUS_TYPE_MAP.get(record.getStatus()), programRunId.getParent());
    if (record.getStatus().isEndState()) {
      key.add(getInvertedTsKeyPart(record.getStartTs()));
    }
    key.add(programRunId.getRun());
    deleteAll(key.build());
  }

  /**
   * @return run records for runs that do not have start time in mds key for the run record.
   */
  private RunRecordMeta getUnfinishedRun(ProgramRunId programRunId, String recordType) {
    MDSKey runningKey = getProgramKeyBuilder(recordType, programRunId.getParent())
      .add(programRunId.getRun())
      .build();

    RunRecordMeta runRecordMeta = get(runningKey, RunRecordMeta.class);

    if (!upgradeComplete.get() && runRecordMeta == null &&
      programRunId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      runningKey = getVersionLessProgramKeyBuilder(recordType, programRunId.getParent())
        .add(programRunId.getRun()).build();
      return get(runningKey, RunRecordMeta.class);
    }

    return runRecordMeta;
  }

  private RunRecordMeta getCompletedRun(ProgramRunId programRunId) {
    MDSKey completedKey = getProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programRunId.getParent()).build();
    RunRecordMeta runRecordMeta = getCompletedRun(completedKey, programRunId.getRun());

    if (!upgradeComplete.get() && runRecordMeta == null &&
      programRunId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
      completedKey = getVersionLessProgramKeyBuilder(TYPE_RUN_RECORD_COMPLETED, programRunId.getParent()).build();
      return getCompletedRun(completedKey, programRunId.getRun());
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

  private Map<ProgramRunId, RunRecordMeta> getRunsForRunIds(final Set<ProgramRunId> runIds, String recordType,
                                                            int limit) {
    Set<MDSKey> keySet = new HashSet<>();
    boolean includeVersionLessKeys = !upgradeComplete.get();
    for (ProgramRunId programRunId : runIds) {
      keySet.add(getProgramKeyBuilder(recordType, programRunId.getParent()).build());
      if (includeVersionLessKeys && programRunId.getVersion().equals(ApplicationId.DEFAULT_VERSION)) {
        keySet.add(getVersionLessProgramKeyBuilder(recordType, programRunId.getParent()).build());
      }
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
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_TOKEN, workflowRunId).build();

    write(key, workflowToken);
  }

  public WorkflowToken getWorkflowToken(ProgramId workflowId, String workflowRunId) {
    Preconditions.checkArgument(ProgramType.WORKFLOW == workflowId.getType());
    // Workflow token is stored with following key:
    // [wft][namespace][app][version][WORKFLOW][workflowName][workflowRun]
    MDSKey key = getProgramKeyBuilder(TYPE_WORKFLOW_TOKEN, workflowId.run(workflowRunId)).build();

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
    LOG.info("Checking upgrade for {}", recordType);
    MDSKey startKey = new MDSKey.Builder().add(recordType).build();
    Map<MDSKey, T> oldMap = listKV(startKey, typeOfT);
    Map<MDSKey, T> newMap = new HashMap<>();
    Set<MDSKey> deleteKeys = new HashSet<>();

    for (Map.Entry<MDSKey, T> oldEntry : oldMap.entrySet()) {
      MDSKey oldKey = oldEntry.getKey();
      MDSKey newKey = appendDefaultVersion(recordType, oldKey);
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

    LOG.info("Upgrading {} entries, deleting {} entries of {}", newMap.size(), deleteKeys.size(), recordType);
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
  private static MDSKey appendDefaultVersion(String recordType, MDSKey originalKey) {
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

/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.proto.BasicThrowable;
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
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import co.cask.cdap.spi.data.table.field.Range;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
public class AppMetadataStore {

  public static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);

  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStore.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type BYTE_TYPE = new TypeToken<byte[]>() { }.getType();
  private static final String APP_VERSION_UPGRADE_KEY = "version.default.store";
  private static final String RUN_COUNT_FIRST_UPGRADE_TIME = "run.count.first.upgrade.time";
  // this row key will be used to record the progress of upgrade run count since for completed run records we do not
  // modify the row key, so we do not know which row has been counted.
  private static final String RUN_COUNT_PROGRESS = "run.count.progress";

  private static final String TYPE_APP_META = "appMeta";

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
  private static final String TYPE_RUN_RECORD_UPGRADE_COUNT = "runRecordUpgradeCount";

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
  private final StructuredTable applicationSpecificationTable;
  private final StructuredTable workflowNodeStateTable;
  private final StructuredTable runRecordsTable;
  private final StructuredTable workflowsTable;
  private final StructuredTable programCountsTable;
  private final StructuredTable upgradeMetadataTable;
  private final StructuredTable subscriberStateTable;

  /**
   * Static method for creating an instance of {@link AppMetadataStore}.
   */
  public static AppMetadataStore create(CConfiguration cConf, StructuredTableContext context,
                                        DatasetContext datasetContext,
                                        DatasetFramework datasetFramework) {
    try {
      return new AppMetadataStore(context, cConf);
    } catch (DatasetManagementException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public AppMetadataStore(StructuredTableContext context, CConfiguration cConf) throws NotFoundException {
    this.cConf = cConf;
    this.applicationSpecificationTable =
      context.getTable(StoreDefinition.AppMetadataStore.APPLICATION_SPECIFICATIONS);
    this.workflowNodeStateTable =
      context.getTable(StoreDefinition.AppMetadataStore.WORKFLOW_NODE_STATES);
    this.runRecordsTable =
      context.getTable(StoreDefinition.AppMetadataStore.RUN_RECORDS);
    this.workflowsTable =
      context.getTable(StoreDefinition.AppMetadataStore.WORKFLOWS);
    this.programCountsTable =
      context.getTable(StoreDefinition.AppMetadataStore.PROGRAM_COUNTS);
    this.upgradeMetadataTable =
      context.getTable(StoreDefinition.AppMetadataStore.UPGRADE_METADATA);
    this.subscriberStateTable =
      context.getTable(StoreDefinition.AppMetadataStore.SUBSCRIBER_STATE);
  }

  /*@Override
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
  }*/

  @Nullable
  public ApplicationMeta getApplication(ApplicationId appId) throws IOException {
    return getApplication(appId.getNamespace(), appId.getApplication(), appId.getVersion());
  }

  @Nullable
  public ApplicationMeta getApplication(String namespaceId, String appId, String versionId) throws IOException {
    List<Field<?>> fields = getApplicationPrimaryKeys(namespaceId, appId, versionId);
    Optional<StructuredRow> row = applicationSpecificationTable.read(fields);
    if (!row.isPresent()) {
      return null;
    }
    return GSON.fromJson(
      row.get().getString(StoreDefinition.AppMetadataStore.APPLICATION_DATA_FIELD), ApplicationMeta.class);
  }

  public List<ApplicationMeta> getAllApplications(String namespaceId) throws IOException {
    return
      scanWithRange(
        getNamespaceRange(namespaceId),
        ApplicationMeta.class,
        applicationSpecificationTable,
        StoreDefinition.AppMetadataStore.APPLICATION_DATA_FIELD);
  }

  public List<ApplicationMeta> getAllAppVersions(String namespaceId, String appId) throws IOException {
    return
      scanWithRange(
        getNamespaceAndApplicationRange(namespaceId, appId),
        ApplicationMeta.class,
        applicationSpecificationTable,
        StoreDefinition.AppMetadataStore.APPLICATION_DATA_FIELD);
  }

  public List<ApplicationId> getAllAppVersionsAppIds(String namespaceId, String appId) throws IOException {
    List<ApplicationId> appIds = new ArrayList<>();
    Iterator<StructuredRow> iterator =
      applicationSpecificationTable.scan(getNamespaceAndApplicationRange(namespaceId, appId), Integer.MAX_VALUE);
   while(iterator.hasNext()) {
     StructuredRow row = iterator.next();
     appIds.add(
       new NamespaceId(row.getString(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD))
         .app(row.getString(StoreDefinition.AppMetadataStore.APPLICATION_FIELD),
              row.getString(StoreDefinition.AppMetadataStore.VERSION_FIELD)));
   }
   return appIds;
  }

  public Map<ApplicationId, ApplicationMeta> getApplicationsForAppIds(Collection<ApplicationId> appIds)
    throws IOException {
    Map<ApplicationId, ApplicationMeta> result = new HashMap<>();
    for (ApplicationId appId: appIds) {
      result.put(appId, getApplication(appId));
    }
    return result;
  }

  public void writeApplication(String namespaceId, String appId, String versionId, ApplicationSpecification spec)
    throws IOException {
    writeApplicationSerialized(namespaceId, appId, versionId, GSON.toJson(new ApplicationMeta(appId, spec)));
  }

  public void deleteApplication(String namespaceId, String appId, String versionId) throws IOException {
    List<Field<?>> fields = getApplicationPrimaryKeys(namespaceId, appId, versionId);
    applicationSpecificationTable.delete(fields);
  }

  public void deleteApplications(String namespaceId) throws IOException {
    applicationSpecificationTable.deleteAll(getNamespaceRange(namespaceId));
  }

  // todo: do we need appId? may be use from appSpec?
  public void updateAppSpec(String namespaceId, String appId, String versionId, ApplicationSpecification spec)
  throws IOException {
    LOG.trace("App spec to be updated: id: {}: spec: {}", appId, GSON.toJson(spec));
    ApplicationMeta existing = getApplication(namespaceId, appId, versionId);
    ApplicationMeta updated;

    if (existing == null) {
      String msg = String.format("No meta for namespace %s app %s exists", namespaceId, appId);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    updated = ApplicationMeta.updateSpec(existing, spec);
    LOG.trace("Application exists in mds: id: {}, spec: {}", existing);
    writeApplicationSerialized(namespaceId, appId, versionId, GSON.toJson(updated));
  }

  /**
   * Return the {@link List} of {@link WorkflowNodeStateDetail} for a given Workflow run.
   */
  public List<WorkflowNodeStateDetail> getWorkflowNodeStates(ProgramRunId workflowRunId) throws IOException {
    return
      scanWithRange(
        Range.singleton(getWorkflowPrimaryKeysWithoutNode(workflowRunId)),
        WorkflowNodeStateDetail.class,
        workflowNodeStateTable,
        StoreDefinition.AppMetadataStore.NODE_STATE_DATA);
  }

  /**
   * This method is called to associate node state of custom action with the Workflow run.
   *
   * @param workflowRunId the run for which node state is to be added
   * @param nodeStateDetail node state details to be added
   */
  public void addWorkflowNodeState(ProgramRunId workflowRunId, WorkflowNodeStateDetail nodeStateDetail)
    throws IOException {
    // Node states will be stored with following key:
    // workflowNodeState.namespace.app.WORKFLOW.workflowName.workflowRun.workflowNodeId
    List<Field<?>> fields = getWorkflowPrimaryKeys(workflowRunId, nodeStateDetail.getNodeId());
    writeToStructuredTableWithPrimaryKeys(
      fields, nodeStateDetail, workflowNodeStateTable, StoreDefinition.AppMetadataStore.NODE_STATE_DATA);
  }

  private void addWorkflowNodeState(ProgramRunId programRunId, Map<String, String> systemArgs,
                                    ProgramRunStatus status, @Nullable BasicThrowable failureCause, byte[] sourceId)
    throws IOException {
    String workflowNodeId = systemArgs.get(ProgramOptionConstants.WORKFLOW_NODE_ID);
    String workflowName = systemArgs.get(ProgramOptionConstants.WORKFLOW_NAME);
    String workflowRun = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);

    ApplicationId appId = programRunId.getParent().getParent();
    ProgramRunId workflowRunId = appId.workflow(workflowName).run(workflowRun);

    WorkflowNodeStateDetail nodeStateDetail = new WorkflowNodeStateDetail(workflowNodeId,
                                                                          ProgramRunStatus.toNodeStatus(status),
                                                                          programRunId.getRun(), failureCause);
    // Node states will be stored with following key:
    // workflowNodeState.namespace.app.WORKFLOW.workflowName.workflowRun.workflowNodeId
    List<Field<?>> fields = getWorkflowPrimaryKeys(workflowRunId, nodeStateDetail.getNodeId());
    writeToStructuredTableWithPrimaryKeys(
      fields, nodeStateDetail, workflowNodeStateTable, StoreDefinition.AppMetadataStore.NODE_STATE_DATA);

    // Get the run record of the Workflow which started this program
    List<Field<?>> runRecordFields = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, workflowRunId,
                                                                  RunIds.getTime(workflowRun, TimeUnit.SECONDS));

    Optional<StructuredRow> row = runRecordsTable.read(runRecordFields);
    if (row.isPresent()) {
      RunRecordMeta record = GSON.fromJson(row.get().getString(StoreDefinition.AppMetadataStore.RUN_RECORD_DATA),
                                           RunRecordMeta.class);
      // Update the parent Workflow run record by adding node id and program run id in the properties
      Map<String, String> properties = new HashMap<>(record.getProperties());
      properties.put(workflowNodeId, programRunId.getRun());
      writeToStructuredTableWithPrimaryKeys(
        runRecordFields, RunRecordMeta.builder(record).setProperties(properties).setSourceId(sourceId).build(),
        runRecordsTable, StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
                                                 @Nullable ArtifactId artifactId) throws IOException {
    long startTs = RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS);
    List<Field<?>> fields = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, startTs);
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
    writeToStructuredTableWithPrimaryKeys(
      fields, meta, runRecordsTable, StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
    List<Field<?>> countKey = getProgramCountPrimaryKeys(TYPE_COUNT, programRunId.getParent());
    programCountsTable.increment(countKey, StoreDefinition.AppMetadataStore.COUNTS, 1L);
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
  public RunRecordMeta recordProgramProvisioned(ProgramRunId programRunId, int numNodes, byte[] sourceId)
    throws IOException {
    RunRecordMeta existing = getRun(programRunId);

    if (existing == null) {
      LOG.warn("Ignoring unexpected request to transition program run {} from non-existent state to cluster state {}.",
                programRunId, ProgramRunClusterStatus.PROVISIONED);
      return null;
    }
    if (!isValid(existing, existing.getStatus(), ProgramRunClusterStatus.PROVISIONED, sourceId)) {
      return null;
    }

    List<Field<?>> key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, existing.getStartTs());
    ProgramRunCluster cluster = new ProgramRunCluster(ProgramRunClusterStatus.PROVISIONED, null, numNodes);
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, runRecordsTable, StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
  public RunRecordMeta recordProgramDeprovisioning(ProgramRunId programRunId, byte[] sourceId) throws IOException {

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

    List<Field<?>> key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_COMPLETED, programRunId, existing.getStartTs());

    ProgramRunCluster cluster = new ProgramRunCluster(ProgramRunClusterStatus.DEPROVISIONING, null,
                                                      existing.getCluster().getNumNodes());
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, runRecordsTable, StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
  public RunRecordMeta recordProgramDeprovisioned(ProgramRunId programRunId, @Nullable Long endTs, byte[] sourceId)
    throws IOException {
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
    List<Field<?>> key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_COMPLETED, programRunId, existing.getStartTs());

    ProgramRunCluster cluster = new ProgramRunCluster(ProgramRunClusterStatus.DEPROVISIONED, endTs,
                                                      existing.getCluster().getNumNodes());
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, runRecordsTable, StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
  public RunRecordMeta recordProgramOrphaned(ProgramRunId programRunId, long endTs, byte[] sourceId)
    throws IOException {
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
    List<Field<?>> key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_COMPLETED, programRunId, existing.getStartTs());

    ProgramRunCluster cluster = new ProgramRunCluster(ProgramRunClusterStatus.ORPHANED, endTs,
                                                      existing.getCluster().getNumNodes());
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, runRecordsTable, StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
                                          Map<String, String> systemArgs, byte[] sourceId) throws IOException {
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
    List<Field<?>> key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, existing.getStartTs());
    meta = RunRecordMeta.builder(existing)
      .setStatus(ProgramRunStatus.STARTING)
      .setTwillRunId(twillRunId)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, runRecordsTable, StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
                                            byte[] sourceId) throws IOException {
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
    List<Field<?>> key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, existing.getStartTs());

    // The existing record's properties already contains the workflowRunId
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setRunTime(stateChangeTime)
      .setStatus(ProgramRunStatus.RUNNING)
      .setTwillRunId(twillRunId)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, runRecordsTable, StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
  public RunRecordMeta recordProgramSuspend(ProgramRunId programRunId, byte[] sourceId, long timestamp)
    throws IOException {
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
  public RunRecordMeta recordProgramResumed(ProgramRunId programRunId, byte[] sourceId, long timestamp)
    throws IOException {
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
                                                   RunRecordMeta existing, String action, long timestamp)
    throws IOException {
    ProgramRunStatus toStatus = ProgramRunStatus.SUSPENDED;

    if (action.equals("resume")) {
      toStatus = ProgramRunStatus.RUNNING;
    }
    // Delete the old run record
    delete(existing);
    List<Field<?>> key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, existing.getStartTs());
    RunRecordMeta.Builder builder = RunRecordMeta.builder(existing).setStatus(toStatus).setSourceId(sourceId);
    if (timestamp != -1) {
      if (action.equals("resume")) {
        builder.setResumeTime(timestamp);
      } else {
        builder.setSuspendTime(timestamp);
      }
    }
    RunRecordMeta meta = builder.build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, runRecordsTable, StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
                                         @Nullable BasicThrowable failureCause, byte[] sourceId) throws IOException {
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

    List<Field<?>> key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_COMPLETED, programRunId, existing.getStartTs());
    RunRecordMeta meta = RunRecordMeta.builder(existing)
      .setStopTime(stopTs)
      .setStatus(runStatus)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, runRecordsTable, StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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

  public Map<ProgramRunId, RunRecordMeta> getRuns(Set<ProgramRunId> programRunIds) throws IOException {
    Map<ProgramRunId, RunRecordMeta> result = new HashMap();
    for (ProgramRunId runId : programRunIds) {
      result.put(runId, getRun(runId));
    }
    return result;
  }

  /**
   * Get active runs in the given set of namespaces that satisfies a filter, active runs means program run with status
   * STARTING, PENDING, RUNNING or SUSPENDED.
   *
   * @param namespaces set of namespaces
   * @param filter filter to filter run record
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(Set<NamespaceId> namespaces, Predicate<RunRecordMeta> filter)
    throws IOException {
    Map<ProgramRunId, RunRecordMeta> result = new HashMap<>();
    for (NamespaceId namespaceId : namespaces) {
      List<Field<?>> prefix = getRunRecordNamespacePrefix(TYPE_RUN_RECORD_ACTIVE, namespaceId);
      result.putAll(getProgramRunIdMap(Range.singleton(prefix), filter));
    }
    return result;
  }

  /**
   * Get active runs in all namespaces with a filter, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param filter filter to filter run record
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(Predicate<RunRecordMeta> filter) throws IOException {
    List<Field<?>> prefix = getRunRecordNamespacePrefix(TYPE_RUN_RECORD_ACTIVE, null);
    return getProgramRunIdMap(Range.singleton(prefix), filter);
  }

  /**
   * Get active runs in the given namespace, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param namespaceId given namespace
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(NamespaceId namespaceId) throws IOException {
    // TODO CDAP-12361 should consolidate these methods and get rid of duplicate / unnecessary methods.
    List<Field<?>> prefix = getRunRecordNamespacePrefix(TYPE_RUN_RECORD_ACTIVE, namespaceId);
    Predicate<RunRecordMeta> timePredicate = getTimeRangePredicate(0, Long.MAX_VALUE);
    return getProgramRunIdMap(Range.singleton(prefix), timePredicate);
  }

  /**
   * Get active runs in the given application, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param applicationId given app
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(ApplicationId applicationId) throws IOException {
    Predicate<RunRecordMeta> timePredicate = getTimeRangePredicate(0, Long.MAX_VALUE);
    List<Field<?>> prefix = getRunRecordApplicationPrefix(TYPE_RUN_RECORD_ACTIVE, applicationId);
    return getProgramRunIdMap(Range.singleton(prefix), timePredicate);
  }

  /**
   * Get active runs in the given program, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param programId given program
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordMeta> getActiveRuns(ProgramId programId) throws IOException {
    Predicate<RunRecordMeta> timePredicate = getTimeRangePredicate(0, Long.MAX_VALUE);
    List<Field<?>> prefix = getRunRecordProgramPrefix(TYPE_RUN_RECORD_ACTIVE, programId);
    return getProgramRunIdMap(Range.singleton(prefix), timePredicate);
  }

  public Map<ProgramRunId, RunRecordMeta> getRuns(@Nullable ProgramId programId, final ProgramRunStatus status,
                                                  long startTime, long endTime, int limit,
                                                  @Nullable Predicate<RunRecordMeta> filter) throws IOException {
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
  public RunRecordMeta getRun(ProgramRunId programRun) throws IOException {
    // Query active run record first
    RunRecordMeta running = getUnfinishedRun(programRun);
    // If program is running, this will be non-null
    if (running != null) {
      return running;
    }
    // If program is not running, query completed run records
    return getCompletedRun(programRun);
  }

  private void delete(RunRecordMeta record) throws IOException {
    ProgramRunId programRunId = record.getProgramRunId();
    List<Field<?>> key = getProgramRunInvertedTimeKey(STATUS_TYPE_MAP.get(record.getStatus()), programRunId,
                                              record.getStartTs());
    runRecordsTable.delete(key);
  }

  /**
   * @return run records for unfinished run
   */
  private RunRecordMeta getUnfinishedRun(ProgramRunId programRunId) throws IOException {
    List<Field<?>> runningKey = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId,
                                                     RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS));
    return getRunRecordMeta(runningKey);
  }

  private RunRecordMeta getCompletedRun(ProgramRunId programRunId) throws IOException {
    List<Field<?>> completedKey = getRunRecordProgramPrefix(TYPE_RUN_RECORD_COMPLETED, programRunId.getParent());
    return getCompletedRun(completedKey, programRunId.getRun());
  }

  private RunRecordMeta getCompletedRun(List<Field<?>> prefix, final String runId) throws IOException {
    // Get start time from RunId
    long programStartSecs = RunIds.getTime(RunIds.fromString(runId), TimeUnit.SECONDS);
    prefix.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_ID, runId));
    if (programStartSecs > -1) {
      // If start time is found, run a get
      prefix.add(
        Fields.longField(StoreDefinition.AppMetadataStore.RUN_START_TIME, getInvertedTsKeyPart(programStartSecs)));
      return getRunRecordMeta(prefix);
    } else {
      // If start time is not found, scan the table (backwards compatibility when run ids were random UUIDs)
      List<Field<?>> startRange = new ArrayList<>(prefix);
      startRange.add(
        Fields.longField(StoreDefinition.AppMetadataStore.RUN_START_TIME, getInvertedTsScanKeyPart(Long.MAX_VALUE)));
      List<Field<?>> endRange = new ArrayList<>(prefix);
      startRange.add(
        Fields.longField(StoreDefinition.AppMetadataStore.RUN_START_TIME, getInvertedTsScanKeyPart(0)));
      List<RunRecordMeta> metas =
        scanWithRange(
          Range.create(startRange, Range.Bound.INCLUSIVE, endRange, Range.Bound.INCLUSIVE),
          RunRecordMeta.class, runRecordsTable, StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
      return metas.size() > 0 ? metas.get(0) : null;
    }
  }

  private Map<ProgramRunId, RunRecordMeta> getNonCompleteRuns(@Nullable ProgramId programId, String recordType,
                                                              final long startTime, final long endTime, int limit,
                                                              Predicate<RunRecordMeta> filter) throws IOException {
    Predicate<RunRecordMeta> valuePredicate = andPredicate(getTimeRangePredicate(startTime, endTime), filter);
    List<Field<?>> prefix = getRunRecordProgramPrefix(recordType, programId);
    return getProgramRunIdMap(Range.singleton(prefix), valuePredicate, null, limit);
  }

  /**
   * Converts MDSkeys in the map to ProgramIDs
   *
   * @param range to scan runRecordsTable with
   * @param predicate to filter the runRecordMetas by. If null, then does not filter.
   * @param limit the maximum number of entries to return
   * @return map with keys as program IDs
   */
  private Map<ProgramRunId, RunRecordMeta> getProgramRunIdMap(
    Range range, @Nullable Predicate<RunRecordMeta> predicate, @Nullable Predicate<StructuredRow> keyPredicate,
    int limit) throws IOException {
    Map<ProgramRunId, RunRecordMeta> programRunIdMap = new LinkedHashMap<>();
    // Only pass in limit if predicates are null, or else we may return fewer than limit items
    Iterator<StructuredRow> iterator =
      runRecordsTable.scan(range, predicate == null && keyPredicate == null ? limit : Integer.MAX_VALUE);
    while (iterator.hasNext() && limit > 0) {
      StructuredRow row = iterator.next();
      ProgramId programId =
        new ApplicationId(row.getString(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD),
                          row.getString(StoreDefinition.AppMetadataStore.APPLICATION_FIELD),
                          row.getString(StoreDefinition.AppMetadataStore.VERSION_FIELD))
          .program(ProgramType.valueOf(row.getString(StoreDefinition.AppMetadataStore.PROGRAM_TYPE_FIELD)),
                   row.getString(StoreDefinition.AppMetadataStore.PROGRAM_FIELD));
      RunRecordMeta meta =
        GSON.fromJson(row.getString(StoreDefinition.AppMetadataStore.RUN_RECORD_DATA), RunRecordMeta.class);
      if ((predicate == null || predicate.test(meta)) && (keyPredicate == null || keyPredicate.test(row))) {
        programRunIdMap.put(programId.run(meta.getPid()), meta);
        limit--;
      }
    }
    return programRunIdMap;
  }

  private Map<ProgramRunId, RunRecordMeta> getProgramRunIdMap(
    Range range, @Nullable  Predicate<RunRecordMeta> predicate) throws IOException {
    return getProgramRunIdMap(range, predicate, null, Integer.MAX_VALUE);
  }

  private Map<ProgramRunId, RunRecordMeta> getHistoricalRuns(@Nullable ProgramId programId, ProgramRunStatus status,
                                                             final long startTime, final long endTime, int limit,
                                                             @Nullable Predicate<RunRecordMeta> filter)
    throws IOException {
    List<Field<?>> prefix = getRunRecordProgramPrefix(TYPE_RUN_RECORD_COMPLETED, programId);
    return getHistoricalRuns(prefix, status, startTime, endTime, limit, filter);
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
                                                            final int limit) throws IOException {
    MDSKey keyPrefix = new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED).build();
    Map<ProgramRunId, RunRecordMeta> result = new HashMap<>();
    for (NamespaceId namespaceId : namespaces) {
      // get active runs in a time window with range [earliestStopTime, latestStartTime),
      // which excludes program run records that stopped before earliestStopTime and
      // program run records that started after latestStartTime, all remaining records are active
      // at some point within the time window and will be returned
      result.putAll(getProgramRunIdMap(Range.singleton(
        getRunRecordNamespacePrefix(TYPE_RUN_RECORD_COMPLETED, namespaceId)),
                                       meta -> meta.getStopTs() != null && meta.getStopTs() >= earliestStopTime
                                         && meta.getStartTs() < latestStartTime, null, limit));
    }
    return result;
  }

  private Map<ProgramRunId, RunRecordMeta> getHistoricalRuns(List<Field<?>> historyKey, ProgramRunStatus status,
                                                             final long startTime, final long endTime, int limit,
                                                             @Nullable Predicate<RunRecordMeta> valueFilter)
    throws IOException {
    long lowerBound = getInvertedTsScanKeyPart(endTime);
    long upperBound = getInvertedTsScanKeyPart(startTime);
    Predicate<StructuredRow> keyFiter = row -> {
      long time = row.getLong(StoreDefinition.AppMetadataStore.RUN_START_TIME);
      return time >= lowerBound && time <= upperBound;

    };
    if (status.equals(ProgramRunStatus.ALL)) {
      //return all records (successful and failed)
      return getProgramRunIdMap(Range.singleton(historyKey), valueFilter, keyFiter, limit);
    }

    if (status.equals(ProgramRunStatus.COMPLETED)) {
      return getProgramRunIdMap(Range.singleton(historyKey),
                                andPredicate(getPredicate(ProgramController.State.COMPLETED), valueFilter),
                                keyFiter, limit);
    }
    if (status.equals(ProgramRunStatus.KILLED)) {
      return getProgramRunIdMap(Range.singleton(historyKey),
                                andPredicate(getPredicate(ProgramController.State.KILLED), valueFilter),
                                keyFiter, limit);
    }
    return getProgramRunIdMap(Range.singleton(historyKey),
                              andPredicate(getPredicate(ProgramController.State.ERROR), valueFilter),
                              keyFiter, limit);
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

  public void deleteProgramHistory(String namespaceId, String appId, String versionId) {
    ApplicationId applicationId = new ApplicationId(namespaceId, appId, versionId);
    runRecordsTable.deleteAll(Range.singleton(getRunRecordApplicationPrefix(TYPE_RUN_RECORD_ACTIVE, applicationId)));
    runRecordsTable.deleteAll(Range.singleton(getRunRecordApplicationPrefix(TYPE_RUN_RECORD_COMPLETED, applicationId)));
    runRecordsTable.deleteAll(Range.singleton(getCountApplicationPrefix(TYPE_COUNT, applicationId)));
    runRecordsTable.deleteAll(Range.singleton(getCountApplicationPrefix(TYPE_RUN_RECORD_UPGRADE_COUNT, applicationId)));
  }

  public void deleteProgramHistory(String namespaceId) {
    runRecordsTable.deleteAll(Range.singleton(getRunRecordNamespacePrefixWithString(TYPE_RUN_RECORD_ACTIVE, namespaceId)));
    runRecordsTable.deleteAll(Range.singleton(
      getRunRecordNamespacePrefixWithString(TYPE_RUN_RECORD_COMPLETED, namespaceId)));
    runRecordsTable.deleteAll(Range.singleton(getCountNamespacePrefix(TYPE_COUNT, namespaceId)));
    runRecordsTable.deleteAll(Range.singleton(
      getCountNamespacePrefix(TYPE_RUN_RECORD_UPGRADE_COUNT, namespaceId)));
  }

  /**
   * Sets the {@link WorkflowToken} for the given workflow run.
   *
   * @param workflowRunId the {@link ProgramRunId} representing the workflow run
   * @param workflowToken the {@link WorkflowToken} to set to
   */
  public void setWorkflowToken(ProgramRunId workflowRunId, WorkflowToken workflowToken) throws IOException {
    if (workflowRunId.getType() != ProgramType.WORKFLOW) {
      throw new IllegalArgumentException("WorkflowToken can only be set for workflow execution: " + workflowRunId);
    }

    List<Field<?>> keys = getWorkflowPrimaryKeysWithoutNode(workflowRunId);
    keys.add(Fields.stringField(StoreDefinition.AppMetadataStore.WORKFLOW_DATA, GSON.toJson(workflowToken)));
    workflowsTable.upsert(keys);
  }

  public WorkflowToken getWorkflowToken(ProgramId workflowId, String workflowRunId) throws IOException {
    Preconditions.checkArgument(ProgramType.WORKFLOW == workflowId.getType());
    List<Field<?>> keys = getWorkflowPrimaryKeysWithoutNode(workflowId.run(workflowRunId));
    Optional<StructuredRow> row = workflowsTable.read(keys);

    if (!row.isPresent()) {
      LOG.debug("No workflow token available for workflow: {}, runId: {}", workflowId, workflowRunId);
      // Its ok to not allow any updates by returning a 0 size token.
      return new BasicWorkflowToken(0);
    }

    return GSON.fromJson(row.get().getString(StoreDefinition.AppMetadataStore.WORKFLOW_DATA), WorkflowToken.class);
  }

  /**
   * @return programs that were running between given start and end time and are completed
   */
  public Set<RunId> getRunningInRangeCompleted(long startTimeInSecs, long endTimeInSecs) throws IOException {
    // This method scans a large amount of data and may timeout. However, the previous implementation would
    // simply return incomplete data. We have doubled the amount of time each transaction can take by using two
    // transactions - and can further get all namespaces from the smaller app spec table and do one transaction per
    // namespace if necessary.
    return getRunningInRangeForStatus(TYPE_RUN_RECORD_COMPLETED, startTimeInSecs, endTimeInSecs);
  }

  /**
   * @return programs that were running between given start and end time and are active
   */
  public Set<RunId> getRunningInRangeActive(long startTimeInSecs, long endTimeInSecs) throws IOException {
    // This method scans a large amount of data and may timeout. However, the previous implementation would
    // simply return incomplete data. We have doubled the amount of time each transaction can take by using two
    // transactions - and can further get all namespaces from the smaller app spec table and do one transaction per
    // namespace if necessary.
    return getRunningInRangeForStatus(TYPE_RUN_RECORD_ACTIVE, startTimeInSecs, endTimeInSecs);
  }

  /**
   * Get the run count of the given program.
   *
   * @param programId the program to get the count
   * @return the number of run count
   */
  public long getProgramRunCount(ProgramId programId) throws IOException {
    List<Field<?>> countKey = getProgramCountPrimaryKeys(TYPE_COUNT, programId);
    Optional<StructuredRow> row = programCountsTable.read(countKey);
    return row.isPresent() ? row.get().getLong(StoreDefinition.AppMetadataStore.COUNTS) : 0;
  }

  /**
   * Get the run counts of the given program collections.
   *
   * @param programIds the collection of program ids to get the program
   * @return the map of the program id to its run count
   */
  public Map<ProgramId, Long> getProgramRunCounts(Collection<ProgramId> programIds)
    throws BadRequestException, IOException {
    Map<ProgramId, Long> result = new LinkedHashMap<>();
    if (programIds.size() > 100) {
      throw new BadRequestException(String.format("%d programs found, the maximum number supported is 100",
                                                  programIds.size()));
    }
    for (ProgramId programId : programIds) {
      List<Field<?>> countKey = getProgramCountPrimaryKeys(TYPE_COUNT, programId);
      Optional<StructuredRow> row = programCountsTable.read(countKey);
      result.put(programId, row.isPresent() ? row.get().getLong(StoreDefinition.AppMetadataStore.COUNTS) : 0);
    }
    return result;
  }

  /**
   * Write the start time for the upgrade if it does not exist.
   */
  void writeUpgradeStartTimeIfNotExist() throws IOException {
    Optional<StructuredRow> row = getUpgradeMetadataRow(RUN_COUNT_FIRST_UPGRADE_TIME);
    if (!row.isPresent()) {
      LOG.info("Start to upgrade the run count");
      Collection<Field<?>> fields = row.get().getPrimaryKeys();
      fields.add(
        Fields.stringField(
          StoreDefinition.AppMetadataStore.METADATA_VALUE,
          String.valueOf(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()))));
      upgradeMetadataTable.upsert(fields);
    }
  }

  /**
   * Upgrade the active run records.
   *
   * @param maxRows maximum number of rows to be upgraded in this call.
   * @return a boolean indicating whether the upgrade of active run records is complete.
   */
  boolean upgradeActiveRunRecords(int maxRows) throws IOException {
    int remainingRows = maxRows - upgradeActiveRowkey(TYPE_RUN_RECORD_STARTING, maxRows);
    if (remainingRows > 0) {
      remainingRows -= upgradeActiveRowkey(TYPE_RUN_RECORD_STARTED, remainingRows);
    }
    if (remainingRows > 0) {
      remainingRows -= upgradeActiveRowkey(TYPE_RUN_RECORD_SUSPENDED, remainingRows);
    }

    // If we are not able to scan to the max number of rows, that means we reach the end of the active run records
    return remainingRows > 0;
  }

  /**
   * Upgrades the rowkeys for the active run record. Note that we do not count the run record of the old active runs
   * since it is hard to deal with the races and we expect user to stop all the programs before they do the upgrade.
   *
   * @param recordType type of the record.
   * @param maxRows maximum number of rows to be upgraded in this call.
   * @return the number of rows it updates
   */
  private int upgradeActiveRowkey(String recordType, int maxRows) throws IOException {
    List<Field<?>> startKey = getRunRecordStatusPrefix(recordType);
    Iterator<StructuredRow> iterator = runRecordsTable.scan(Range.singleton(startKey), maxRows);
    List<StructuredRow> rows = ImmutableList.copyOf(iterator);

    for(StructuredRow row : rows) {
      Collection<Field<?>> keys = row.getPrimaryKeys();
      RunRecordMeta runRecord =
        GSON.fromJson(row.getString(StoreDefinition.AppMetadataStore.RUN_RECORD_DATA), RunRecordMeta.class);
      List<Field<?>> newKeys = getProgramIDFields(keys);
      newKeys.add(
        Fields.longField(
          StoreDefinition.AppMetadataStore.RUN_START_TIME, getInvertedTsKeyPart(runRecord.getStartTs())));
      newKeys.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_ID, runRecord.getPid()));
      newKeys.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_RECORD_DATA, GSON.toJson(runRecord)));
      runRecordsTable.delete(keys);
      runRecordsTable.upsert(newKeys);
    }

    LOG.info("Upgrading {} active run records of {}", rows.size(), recordType);

    return rows.size();
  }

  /**
   * Upgrades the rowkeys for the completed run record which is older than the time at row key
   * RUN_COUNT_FIRST_UPGRADE_TIME.
   *
   * @param maxRows maximum number of rows to be upgraded in this call.
   * @return true if all old completed run records has been scanned.
   */
  boolean computeOldCompletedRunCount(int maxRows) throws IOException {
    Optional<StructuredRow> upgradeStart = getUpgradeMetadataRow(RUN_COUNT_FIRST_UPGRADE_TIME);
    if (!upgradeStart.isPresent()) {
      // this should not happen since we will write the upgrade start time before the upgrade start
      LOG.warn("Unable to get the first upgrade start time, will use the current timestamp to distinguish the old " +
                 "run records");
    }

    long upgradeStartTime =
      upgradeStart.isPresent()
        ? Long.valueOf(upgradeStart.get().getString(StoreDefinition.AppMetadataStore.METADATA_VALUE))
        : TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

    // get the progress we have, if no entry, scan from the beginning
    Optional<StructuredRow> progress = getUpgradeMetadataRow(RUN_COUNT_PROGRESS);
    List<Field<?>> prefix = getRunRecordStatusPrefix(TYPE_RUN_RECORD_COMPLETED);
    // ArrayList to be precise about deserialization
    List<Field<?>> startKey =
      progress.isPresent()
        ? GSON.fromJson(progress.get().getString(StoreDefinition.AppMetadataStore.METADATA_VALUE),
                        new TypeToken<ArrayList<Field<?>>>() {
                        }.getType())
        : prefix;

    // Bound is EXCLUSIVE so that we do not re-read the same record. This is fine for the beginning since prefix is
    // not a valid key.
    Iterator<StructuredRow> iterator = runRecordsTable.scan(Range.from(startKey, Range.Bound.EXCLUSIVE), maxRows);
    List<Field<?>> lastPrimaryKey = null;
    Map<List<Field<?>>, Long> counts = new HashMap<>();
    int numProcessed = 0;

    while (iterator.hasNext()) {
      StructuredRow row = iterator.next();
      RunRecordMeta runRecord =
        GSON.fromJson(row.getString(StoreDefinition.AppMetadataStore.RUN_RECORD_DATA), RunRecordMeta.class);
      lastPrimaryKey = new ArrayList<>(row.getPrimaryKeys());
      List<Field<?>> programFields = getProgramIDFields(lastPrimaryKey);
      // Sort by field name so the list can be used as a key in the map. This way, we can be lazy and not convert
      // the list of fields to a ProgramId.
      programFields.sort(Comparator.comparing(Field::getName));
      if (runRecord.getStartTs() < upgradeStartTime) {
        counts.compute(programFields, (programFields1, count) -> count == null ? 1L : count + 1L);
      }
      numProcessed++;
    }

    if (lastPrimaryKey != null) {
      upgradeMetadataTable.upsert(
        ImmutableList.of(Fields.stringField(StoreDefinition.AppMetadataStore.METADATA_KEY, RUN_COUNT_PROGRESS),
                         Fields.stringField(StoreDefinition.AppMetadataStore.METADATA_VALUE,
                                            GSON.toJson(lastPrimaryKey))));
    }

    // If we are not able to get the max number of rows, that means we reach the end of this record type
    return numProcessed < maxRows;
  }

  /**
   * Merge the old count result to the new count cell.
   *
   * @param maxRows maximum number of rows to be upgraded in this call.
   * @return true if all the old result has been merged.
   */
  boolean mergeCountResult(int maxRows) throws IOException {
    List<Field<?>> key = getCountTypePrefix(TYPE_RUN_RECORD_UPGRADE_COUNT);
    Iterator<StructuredRow> iterator = programCountsTable.scan(Range.from(key, Range.Bound.INCLUSIVE), maxRows);
    List<StructuredRow> rows = ImmutableList.copyOf(iterator);
    for (StructuredRow row : rows) {
      Collection<Field<?>> primaryKeys = row.getPrimaryKeys();
      List<Field<?>> newKeys = new ArrayList<>();
      for (Field<?> field : primaryKeys) {
        if (field.getName().equals(StoreDefinition.AppMetadataStore.COUNT_TYPE)) {
          newKeys.add(Fields.stringField(StoreDefinition.AppMetadataStore.COUNT_TYPE, TYPE_COUNT));
        } else {
          newKeys.add(field);
        }
      }
      programCountsTable.increment(
        newKeys, StoreDefinition.AppMetadataStore.COUNTS, row.getLong(StoreDefinition.AppMetadataStore.COUNTS));
      programCountsTable.delete(primaryKeys);
    }

    return rows.size() < maxRows;
  }

  void deleteStartUpTimeRow() throws IOException {
    upgradeMetadataTable.delete(
      ImmutableList.of(
        Fields.stringField(StoreDefinition.AppMetadataStore.METADATA_KEY, RUN_COUNT_FIRST_UPGRADE_TIME)));
  }

  /**
   * @return true if the upgrade of the app meta store is complete
   */
  boolean hasUpgraded() throws IOException {
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

      Optional<StructuredRow> row = getUpgradeMetadataRow(APP_VERSION_UPGRADE_KEY);
      if (!row.isPresent()) {
        return false;
      }
      String version = row.get().getString(StoreDefinition.AppMetadataStore.METADATA_VALUE);
      ProjectInfo.Version actual = new ProjectInfo.Version(version);
      upgradeCompleted = upgraded = actual.compareTo(ProjectInfo.getVersion()) >= 0;
    }
    return upgraded;
  }

  /**
   * Mark the table that the upgrade is complete.
   * TODO: CDAP-14114 change the logic to run count upgrade
   */
  public void upgradeCompleted() throws IOException {
    MDSKey.Builder keyBuilder = new MDSKey.Builder();
    keyBuilder.add(APP_VERSION_UPGRADE_KEY);
    List<Field<?>> fields =
      ImmutableList.of(Fields.stringField(StoreDefinition.AppMetadataStore.METADATA_KEY, APP_VERSION_UPGRADE_KEY),
                       Fields.stringField(StoreDefinition.AppMetadataStore.METADATA_VALUE, ProjectInfo.getVersion().toString()));
    upgradeMetadataTable.upsert(fields);

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
  public String retrieveSubscriberState(String topic, String subscriber) throws IOException {
    List<Field<?>> keys = getSubscriberKeys(topic, subscriber);
    Optional<StructuredRow> row = subscriberStateTable.read(keys);

    return row.isPresent() ? row.get().getString(StoreDefinition.AppMetadataStore.SUBSCRIBER_MESSAGE) : null;
  }

  /**
   * Updates the given topic's last fetched message id with the given message id for the given subscriber.
   *
   * @param topic the topic to persist the message id
   * @param subscriber the subscriber name
   * @param messageId the most recently processed message id
   */
  public void persistSubscriberState(String topic, String subscriber, String messageId) throws IOException {
    List<Field<?>> keys = getSubscriberKeys(topic, subscriber);
    keys.add(Fields.stringField(StoreDefinition.AppMetadataStore.SUBSCRIBER_MESSAGE, messageId));

    subscriberStateTable.upsert(keys);
  }

  /**
   * Deletes the given topic's last fetched message id with the given message id for the given subscriber.
   *
   * @param topic the topic to persist the message id
   * @param subscriber the subscriber name
   */
  public void deleteSubscriberState(String topic, String subscriber) throws IOException {
    List<Field<?>> keys = getSubscriberKeys(topic, subscriber);
    subscriberStateTable.delete(keys);
  }

  @VisibleForTesting
  Set<RunId> getRunningInRangeForStatus(String statusKey, final long startTimeInSecs,
                                                   final long endTimeInSecs) throws IOException {
    // Create time filter to get running programs between start and end time
    Predicate<RunRecordMeta> timeFilter = (runRecordMeta) ->
      runRecordMeta.getStartTs() < endTimeInSecs &&
        (runRecordMeta.getStopTs() == null || runRecordMeta.getStopTs() >= startTimeInSecs);

    List<Field<?>> prefix = getRunRecordStatusPrefix(statusKey);
    return
      getProgramRunIdMap(Range.singleton(prefix), timeFilter, null, Integer.MAX_VALUE).entrySet()
        .stream()
        .map(entry -> RunIds.fromString(entry.getValue().getPid()))
        .collect(Collectors.toSet());
  }

  /**
   * Returns a ProgramId given the MDS key
   *
   * @param fullPrimaryKeys the full set of primary keys to get the programId fields from
   * @return the remaining list of primary keys which describe a programId
   */
  private List<Field<?>> getProgramIDFields(Collection<Field<?>> fullPrimaryKeys) {
    List<Field<?>> newKeys = new ArrayList();
    for (Field<?> field : fullPrimaryKeys) {
      if (!field.getName().equals(StoreDefinition.AppMetadataStore.RUN_START_TIME)
        && !field.getName().equals(StoreDefinition.AppMetadataStore.RUN_ID)) {
        newKeys.add(field);
      }
    }
    return newKeys;
  }

  private List<Field<?>> getSubscriberKeys(String topic, String subscriber) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.SUBSCRIBER_TOPIC, topic));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.SUBSCRIBER, subscriber));
    return fields;
  }

  private Optional<StructuredRow> getUpgradeMetadataRow(String keyFieldName) throws IOException {
    List<Field<?>> fields = new ArrayList();
    fields.add(
      Fields.stringField(StoreDefinition.AppMetadataStore.METADATA_KEY, keyFieldName));
    return upgradeMetadataTable.read(fields);
  }

  private List<Field<?>> getProgramRunInvertedTimeKey(String recordType, ProgramRunId runId, long startTs) {
    List<Field<?>> fields = getWorkflowPrimaryKeysWithoutNode(runId);
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_STATUS, recordType));
    fields.add(Fields.longField(StoreDefinition.AppMetadataStore.RUN_START_TIME, getInvertedTsKeyPart(startTs)));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_ID, runId.getRun()));
    return fields;
  }

  private List<Field<?>> getApplicationPrimaryKeys(String namespaceId, String appId, String versionId) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, namespaceId));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_FIELD, appId));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.VERSION_FIELD, versionId));
    return fields;
  }

  private Range getNamespaceRange(String namespaceId) {
    return Range.singleton(
      ImmutableList.of(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, namespaceId)));
  }

  private Range getNamespaceAndApplicationRange(String namespaceId, String applicationId) {
    return Range.singleton(
      ImmutableList.of(
        Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, namespaceId),
        Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_FIELD, applicationId)));
  }

  private void writeApplicationSerialized(String namespaceId, String appId, String versionId, String serialized)
    throws IOException {
    List<Field<?>> fields = getApplicationPrimaryKeys(namespaceId, appId, versionId);
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_DATA_FIELD, serialized));
    applicationSpecificationTable.upsert(fields);
  }

  private List<Field<?>> getWorkflowPrimaryKeysWithoutNode(ProgramRunId programRunId) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, programRunId.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_FIELD, programRunId.getApplication()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.VERSION_FIELD, programRunId.getVersion()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_TYPE_FIELD, programRunId.getType().name()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_FIELD, programRunId.getProgram()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_FIELD, programRunId.getRun()));
    return fields;
  }

  private List<Field<?>> getWorkflowPrimaryKeys(ProgramRunId programRunId, String nodeId) {
    List<Field<?>> fields = getWorkflowPrimaryKeysWithoutNode(programRunId);
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NODE_ID, nodeId));
    return fields;
  }

  private List<Field<?>> getCountTypePrefix(String countType) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.COUNT_TYPE, countType));
    return fields;
  }

  private List<Field<?>> getCountNamespacePrefix(String countType, String namespace) {
    List<Field<?>> fields = getCountTypePrefix(countType);
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, namespace));
    return fields;
  }

  private List<Field<?>> getCountApplicationPrefix(String countType, ApplicationId applicationId) {
    List<Field<?>> fields = getCountTypePrefix(countType);
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, applicationId.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_FIELD, applicationId.getApplication()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.VERSION_FIELD, applicationId.getVersion()));
    return fields;
  }

  private <T> List<T> scanWithRange(Range range, Type typeofT, StructuredTable table, String field)
    throws IOException {
    List<T> result = new ArrayList<>();
    Iterator<StructuredRow> iterator = table.scan(range, Integer.MAX_VALUE);
    while (iterator.hasNext()) {
      result.add(
        GSON.fromJson(iterator.next().getString(field), typeofT));
    }
    return result;
  }

  private void writeToStructuredTableWithPrimaryKeys(
    List<Field<?>> keys, Object data, StructuredTable table, String field) throws IOException {
    keys.add(Fields.stringField(field, GSON.toJson(data)));
    table.upsert(keys);
  }

  private List<Field<?>> getRunRecordStatusPrefix(String status) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_STATUS, status));
    return fields;
  }

  private List<Field<?>> getRunRecordNamespacePrefix(String status, @Nullable NamespaceId namespaceId) {
    List<Field<?>> fields = getRunRecordStatusPrefix(status);
    if (namespaceId == null) {
      return fields;
    }
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, namespaceId.getNamespace()));
    return fields;
  }

  private List<Field<?>> getRunRecordNamespacePrefixWithString(String status, String namespaceId) {
    List<Field<?>> fields = getRunRecordStatusPrefix(status);
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, namespaceId));
    return fields;
  }

  private List<Field<?>> getRunRecordApplicationPrefix(String status, @Nullable ApplicationId applicationId) {
    List<Field<?>> fields = getRunRecordStatusPrefix(status);
    if (applicationId == null) {
      return fields;
    }
    fields.addAll(getApplicationPrimaryKeys(
      applicationId.getNamespace(), applicationId.getApplication(), applicationId.getVersion()));
    return fields;
  }

  private List<Field<?>> getRunRecordProgramPrefix(String status, @Nullable ProgramId programId) {
    if (programId == null) {
      return getRunRecordStatusPrefix(status);
    }
    List<Field<?>> fields =
      getRunRecordApplicationPrefix(
        status, new ApplicationId(programId.getNamespace(), programId.getApplication(), programId.getVersion()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_TYPE_FIELD, programId.getType().name()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_FIELD, programId.getProgram()));
    return fields;
  }

  @Nullable
  private RunRecordMeta getRunRecordMeta(List<Field<?>> primaryKeys) throws IOException {
    Optional<StructuredRow> row = runRecordsTable.read(primaryKeys);
    if (!row.isPresent()) {
      return null;
    }
    return GSON.fromJson(row.get().getString(StoreDefinition.AppMetadataStore.RUN_RECORD_DATA), RunRecordMeta.class);
  }

  private List<Field<?>> getProgramCountPrimaryKeys(String type, ProgramId programId) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.COUNT_TYPE, type));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, programId.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_FIELD, programId.getApplication()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.VERSION_FIELD, programId.getVersion()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_TYPE_FIELD, programId.getType().name()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_FIELD, programId.getProgram()));
    return fields;
  }
}

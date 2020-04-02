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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.ProgramRunCluster;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
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

  private static final String TYPE_RUN_RECORD_ACTIVE = "runRecordActive";

  private static final String TYPE_RUN_RECORD_COMPLETED = "runRecordCompleted";

  private static final String TYPE_COUNT = "runRecordCount";
  private static final String TYPE_RUN_RECORD_UPGRADE_COUNT = "runRecordUpgradeCount";
  private static final String SMALLEST_POSSIBLE_STRING = "";

  private static final Map<ProgramRunStatus, String> STATUS_TYPE_MAP = ImmutableMap.<ProgramRunStatus, String>builder()
    .put(ProgramRunStatus.PENDING, TYPE_RUN_RECORD_ACTIVE)
    .put(ProgramRunStatus.STARTING, TYPE_RUN_RECORD_ACTIVE)
    .put(ProgramRunStatus.RUNNING, TYPE_RUN_RECORD_ACTIVE)
    .put(ProgramRunStatus.SUSPENDED, TYPE_RUN_RECORD_ACTIVE)
    .put(ProgramRunStatus.COMPLETED, TYPE_RUN_RECORD_COMPLETED)
    .put(ProgramRunStatus.KILLED, TYPE_RUN_RECORD_COMPLETED)
    .put(ProgramRunStatus.FAILED, TYPE_RUN_RECORD_COMPLETED)
    .put(ProgramRunStatus.REJECTED, TYPE_RUN_RECORD_COMPLETED)
    .build();

  private final StructuredTableContext context;
  private StructuredTable applicationSpecificationTable;
  private StructuredTable workflowNodeStateTable;
  private StructuredTable runRecordsTable;
  private StructuredTable workflowsTable;
  private StructuredTable programCountsTable;
  private StructuredTable subscriberStateTable;

  /**
   * Static method for creating an instance of {@link AppMetadataStore}.
   */
  public static AppMetadataStore create(StructuredTableContext context) {
    return new AppMetadataStore(context);
  }

  private AppMetadataStore(StructuredTableContext context) {
    this.context = context;
  }

  private StructuredTable getApplicationSpecificationTable() {
    try {
      if (applicationSpecificationTable == null) {
        applicationSpecificationTable = context.getTable(StoreDefinition.AppMetadataStore.APPLICATION_SPECIFICATIONS);
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    return applicationSpecificationTable;
  }

  private StructuredTable getWorkflowNodeStateTable() {
    try {
      if (workflowNodeStateTable == null) {
        workflowNodeStateTable = context.getTable(StoreDefinition.AppMetadataStore.WORKFLOW_NODE_STATES);
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    return workflowNodeStateTable;
  }

  private StructuredTable getRunRecordsTable() {
    try {
      if (runRecordsTable == null) {
        runRecordsTable = context.getTable(StoreDefinition.AppMetadataStore.RUN_RECORDS);
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    return runRecordsTable;
  }

  private StructuredTable getWorkflowsTable() {
    try {
      if (workflowsTable == null) {
        workflowsTable = context.getTable(StoreDefinition.AppMetadataStore.WORKFLOWS);
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    return workflowsTable;
  }

  private StructuredTable getProgramCountsTable() {
    try {
      if (programCountsTable == null) {
        programCountsTable = context.getTable(StoreDefinition.AppMetadataStore.PROGRAM_COUNTS);
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    return programCountsTable;
  }

  private StructuredTable getSubscriberStateTable() {
    try {
      if (subscriberStateTable == null) {
        subscriberStateTable = context.getTable(StoreDefinition.AppMetadataStore.SUBSCRIBER_STATES);
      }
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    return subscriberStateTable;
  }

  @Nullable
  public ApplicationMeta getApplication(ApplicationId appId) throws IOException {
    return getApplication(appId.getNamespace(), appId.getApplication(), appId.getVersion());
  }

  @Nullable
  public ApplicationMeta getApplication(String namespaceId, String appId, String versionId)
    throws IOException {
    List<Field<?>> fields = getApplicationPrimaryKeys(namespaceId, appId, versionId);
    return getApplicationSpecificationTable().read(fields)
      .map(r -> GSON.fromJson(r.getString(StoreDefinition.AppMetadataStore.APPLICATION_DATA_FIELD),
                              ApplicationMeta.class))
      .orElse(null);
  }

  public List<ApplicationMeta> getAllApplications(String namespaceId) throws IOException {
    return
      scanWithRange(
        getNamespaceRange(namespaceId),
        ApplicationMeta.class,
        getApplicationSpecificationTable(),
        StoreDefinition.AppMetadataStore.APPLICATION_DATA_FIELD);
  }

  public List<ApplicationMeta> getAllAppVersions(String namespaceId, String appId) throws IOException {
    return scanWithRange(
      getNamespaceAndApplicationRange(namespaceId, appId),
      ApplicationMeta.class,
      getApplicationSpecificationTable(),
      StoreDefinition.AppMetadataStore.APPLICATION_DATA_FIELD);
  }

  public List<ApplicationId> getAllAppVersionsAppIds(String namespaceId, String appId) throws IOException {
    List<ApplicationId> appIds = new ArrayList<>();
    try (CloseableIterator<StructuredRow> iterator =
           getApplicationSpecificationTable().scan(getNamespaceAndApplicationRange(namespaceId, appId),
                                                   Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        appIds.add(getApplicationIdFromRow(iterator.next()));
      }
    }
   return appIds;
  }

  /**
   * Gets the {@link ApplicationMeta} for the given set of {@link ApplicationId}.
   *
   * @param appIds set of application id to read
   * @return a {@link Map} from {@link ApplicationId} to the corresponding {@link ApplicationMeta}. There will be
   *         no entry for application that doesn't exist
   * @throws IOException if failed to read metadata
   */
  public Map<ApplicationId, ApplicationMeta> getApplicationsForAppIds(Collection<ApplicationId> appIds)
    throws IOException {
    Map<ApplicationId, ApplicationMeta> result = new HashMap<>();
    List<List<Field<?>>> multiKeys = new ArrayList<>();
    for (ApplicationId appId: appIds) {
      multiKeys.add(getApplicationPrimaryKeys(appId.getNamespace(), appId.getApplication(), appId.getVersion()));
    }

    for (StructuredRow row : getApplicationSpecificationTable().multiRead(multiKeys)) {
      ApplicationId appId = getApplicationIdFromRow(row);
      result.put(appId, GSON.fromJson(row.getString(StoreDefinition.AppMetadataStore.APPLICATION_DATA_FIELD),
                                      ApplicationMeta.class));
    }

    return result;
  }

  /**
   * Filter the given set of programs and return those that exist.
   *
   * @param programIds the set of program ids to filter
   * @return the set of program ids that exist
   * @throws IOException if failed to read metadata
   */
  public Set<ProgramId> filterProgramsExistence(Collection<ProgramId> programIds) throws IOException {
    Set<ApplicationId> appIds = programIds.stream().map(ProgramId::getParent).collect(Collectors.toSet());
    List<List<Field<?>>> multiKeys = new ArrayList<>();
    for (ApplicationId appId: appIds) {
      multiKeys.add(getApplicationPrimaryKeys(appId.getNamespace(), appId.getApplication(), appId.getVersion()));
    }

    Set<ProgramId> existingPrograms = new HashSet<>();
    for (StructuredRow row : getApplicationSpecificationTable().multiRead(multiKeys)) {
      ApplicationId appId = getApplicationIdFromRow(row);
      String appMeta = row.getString(StoreDefinition.AppMetadataStore.APPLICATION_DATA_FIELD);
      if (appMeta == null) {
        throw new IOException("Missing application metadata for application " + appId);
      }
      try (JsonReader reader = new JsonReader(new StringReader(appMeta))) {
        reader.beginObject();
        while (reader.peek() != JsonToken.END_OBJECT) {
          String name = reader.nextName();
          if (name.equals("spec")) {
            existingPrograms.addAll(ApplicationSpecificationAdapter.getProgramIds(appId, reader));
          } else {
            reader.skipValue();
          }
        }
        reader.endObject();
      }
    }
    return programIds.stream().filter(existingPrograms::contains).collect(Collectors.toSet());
  }


  public void writeApplication(String namespaceId, String appId, String versionId, ApplicationSpecification spec)
    throws IOException {
    writeApplicationSerialized(namespaceId, appId, versionId, GSON.toJson(new ApplicationMeta(appId, spec)));
  }

  public void deleteApplication(String namespaceId, String appId, String versionId)
    throws IOException {
    List<Field<?>> fields = getApplicationPrimaryKeys(namespaceId, appId, versionId);
    getApplicationSpecificationTable().delete(fields);
  }

  public void deleteApplications(String namespaceId)
    throws IOException {
    getApplicationSpecificationTable().deleteAll(getNamespaceRange(namespaceId));
  }

  public void updateAppSpec(ApplicationId appId, ApplicationSpecification spec) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("App spec to be updated: id: {}: spec: {}", appId, GSON.toJson(spec));
    }
    ApplicationMeta existing = getApplication(appId);

    if (existing == null) {
      throw new IllegalArgumentException("Application " + appId + " does not exist");
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Application {} exists in mds with specification {}", appId, GSON.toJson(existing));
    }
    ApplicationMeta updated = ApplicationMeta.updateSpec(existing, spec);
    writeApplicationSerialized(appId.getNamespace(), appId.getApplication(), appId.getVersion(), GSON.toJson(updated));
  }

  /**
   * Return the {@link List} of {@link WorkflowNodeStateDetail} for a given Workflow run.
   */
  public List<WorkflowNodeStateDetail> getWorkflowNodeStates(ProgramRunId workflowRunId) throws IOException {
    return scanWithRange(
      Range.singleton(getProgramRunPrimaryKeys(workflowRunId)),
      WorkflowNodeStateDetail.class,
      getWorkflowNodeStateTable(),
      StoreDefinition.AppMetadataStore.NODE_STATE_DATA);
  }

  /**
   * This method is called to associate node state of custom action with the Workflow run.
   *
   * @param workflowRunId the run for which node state is to be added
   * @param nodeStateDetail node state details to be added
   */
  public void addWorkflowNodeState(ProgramRunId workflowRunId,
                                   WorkflowNodeStateDetail nodeStateDetail) throws IOException {
    List<Field<?>> fields = getWorkflowPrimaryKeys(workflowRunId, nodeStateDetail.getNodeId());
    writeToStructuredTableWithPrimaryKeys(fields, nodeStateDetail, getWorkflowNodeStateTable(),
                                          StoreDefinition.AppMetadataStore.NODE_STATE_DATA);
  }

  private void addWorkflowNodeState(ProgramRunId programRunId, Map<String, String> systemArgs,
                                    ProgramRunStatus status,
                                    @Nullable BasicThrowable failureCause, byte[] sourceId) throws IOException {
    String workflowNodeId = systemArgs.get(ProgramOptionConstants.WORKFLOW_NODE_ID);
    String workflowName = systemArgs.get(ProgramOptionConstants.WORKFLOW_NAME);
    String workflowRun = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);

    ApplicationId appId = programRunId.getParent().getParent();
    ProgramRunId workflowRunId = appId.workflow(workflowName).run(workflowRun);

    WorkflowNodeStateDetail nodeStateDetail = new WorkflowNodeStateDetail(workflowNodeId,
                                                                          ProgramRunStatus.toNodeStatus(status),
                                                                          programRunId.getRun(), failureCause);
    List<Field<?>> fields = getWorkflowPrimaryKeys(workflowRunId, nodeStateDetail.getNodeId());
    writeToStructuredTableWithPrimaryKeys(fields, nodeStateDetail, getWorkflowNodeStateTable(),
                                          StoreDefinition.AppMetadataStore.NODE_STATE_DATA);

    // Get the run record of the Workflow which started this program
    List<Field<?>> runRecordFields = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, workflowRunId,
                                                                  RunIds.getTime(workflowRun, TimeUnit.SECONDS));

    Optional<StructuredRow> row = getRunRecordsTable().read(runRecordFields);
    if (row.isPresent()) {
      RunRecordDetail record = deserializeRunRecordMeta(row.get());
      // Update the parent Workflow run record by adding node id and program run id in the properties
      Map<String, String> properties = new HashMap<>(record.getProperties());
      properties.put(workflowNodeId, programRunId.getRun());
      writeToStructuredTableWithPrimaryKeys(
        runRecordFields, RunRecordDetail.builder(record).setProperties(properties).setSourceId(sourceId).build(),
        getRunRecordsTable(), StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
  public RunRecordDetail recordProgramProvisioning(ProgramRunId programRunId, Map<String, String> runtimeArgs,
                                                   Map<String, String> systemArgs, byte[] sourceId,
                                                   @Nullable ArtifactId artifactId)
    throws IOException {
    long startTs = RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS);
    if (startTs == -1L) {
      LOG.error("Ignoring unexpected request to record provisioning state for program run {} that does not have " +
                  "a timestamp in the run id.", programRunId);
      return null;
    }

    RunRecordDetail existing = getRun(programRunId);
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
    RunRecordDetail meta = RunRecordDetail.builder()
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
    writeNewRunRecord(meta, TYPE_RUN_RECORD_ACTIVE);
    LOG.trace("Recorded {} for program {}", ProgramRunClusterStatus.PROVISIONING, programRunId);
    return meta;
  }

  // return the property map to set in the RunRecordDetail
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
   * @return {@link RunRecordDetail} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordDetail recordProgramProvisioned(ProgramRunId programRunId, int numNodes, byte[] sourceId)
    throws IOException {
    RunRecordDetail existing = getRun(programRunId);

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
    RunRecordDetail meta = RunRecordDetail.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, getRunRecordsTable(), StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
   * @return {@link RunRecordDetail} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordDetail recordProgramDeprovisioning(ProgramRunId programRunId, byte[] sourceId)
    throws IOException {
    RunRecordDetail existing = getRun(programRunId);
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
    RunRecordDetail meta = RunRecordDetail.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, getRunRecordsTable(), StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
   * @return {@link RunRecordDetail} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordDetail recordProgramDeprovisioned(ProgramRunId programRunId, @Nullable Long endTs, byte[] sourceId)
    throws IOException {
    RunRecordDetail existing = getRun(programRunId);
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
    RunRecordDetail meta = RunRecordDetail.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, getRunRecordsTable(), StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
   * @return {@link RunRecordDetail} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordDetail recordProgramOrphaned(ProgramRunId programRunId, long endTs, byte[] sourceId)
    throws IOException {
    RunRecordDetail existing = getRun(programRunId);
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
    RunRecordDetail meta = RunRecordDetail.builder(existing)
      .setCluster(cluster)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, getRunRecordsTable(), StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
    LOG.trace("Recorded {} for program {}", ProgramRunClusterStatus.ORPHANED, programRunId);
    return meta;
  }

  @Nullable
  public RunRecordDetail recordProgramRejected(ProgramRunId programRunId,
                                               Map<String, String> runtimeArgs, Map<String, String> systemArgs,
                                               byte[] sourceId, @Nullable ArtifactId artifactId)
    throws IOException {
    long startTs = RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS);
    if (startTs == -1L) {
      LOG.error("Ignoring unexpected request to record provisioning state for program run {} that does not have " +
                  "a timestamp in the run id.", programRunId);
      return null;
    }

    RunRecordDetail existing = getRun(programRunId);
    // for some reason, there is an existing run record?
    if (existing != null) {
      LOG.error("Ignoring unexpected request to record rejected state for program run {} that has an existing "
                  + "run record in run state {} and cluster state {}.",
                programRunId, existing.getStatus(), existing.getCluster().getStatus());
      return null;
    }

    Optional<ProfileId> profileId = SystemArguments.getProfileIdFromArgs(programRunId.getNamespaceId(), systemArgs);
    RunRecordDetail meta = RunRecordDetail.builder()
      .setProgramRunId(programRunId)
      .setStartTime(startTs)
      .setStopTime(startTs) // rejected: stop time == start time
      .setStatus(ProgramRunStatus.REJECTED)
      .setProperties(getRecordProperties(systemArgs, runtimeArgs))
      .setSystemArgs(systemArgs)
      .setProfileId(profileId.orElse(null))
      .setArtifactId(artifactId)
      .setSourceId(sourceId)
      .setPrincipal(systemArgs.get(ProgramOptionConstants.PRINCIPAL))
      .build();

    writeNewRunRecord(meta, TYPE_RUN_RECORD_COMPLETED);
    LOG.trace("Recorded {} for program {}", ProgramRunStatus.REJECTED, programRunId);
    return meta;
  }

  /**
   * Writes a new {@link RunRecordDetail} and increments the run count of a program.
   */
  private void writeNewRunRecord(RunRecordDetail meta, String typeRunRecordCompleted) throws IOException {
    List<Field<?>> fields = getProgramRunInvertedTimeKey(typeRunRecordCompleted,
                                                         meta.getProgramRunId(), meta.getStartTs());
    writeToStructuredTableWithPrimaryKeys(fields, meta, getRunRecordsTable(),
                                          StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
    List<Field<?>> countKey = getProgramCountPrimaryKeys(TYPE_COUNT, meta.getProgramRunId().getParent());
    getProgramCountsTable().increment(countKey, StoreDefinition.AppMetadataStore.COUNTS, 1L);
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
   * @return {@link RunRecordDetail} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordDetail recordProgramStart(ProgramRunId programRunId, @Nullable String twillRunId,
                                            Map<String, String> systemArgs, byte[] sourceId)
    throws IOException {
    RunRecordDetail existing = getRun(programRunId);
    RunRecordDetail meta;

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

    Map<String, String> newSystemArgs = new HashMap<>(existing.getSystemArgs());
    newSystemArgs.putAll(systemArgs);

    // Delete the old run record
    delete(existing);
    List<Field<?>> key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, existing.getStartTs());
    meta = RunRecordDetail.builder(existing)
      .setStatus(ProgramRunStatus.STARTING)
      .setSystemArgs(newSystemArgs)
      .setTwillRunId(twillRunId)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, getRunRecordsTable(), StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
   * @return {@link RunRecordDetail} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordDetail recordProgramRunning(ProgramRunId programRunId, long stateChangeTime,
                                              @Nullable String twillRunId,
                                              byte[] sourceId) throws IOException {
    RunRecordDetail existing = getRun(programRunId);
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
    RunRecordDetail meta = RunRecordDetail.builder(existing)
      .setRunTime(stateChangeTime)
      .setStatus(ProgramRunStatus.RUNNING)
      .setTwillRunId(twillRunId)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, getRunRecordsTable(), StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
   * @return {@link RunRecordDetail} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordDetail recordProgramSuspend(ProgramRunId programRunId, byte[] sourceId, long timestamp)
    throws IOException {
    RunRecordDetail existing = getRun(programRunId);
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
   * @return {@link RunRecordDetail} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordDetail recordProgramResumed(ProgramRunId programRunId, byte[] sourceId, long timestamp)
    throws IOException {
    RunRecordDetail existing = getRun(programRunId);
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

  private RunRecordDetail recordProgramSuspendResume(ProgramRunId programRunId, byte[] sourceId,
                                                     RunRecordDetail existing, String action, long timestamp)
    throws IOException {
    ProgramRunStatus toStatus = ProgramRunStatus.SUSPENDED;

    if (action.equals("resume")) {
      toStatus = ProgramRunStatus.RUNNING;
    }
    // Delete the old run record
    delete(existing);
    List<Field<?>> key = getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId, existing.getStartTs());
    RunRecordDetail.Builder builder = RunRecordDetail.builder(existing).setStatus(toStatus).setSourceId(sourceId);
    if (timestamp != -1) {
      if (action.equals("resume")) {
        builder.setResumeTime(timestamp);
      } else {
        builder.setSuspendTime(timestamp);
      }
    }
    RunRecordDetail meta = builder.build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, getRunRecordsTable(), StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
   * @return {@link RunRecordDetail} that was persisted, or {@code null} if the update was ignored.
   */
  @Nullable
  public RunRecordDetail recordProgramStop(ProgramRunId programRunId, long stopTs, ProgramRunStatus runStatus,
                                           @Nullable BasicThrowable failureCause, byte[] sourceId)
    throws IOException {
    RunRecordDetail existing = getRun(programRunId);
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
    RunRecordDetail meta = RunRecordDetail.builder(existing)
      .setStopTime(stopTs)
      .setStatus(runStatus)
      .setSourceId(sourceId)
      .build();
    writeToStructuredTableWithPrimaryKeys(
      key, meta, getRunRecordsTable(), StoreDefinition.AppMetadataStore.RUN_RECORD_DATA);
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
  private boolean isValid(RunRecordDetail existing, ProgramRunStatus nextProgramState,
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

  /**
   * Reads run records for the given set of {@link ProgramRunId}.
   *
   * @param programRunIds the set of program run ids to read
   * @return a {@link Map} from the program run id to the run record. If there is no run record
   *         for a given program run id, an entry will be presented with a {@code null} value
   * @throws IOException if failed to read run records
   */
  public Map<ProgramRunId, RunRecordDetail> getRuns(Set<ProgramRunId> programRunIds) throws IOException {
    // Query active run record first
    Map<ProgramRunId, RunRecordDetail> unfinishedRuns = getUnfinishedRuns(programRunIds);
    // For programs that are not running, fetch completed run
    Map<ProgramRunId, RunRecordDetail> completedRuns = getCompletedRuns(Sets.difference(programRunIds,
                                                                                        unfinishedRuns.keySet()));
    Map<ProgramRunId, RunRecordDetail> result = new LinkedHashMap<>();
    for (ProgramRunId programRunId : programRunIds) {
      result.put(programRunId, unfinishedRuns.getOrDefault(programRunId, completedRuns.get(programRunId)));
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
  public Map<ProgramRunId, RunRecordDetail> getActiveRuns(Set<NamespaceId> namespaces,
                                                          Predicate<RunRecordDetail> filter) throws IOException {
    Map<ProgramRunId, RunRecordDetail> result = new HashMap<>();
    for (NamespaceId namespaceId : namespaces) {
      List<Field<?>> prefix = getRunRecordNamespacePrefix(TYPE_RUN_RECORD_ACTIVE, namespaceId);
      result.putAll(getRuns(Range.singleton(prefix), ProgramRunStatus.ALL, Integer.MAX_VALUE, null, filter));
    }
    return result;
  }

  /**
   * Count all active runs.
   *
   * @param limit count at most that many runs, stop if there are more.
   */
  public int countActiveRuns(@Nullable Integer limit) throws IOException {
    AtomicInteger count = new AtomicInteger(0);
    try (CloseableIterator<RunRecordDetail> iterator = queryProgramRuns(
      Range.singleton(getRunRecordNamespacePrefix(TYPE_RUN_RECORD_ACTIVE, null)),
      key -> !NamespaceId.SYSTEM.getNamespace().equals(key.getString(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD)),
      null, limit != null ? limit : Integer.MAX_VALUE)) {
      iterator.forEachRemaining(m -> count.getAndIncrement());
    }
    return count.get();
  }

  /**
   * Represents a position for scanning.
   */
  public static final class Cursor {

    private final Collection<Field<?>> fields;
    private final Range.Bound bound;

    Cursor(Collection<Field<?>> fields, Range.Bound bound) {
      this.fields = fields;
      this.bound = bound;
    }
  }

  /**
   * Scans active runs, starting from the given cursor.
   *
   * @param cursor the cursor to start the scan, or {@code null} for the first scan. A cursor can be obtained
   *               from the call to the given {@link BiConsumer} for some previous scan.
   * @param limit maximum number of records to scan
   * @param consumer a {@link BiConsumer} to consume the scan result
   * @throws IOException if failed to query the storage
   */
  public void scanActiveRuns(@Nullable Cursor cursor, int limit,
                             BiConsumer<Cursor, RunRecordDetail> consumer) throws IOException {
    Collection<Field<?>> begin = cursor == null ? getRunRecordStatusPrefix(TYPE_RUN_RECORD_ACTIVE) : cursor.fields;
    Range range = Range.create(begin, cursor == null ? Range.Bound.INCLUSIVE : cursor.bound,
                               getRunRecordStatusPrefix(TYPE_RUN_RECORD_ACTIVE), Range.Bound.INCLUSIVE);

    StructuredTable table = getRunRecordsTable();
    try (CloseableIterator<StructuredRow> iterator = table.scan(range, limit)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        consumer.accept(new Cursor(row.getPrimaryKeys(), Range.Bound.EXCLUSIVE), deserializeRunRecordMeta(row));
      }
    }
  }

  /**
   * Get active runs in all namespaces with a filter, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param filter filter to filter run record
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordDetail> getActiveRuns(Predicate<RunRecordDetail> filter) throws IOException {
    List<Field<?>> prefix = getRunRecordNamespacePrefix(TYPE_RUN_RECORD_ACTIVE, null);
    return getRuns(Range.singleton(prefix), ProgramRunStatus.ALL, Integer.MAX_VALUE, null, filter);
  }

  /**
   * Get active runs in the given namespace, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param namespaceId given namespace
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordDetail> getActiveRuns(NamespaceId namespaceId) throws IOException {
    List<Field<?>> prefix = getRunRecordNamespacePrefix(TYPE_RUN_RECORD_ACTIVE, namespaceId);
    return getRuns(Range.singleton(prefix), ProgramRunStatus.ALL, Integer.MAX_VALUE, null, null);
  }

  /**
   * Get active runs in the given application, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param applicationId given app
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordDetail> getActiveRuns(ApplicationId applicationId) throws IOException {
    List<Field<?>> prefix = getRunRecordApplicationPrefix(TYPE_RUN_RECORD_ACTIVE, applicationId);
    return getRuns(Range.singleton(prefix), ProgramRunStatus.ALL, Integer.MAX_VALUE, null, null);
  }

  /**
   * Get active runs in the given program, active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param programId given program
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordDetail> getActiveRuns(ProgramId programId) throws IOException {
    List<Field<?>> prefix = getRunRecordProgramPrefix(TYPE_RUN_RECORD_ACTIVE, programId);
    return getRuns(Range.singleton(prefix), ProgramRunStatus.ALL, Integer.MAX_VALUE, null, null);
  }

  /**
   * Get active runs for the given programs. Active runs means program run with status STARTING, PENDING,
   * RUNNING or SUSPENDED.
   *
   * @param ids set of program ids to fetch for active run records
   * @return a map from {@link ProgramId} to a {@link Collection} of {@link RunRecordDetail}. It is guaranteed to have
   *         an entry for each of the given program id.
   * @throws IOException if failed to fetch the run records.
   */
  public Map<ProgramId, Collection<RunRecordDetail>> getActiveRuns(Collection<ProgramId> ids) throws IOException {
    Collection<Range> ranges = new ArrayList<>();
    Map<ProgramId, Collection<RunRecordDetail>> result = new LinkedHashMap<>();

    for (ProgramId programId : ids) {
      ranges.add(Range.singleton(getRunRecordProgramPrefix(TYPE_RUN_RECORD_ACTIVE, programId)));
      result.put(programId, new LinkedHashSet<>());
    }

    try (CloseableIterator<StructuredRow> iterator = getRunRecordsTable().multiScan(ranges, Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        RunRecordDetail meta = deserializeRunRecordMeta(row);
        result.get(meta.getProgramRunId().getParent()).add(meta);
      }
    }

    return result;
  }

  /**
   * Get runs for an optional {@link ProgramId} that fits the given set of criteria.
   * If the program id is not provided, it fetches all runs that matches with the criteria.
   *
   * @param programId an optional program id to match
   * @param status to filter by
   * @param startTime the run has to be started on or after this time
   * @param endTime the run has to be started before this time
   * @param limit of number of records to return
   * @param filter of RunRecordDetail to post filter by
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordDetail> getRuns(@Nullable ProgramId programId, ProgramRunStatus status,
                                                    long startTime, long endTime, int limit,
                                                    @Nullable Predicate<RunRecordDetail> filter)
    throws IOException {
    switch (status) {
      case ALL:
        Map<ProgramRunId, RunRecordDetail> runRecords = getProgramRuns(programId, status, startTime, endTime,
                                                                       limit, filter, TYPE_RUN_RECORD_ACTIVE);
        if (runRecords.size() < limit) {
          runRecords.putAll(getProgramRuns(programId, status, startTime, endTime,
                                           limit, filter, TYPE_RUN_RECORD_COMPLETED));
        }
        return runRecords;
      case PENDING:
      case STARTING:
      case RUNNING:
      case SUSPENDED:
        return getProgramRuns(programId, status, startTime, endTime, limit, filter, TYPE_RUN_RECORD_ACTIVE);
      default:
        return getProgramRuns(programId, status, startTime, endTime, limit, filter, TYPE_RUN_RECORD_COMPLETED);
    }
  }

  /**
   * Get runs in the given application.
   *
   * @param applicationId given application
   * @param status to filter by
   * @param limit of number of records to return
   * @param filter of RunRecordDetail to post filter by
   * @return map of run id to run record meta
   */
  public Map<ProgramRunId, RunRecordDetail> getRuns(ApplicationId applicationId, final ProgramRunStatus status,
                                                    int limit, @Nullable Predicate<RunRecordDetail> filter)
    throws IOException {
    switch (status) {
      case ALL:
        Map<ProgramRunId, RunRecordDetail> runRecords = getApplicationRuns(applicationId, status, limit, filter,
                                                                           TYPE_RUN_RECORD_ACTIVE);
        if (runRecords.size() < limit) {
          runRecords.putAll(getApplicationRuns(applicationId, status, limit - runRecords.size(), filter,
                                               TYPE_RUN_RECORD_COMPLETED));
        }
        return runRecords;
      case PENDING:
      case STARTING:
      case RUNNING:
      case SUSPENDED:
        return getApplicationRuns(applicationId, status, limit, filter, TYPE_RUN_RECORD_ACTIVE);
      default:
        return getApplicationRuns(applicationId, status, limit, filter, TYPE_RUN_RECORD_COMPLETED);
    }
  }

  // TODO: getRun is duplicated in cdap-watchdog AppMetadataStore class.
  // Any changes made here will have to be made over there too.
  // JIRA https://issues.cask.co/browse/CDAP-2172
  @Nullable
  public RunRecordDetail getRun(ProgramRunId programRun) throws IOException {
    // Query active run record first
    RunRecordDetail running = getUnfinishedRuns(Collections.singleton(programRun)).get(programRun);
    // If program is running, this will be non-null
    if (running != null) {
      return running;
    }
    // If program is not running, query completed run records
    return getCompletedRuns(Collections.singleton(programRun)).get(programRun);
  }

  /**
   * Deletes the run record for the given program run if the program state is in one of the terminated state.
   *
   * @param programRunId the program run id to lookup for run to be deleted
   * @param sourceId the source id that the program run recorded with.
   *                 It has to match with the run record for the deletion to proceed
   * @return the deleted {@link RunRecordDetail} or {@code null} if no record has been deleted
   * @throws IOException if failed to find or delete the record
   */
  @Nullable
  public RunRecordDetail deleteRunIfTerminated(ProgramRunId programRunId, byte[] sourceId) throws IOException {
    RunRecordDetail detail = getRun(programRunId);
    if (detail == null || !detail.getStatus().isEndState()) {
      return null;
    }
    if (detail.getSourceId() == null || sourceId == null) {
      return null;
    }
    if (!Arrays.equals(detail.getSourceId(), sourceId)) {
      return null;
    }
    delete(detail);
    return detail;
  }

  private void delete(RunRecordDetail record) throws IOException {
    ProgramRunId programRunId = record.getProgramRunId();
    List<Field<?>> key = getProgramRunInvertedTimeKey(STATUS_TYPE_MAP.get(record.getStatus()), programRunId,
                                              record.getStartTs());
    getRunRecordsTable().delete(key);
  }

  /**
   * @return run records for unfinished run
   */
  private Map<ProgramRunId, RunRecordDetail> getUnfinishedRuns(Set<ProgramRunId> programRunIds) throws IOException {
    List<List<Field<?>>> allKeys = new ArrayList<>();
    for (ProgramRunId programRunId : programRunIds) {
      allKeys.add(getProgramRunInvertedTimeKey(TYPE_RUN_RECORD_ACTIVE, programRunId,
                                               RunIds.getTime(programRunId.getRun(), TimeUnit.SECONDS)));
    }
    return getRunRecordsTable().multiRead(allKeys).stream()
      .map(AppMetadataStore::deserializeRunRecordMeta)
      .collect(Collectors.toMap(RunRecordDetail::getProgramRunId, r -> r, (r1, r2) -> {
        throw new IllegalStateException("Duplicate run record for " + r1.getProgramRunId());
      }, LinkedHashMap::new));
  }

  private Map<ProgramRunId, RunRecordDetail> getCompletedRuns(Set<ProgramRunId> programRunIds) throws IOException {
    List<List<Field<?>>> allKeys = new ArrayList<>();
    for (ProgramRunId programRunId : programRunIds) {
      List<Field<?>> keys = getRunRecordProgramPrefix(TYPE_RUN_RECORD_COMPLETED, programRunId.getParent());
      // Get start time from RunId
      long programStartSecs = RunIds.getTime(RunIds.fromString(programRunId.getRun()), TimeUnit.SECONDS);
      keys.add(Fields.longField(StoreDefinition.AppMetadataStore.RUN_START_TIME,
                                getInvertedTsKeyPart(programStartSecs)));
      keys.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_FIELD, programRunId.getRun()));
      allKeys.add(keys);
    }
    return getRunRecordsTable().multiRead(allKeys).stream()
      .map(AppMetadataStore::deserializeRunRecordMeta)
      .collect(Collectors.toMap(RunRecordDetail::getProgramRunId, r -> r, (r1, r2) -> {
        throw new IllegalStateException("Duplicate run record for " + r1.getProgramRunId());
      }, LinkedHashMap::new));
  }

  /**
   * Creates a {@link Range} for scanning the run record table with the given key prefix for records start time
   * fall in the given time range.
   */
  private Range createRunRecordScanRange(List<Field<?>> keyPrefix, long startTime, long endTime) {
    if (startTime <= 0 && endTime == Long.MAX_VALUE) {
      return Range.singleton(keyPrefix);
    }

    List<Field<?>> begin = new ArrayList<>(keyPrefix);
    List<Field<?>> end = new ArrayList<>(keyPrefix);

    begin.add(Fields.longField(StoreDefinition.AppMetadataStore.RUN_START_TIME, getInvertedTsScanKeyPart(endTime)));
    end.add(Fields.longField(StoreDefinition.AppMetadataStore.RUN_START_TIME, getInvertedTsScanKeyPart(startTime)));
    return Range.create(begin, Range.Bound.INCLUSIVE, end, Range.Bound.EXCLUSIVE);
  }

  /**
   * Returns the query limit based on the given {@link ProgramRunStatus}.
   * If the given status is {@link ProgramRunStatus#ALL}, then the limit is unaltered. Otherwise it will be
   * multiplied to make sure it will include enough records for post query status filtering.
   */
  private int getLimitByStatus(int limit, ProgramRunStatus status) {
    if (status == ProgramRunStatus.ALL) {
      return limit;
    }

    String type = STATUS_TYPE_MAP.get(status);
    int multiplier = (int) STATUS_TYPE_MAP.values().stream().filter(type::equals).count();

    if (limit > Integer.MAX_VALUE / multiplier) {
      return Integer.MAX_VALUE;
    }
    return limit * multiplier;
  }

  /**
   * Iterate over a range of run records, filter by predicates and pass each run record to the consumer.
   * @param range to scan runRecordsTable with
   * @param keyPredicate to filter the row keys by. If null, then does not filter.
   * @param predicate to filter the runRecordMetas by. If null, then does not filter.
   * @param limit the maximum number of entries to return
   */
  private CloseableIterator<RunRecordDetail> queryProgramRuns(Range range,
                                                              @Nullable Predicate<StructuredRow> keyPredicate,
                                                              @Nullable Predicate<RunRecordDetail> predicate,
                                                              int limit) throws IOException {
    CloseableIterator<StructuredRow> iterator = getRunRecordsTable()
      .scan(range, predicate == null && keyPredicate == null ? limit : Integer.MAX_VALUE);

    return new AbstractCloseableIterator<RunRecordDetail>() {

      private int currentLimit = limit;

      @Override
      protected RunRecordDetail computeNext() {
        if (currentLimit <= 0) {
          return endOfData();
        }

        while (iterator.hasNext()) {
          StructuredRow row = iterator.next();
          if (keyPredicate != null && !keyPredicate.test(row)) {
            continue;
          }
          RunRecordDetail recordMeta = deserializeRunRecordMeta(row);
          if (predicate == null || predicate.test(recordMeta)) {
            currentLimit--;
            return recordMeta;
          }
        }
        return endOfData();
      }

      @Override
      public void close() {
        iterator.close();
      }
    };
  }

  private Map<ProgramRunId, RunRecordDetail> getApplicationRuns(ApplicationId applicationId, ProgramRunStatus status,
                                                                int limit, @Nullable Predicate<RunRecordDetail> filter,
                                                                String recordType) throws IOException {
    List<Field<?>> prefix = getRunRecordApplicationPrefix(recordType, applicationId);
    return getRuns(Range.singleton(prefix), status, limit, null, filter);
  }


  private Map<ProgramRunId, RunRecordDetail> getProgramRuns(@Nullable ProgramId programId, ProgramRunStatus status,
                                                            long startTime, long endTime, int limit,
                                                            @Nullable Predicate<RunRecordDetail> filter,
                                                            String recordType) throws IOException {
    List<Field<?>> prefix = getRunRecordProgramPrefix(recordType, programId);
    Range scanRange;
    Predicate<StructuredRow> keyFilter = null;

    if (programId == null) {
      // Cannot use the run start time field if programId is missing. Need to use a key filter.
      keyFilter = getKeyFilterByTimeRange(startTime, endTime);
      scanRange = Range.singleton(prefix);
    } else {
      scanRange = createRunRecordScanRange(prefix, startTime, endTime);
    }

    return getRuns(scanRange, status, limit, keyFilter, filter);
  }

  private Map<ProgramRunId, RunRecordDetail> getRuns(Range range, ProgramRunStatus status, int limit,
                                                     @Nullable Predicate<StructuredRow> keyFilter,
                                                     @Nullable Predicate<RunRecordDetail> valueFilter)
    throws IOException {

    Map<ProgramRunId, RunRecordDetail> map = new LinkedHashMap<>();
    try (CloseableIterator<RunRecordDetail> iterator = queryProgramRuns(range, keyFilter, valueFilter,
                                                                        getLimitByStatus(limit, status))) {
      while (iterator.hasNext() && map.size() < limit) {
        RunRecordDetail meta = iterator.next();
        if (status == ProgramRunStatus.ALL || status == meta.getStatus()) {
          map.put(meta.getProgramRunId(), meta);
        }
      }
    }
    return map;
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

  public void deleteProgramHistory(String namespaceId, String appId, String versionId)
    throws IOException {
    ApplicationId applicationId = new ApplicationId(namespaceId, appId, versionId);
    getRunRecordsTable()
      .deleteAll(Range.singleton(getRunRecordApplicationPrefix(TYPE_RUN_RECORD_ACTIVE, applicationId)));
    getRunRecordsTable()
      .deleteAll(Range.singleton(getRunRecordApplicationPrefix(TYPE_RUN_RECORD_COMPLETED, applicationId)));
    getProgramCountsTable().deleteAll(Range.singleton(getCountApplicationPrefix(TYPE_COUNT, applicationId)));
    getProgramCountsTable().deleteAll(
      Range.singleton(getCountApplicationPrefix(TYPE_RUN_RECORD_UPGRADE_COUNT, applicationId)));
  }

  public void deleteProgramHistory(NamespaceId namespaceId) throws IOException {
    getRunRecordsTable().deleteAll(
      Range.singleton(getRunRecordNamespacePrefix(TYPE_RUN_RECORD_ACTIVE, namespaceId)));
    getRunRecordsTable().deleteAll(Range.singleton(
      getRunRecordNamespacePrefix(TYPE_RUN_RECORD_COMPLETED, namespaceId)));
    getProgramCountsTable().deleteAll(Range.singleton(getCountNamespacePrefix(TYPE_COUNT, namespaceId)));
    getProgramCountsTable().deleteAll(Range.singleton(
      getCountNamespacePrefix(TYPE_RUN_RECORD_UPGRADE_COUNT, namespaceId)));
  }

  /**
   * Sets the {@link WorkflowToken} for the given workflow run.
   *
   * @param workflowRunId the {@link ProgramRunId} representing the workflow run
   * @param workflowToken the {@link WorkflowToken} to set to
   */
  public void setWorkflowToken(ProgramRunId workflowRunId, WorkflowToken workflowToken)
    throws IOException {
    if (workflowRunId.getType() != ProgramType.WORKFLOW) {
      throw new IllegalArgumentException("WorkflowToken can only be set for workflow execution: " + workflowRunId);
    }

    List<Field<?>> keys = getProgramRunPrimaryKeys(workflowRunId);
    keys.add(Fields.stringField(StoreDefinition.AppMetadataStore.WORKFLOW_DATA, GSON.toJson(workflowToken)));
    getWorkflowsTable().upsert(keys);
  }

  public WorkflowToken getWorkflowToken(ProgramId workflowId, String workflowRunId)
    throws IOException {
    Preconditions.checkArgument(ProgramType.WORKFLOW == workflowId.getType());
    List<Field<?>> keys = getProgramRunPrimaryKeys(workflowId.run(workflowRunId));
    Optional<StructuredRow> row = getWorkflowsTable().read(keys);

    if (!row.isPresent()) {
      LOG.debug("No workflow token available for workflow: {}, runId: {}", workflowId, workflowRunId);
      // Its ok to not allow any updates by returning a 0 size token.
      return new BasicWorkflowToken(0);
    }

    return GSON.fromJson(row.get().getString(StoreDefinition.AppMetadataStore.WORKFLOW_DATA), BasicWorkflowToken.class);
  }

  /**
   * @return programs that were running between given start and end time and are completed
   */
  public Set<RunId> getRunningInRangeCompleted(long startTimeInSecs, long endTimeInSecs)
    throws IOException {
    // This method scans a large amount of data and may timeout. However, the previous implementation would
    // simply return incomplete data. We have doubled the amount of time each transaction can take by using two
    // transactions - and can further get all namespaces from the smaller app spec table and do one transaction per
    // namespace if necessary.
    return getRunningInRangeForStatus(TYPE_RUN_RECORD_COMPLETED, startTimeInSecs, endTimeInSecs);
  }

  /**
   * @return programs that were running between given start and end time and are active
   */
  public Set<RunId> getRunningInRangeActive(long startTimeInSecs, long endTimeInSecs)
    throws IOException {
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
    Optional<StructuredRow> row = getProgramCountsTable().read(countKey);
    return row.isPresent() ? Objects.firstNonNull(row.get().getLong(StoreDefinition.AppMetadataStore.COUNTS), 0L) : 0L;
  }

  /**
   * Get the run counts of the given program collections.
   *
   * @param programIds the collection of program ids to get the program
   * @return the map of the program id to its run count
   */
  public Map<ProgramId, Long> getProgramRunCounts(Collection<ProgramId> programIds)
    throws BadRequestException, IOException {
    if (programIds.size() > 100) {
      throw new BadRequestException(String.format("%d programs found, the maximum number supported is 100",
                                                  programIds.size()));
    }

    Map<ProgramId, Long> result = programIds.stream()
      .collect(Collectors.toMap(id -> id, id -> 0L, (v1, v2) -> 0L, LinkedHashMap::new));

    List<List<Field<?>>> multiKeys = programIds.stream()
      .map(id -> getProgramCountPrimaryKeys(TYPE_COUNT, id))
      .collect(Collectors.toList());

    for (StructuredRow row : getProgramCountsTable().multiRead(multiKeys)) {
      ProgramId programId = getApplicationIdFromRow(row)
        .program(ProgramType.valueOf(row.getString(StoreDefinition.AppMetadataStore.PROGRAM_TYPE_FIELD)),
                 row.getString(StoreDefinition.AppMetadataStore.PROGRAM_FIELD));
      result.put(programId, row.getLong(StoreDefinition.AppMetadataStore.COUNTS));
    }
    return result;
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
    return getSubscriberStateTable().read(getSubscriberKeys(topic, subscriber))
      .map(row -> row.getString(StoreDefinition.AppMetadataStore.SUBSCRIBER_MESSAGE))
      .orElse(null);
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
    getSubscriberStateTable().upsert(keys);
  }

  /**
   * Deletes the given topic's last fetched message id with the given message id for the given subscriber.
   *
   * @param topic the topic to persist the message id
   * @param subscriber the subscriber name
   */
  public void deleteSubscriberState(String topic, String subscriber) throws IOException {
    getSubscriberStateTable().delete(getSubscriberKeys(topic, subscriber));
  }

  @VisibleForTesting
  Set<RunId> getRunningInRangeForStatus(String statusKey, long startTimeInSecs,
                                        long endTimeInSecs) throws IOException {
    // Create time filter to get running programs between start and end time
    Predicate<RunRecordDetail> timeFilter = (runRecordMeta) ->
      runRecordMeta.getStartTs() < endTimeInSecs &&
        (runRecordMeta.getStopTs() == null || runRecordMeta.getStopTs() >= startTimeInSecs);

    List<Field<?>> prefix = getRunRecordStatusPrefix(statusKey);
    return getRuns(Range.singleton(prefix), ProgramRunStatus.ALL, Integer.MAX_VALUE, null, timeFilter).values().stream()
      .map(RunRecordDetail::getPid)
      .map(RunIds::fromString)
      .collect(Collectors.toSet());
  }

  @VisibleForTesting
  // USE ONLY IN TESTS: WILL DELETE ALL METADATA STORE INFO
  public void deleteAllAppMetadataTables() throws IOException {
    deleteTable(getApplicationSpecificationTable(), StoreDefinition.AppMetadataStore.NAMESPACE_FIELD);
    deleteTable(getWorkflowNodeStateTable(), StoreDefinition.AppMetadataStore.NAMESPACE_FIELD);
    deleteTable(getRunRecordsTable(), StoreDefinition.AppMetadataStore.RUN_STATUS);
    deleteTable(getWorkflowsTable(), StoreDefinition.AppMetadataStore.NAMESPACE_FIELD);
    deleteTable(getProgramCountsTable(), StoreDefinition.AppMetadataStore.COUNT_TYPE);
    deleteTable(getSubscriberStateTable(), StoreDefinition.AppMetadataStore.SUBSCRIBER_TOPIC);
  }

  private void deleteTable(StructuredTable table, String firstKey) throws IOException {
    table.deleteAll(
      Range.from(ImmutableList.of(Fields.stringField(firstKey, SMALLEST_POSSIBLE_STRING)), Range.Bound.INCLUSIVE));
  }

  private List<Field<?>> getSubscriberKeys(String topic, String subscriber) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.SUBSCRIBER_TOPIC, topic));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.SUBSCRIBER, subscriber));
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
    getApplicationSpecificationTable().upsert(fields);
  }

  private List<Field<?>> getCountTypePrefix(String countType) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.COUNT_TYPE, countType));
    return fields;
  }

  private List<Field<?>> getCountNamespacePrefix(String countType, NamespaceId namespaceId) {
    List<Field<?>> fields = getCountTypePrefix(countType);
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, namespaceId.getNamespace()));
    return fields;
  }

  private List<Field<?>> getCountApplicationPrefix(String countType, ApplicationId applicationId) {
    List<Field<?>> fields = getCountTypePrefix(countType);
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, applicationId.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_FIELD, applicationId.getApplication()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.VERSION_FIELD, applicationId.getVersion()));
    return fields;
  }

  // Do NOT use with type = RunRecordDetail since that needs custom deserialization {@link deserializeRunRecordMeta}
  private <T> List<T> scanWithRange(Range range, Type typeofT, StructuredTable table, String field)
    throws IOException {
    List<T> result = new ArrayList<>();
    try (CloseableIterator<StructuredRow> iterator = table.scan(range, Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        result.add(
          GSON.fromJson(iterator.next().getString(field), typeofT));
      }
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

  private static RunRecordDetail deserializeRunRecordMeta(StructuredRow row) {
    RunRecordDetail existing =
      GSON.fromJson(row.getString(StoreDefinition.AppMetadataStore.RUN_RECORD_DATA), RunRecordDetail.class);
    return RunRecordDetail.builder(existing)
      .setProgramRunId(
        getProgramIdFromRunRecordsPrimaryKeys(new ArrayList<>(row.getPrimaryKeys())).run(existing.getPid()))
      .build();
  }

  private static ProgramId getProgramIdFromRunRecordsPrimaryKeys(List<Field<?>> primaryKeys) {
    // Assume keys are in correct ordering - skip first field since it's run_status
    return new ApplicationId(getStringFromField(primaryKeys.get(1)), getStringFromField(primaryKeys.get(2)),
                             getStringFromField(primaryKeys.get(3)))
      .program(ProgramType.valueOf(getStringFromField(primaryKeys.get(4))), getStringFromField(primaryKeys.get(5)));
  }

  private static String getStringFromField(Field<?> field) {
    return (String) field.getValue();
  }

  private List<Field<?>> addProgramPrimaryKeys(ProgramId programRunId, List<Field<?>> fields) {
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, programRunId.getNamespace()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_FIELD, programRunId.getApplication()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.VERSION_FIELD, programRunId.getVersion()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_TYPE_FIELD, programRunId.getType().name()));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.PROGRAM_FIELD, programRunId.getProgram()));
    return fields;
  }

  private List<Field<?>> getProgramRunPrimaryKeys(ProgramRunId programRunId) {
    List<Field<?>> fields = addProgramPrimaryKeys(programRunId.getParent(), new ArrayList<>());
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_FIELD, programRunId.getRun()));
    return fields;
  }

  private List<Field<?>> getWorkflowPrimaryKeys(ProgramRunId programRunId, String nodeId) {
    List<Field<?>> fields = getProgramRunPrimaryKeys(programRunId);
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NODE_ID, nodeId));
    return fields;
  }

  private List<Field<?>> getProgramRunInvertedTimeKey(String recordType, ProgramRunId runId, long startTs) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_STATUS, recordType));
    addProgramPrimaryKeys(runId.getParent(), fields);
    fields.add(Fields.longField(StoreDefinition.AppMetadataStore.RUN_START_TIME, getInvertedTsKeyPart(startTs)));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.RUN_FIELD, runId.getRun()));
    return fields;
  }

  private List<Field<?>> getProgramCountPrimaryKeys(String type, ProgramId programId) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.COUNT_TYPE, type));
    return addProgramPrimaryKeys(programId, fields);
  }

  private ApplicationId getApplicationIdFromRow(StructuredRow row) {
    return new NamespaceId(row.getString(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD))
      .app(row.getString(StoreDefinition.AppMetadataStore.APPLICATION_FIELD),
           row.getString(StoreDefinition.AppMetadataStore.VERSION_FIELD));
  }

  @Nullable
  private Predicate<StructuredRow> getKeyFilterByTimeRange(long startTime, long endTime) {
    if (startTime <= 0 && endTime == Long.MAX_VALUE) {
      return null;
    }
    long lowerBound = getInvertedTsScanKeyPart(endTime);
    long upperBound = getInvertedTsScanKeyPart(startTime);
    return row -> {
      Long time = row.getLong(StoreDefinition.AppMetadataStore.RUN_START_TIME);
      return time != null && time >= lowerBound && time <= upperBound;
    };
  }
}

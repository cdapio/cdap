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

package io.cdap.cdap.app.store;

import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.worker.Worker;
import io.cdap.cdap.api.workflow.Workflow;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.app.store.WorkflowTable;
import io.cdap.cdap.internal.app.store.state.AppStateKey;
import io.cdap.cdap.internal.app.store.state.AppStateKeyValue;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.ProgramHistory;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunCountResult;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.WorkflowStatistics;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.twill.api.RunId;

/**
 * Responsible for managing {@link Program} and {@link Application} metadata.
 * TODO: set state methods are only called from unit tests. They should be removed in favor of using AppMetadataStore.
 */
public interface Store {

  /**
   * Logs initialization of program run and persists program cluster status to
   * {@link ProgramRunClusterStatus#PROVISIONING}.
   *
   * @param id run id of the program
   * @param runtimeArgs the runtime arguments for this program run
   * @param systemArgs the system arguments for this program run
   * @param sourceId id of the source of program run status, which is proportional to the timestamp of
   *                 when the current program run status is reached
   * @param artifactId artifact id used to create the application the program belongs to
   */
  void setProvisioning(ProgramRunId id, Map<String, String> runtimeArgs,
                       Map<String, String> systemArgs, byte[] sourceId, ArtifactId artifactId);

  /**
   * Persists program run cluster status to {@link ProgramRunClusterStatus#PROVISIONED}.
   *
   * @param id run id of the program
   * @param numNodes number of nodes for the cluster
   * @param sourceId id of the source of program run status, which is proportional to the timestamp of
   *                 when the current program run status is reached
   */
  void setProvisioned(ProgramRunId id, int numNodes, byte[] sourceId);

  /**
   * Logs initialization of program run and persists program status to {@link ProgramRunStatus#STARTING}.
   *
   * @param id run id of the program
   * @param twillRunId Twill run id
   * @param systemArgs the system arguments for this program run
   * @param sourceId id of the source of program run status, which is proportional to the timestamp of
   *                 when the current program run status is reached
   */
  void setStart(ProgramRunId id, @Nullable String twillRunId, Map<String, String> systemArgs, byte[] sourceId);

  /**
   * Logs start of program run and persists program status to {@link ProgramRunStatus#RUNNING}.
   *
   * @param id run id of the program
   * @param twillRunId Twill run id
   * @param runTime start timestamp in seconds
   * @param sourceId id of the source of program run status, which is proportional to the timestamp of
   *                 when the current program run status is reached
   */
  void setRunning(ProgramRunId id, long runTime, @Nullable String twillRunId, byte[] sourceId);

  /**
   * Logs stopping of a program run and persists program status to {@link ProgramRunStatus#STOPPING}.
   *
   * @param id run id of the program
   * @param sourceId id of the source of program run status, which is proportional to the timestamp of
   *                 when the current program run status is reached
   * @param stoppingTime stopping timestamp in seconds
   * @param terminateTs timestamp at which program should be in an end state calculated using graceful shutdown period
   */
  void setStopping(ProgramRunId id, byte[] sourceId, long stoppingTime, long terminateTs);

  /**
   * Logs end of program run and sets the run status to one of: {@link ProgramRunStatus#COMPLETED},
   * or {@link ProgramRunStatus#KILLED}.
   *
   * @param id run id of the program
   * @param endTime end timestamp in seconds
   * @param runStatus {@link ProgramRunStatus} of program run
   * @param sourceId id of the source of program run status, which is proportional to the timestamp of
   *                 when the current program run status is reached
   */
  void setStop(ProgramRunId id, long endTime, ProgramRunStatus runStatus, byte[] sourceId);

  /**
   * Logs end of program run and sets the run status to {@link ProgramRunStatus#FAILED} with a failure cause.
   *
   * @param id run id of the program
   * @param endTime end timestamp in seconds
   * @param runStatus {@link ProgramRunStatus} of program run
   * @param failureCause failure cause if the program failed to execute
   * @param sourceId id of the source of program run status, which is proportional to the timestamp of
   *                 when the current program run status is reached
   */
  void setStop(ProgramRunId id, long endTime, ProgramRunStatus runStatus,
               @Nullable BasicThrowable failureCause, byte[] sourceId);

  /**
   * Logs suspend of a program run and sets the run status to {@link ProgramRunStatus#SUSPENDED}.
   *
   * @param id run id of the program
   * @param sourceId id of the source of program run status, which is proportional to the timestamp of
   *                 when the current program run status is reached
   * @param suspendTime time when the program is suspended
   */
  void setSuspend(ProgramRunId id, byte[] sourceId, long suspendTime);

  /**
   * Logs resume of a program run and sets the run status to {@link ProgramRunStatus#RUNNING}.
   *
   * @param id run id of the program
   * @param sourceId id of the source of program run status, which is proportional to the timestamp of
   *                 when the current program run status is reached
   * @param resumeTime time when the program is resumed from suspended state
   */
  void setResume(ProgramRunId id, byte[] sourceId, long resumeTime);

  /**
   * Loads a given program.
   *
   * @param program id of the program
   * @return An instance of {@link ProgramDescriptor} if found.
   * @throws IOException when Store fails to get application
   */
  ProgramDescriptor loadProgram(ProgramId program) throws IOException, NotFoundException;

  /**
   * Loads a given versionless program.
   *
   * @param programReference reference of the program
   * @return An instance of {@link ProgramDescriptor} if found.
   * @throws IOException when Store fails to get application
   */
  ProgramDescriptor loadProgram(ProgramReference programReference) throws IOException, NotFoundException;

  /**
   * Fetches all run records for all versions of a program.
   * Returned ProgramRunRecords are sorted by their startTime.
   *
   * @param programReference programReference of the program
   * @param status           status of the program running/completed/failed or all
   * @param startTime        fetch run history that has started after the startTime in seconds
   * @param endTime          fetch run history that has started before the endTime in seconds
   * @param limit            max number of entries to fetch for this history call
   * @return                 map of logged runs
   */
  Map<ProgramRunId, RunRecordDetail> getAllRuns(ProgramReference programReference, ProgramRunStatus status,
      long startTime, long endTime, int limit,
      Predicate<RunRecordDetail> filter);

  /**
   * Fetches run records for particular program.
   * Returned ProgramRunRecords are sorted by their startTime.
   *
   * @param id        id of the program
   * @param status    status of the program running/completed/failed or all
   * @param startTime fetch run history that has started after the startTime in seconds
   * @param endTime   fetch run history that has started before the endTime in seconds
   * @param limit     max number of entries to fetch for this history call
   * @return          map of logged runs
   */
  Map<ProgramRunId, RunRecordDetail> getRuns(ProgramId id, ProgramRunStatus status,
                                             long startTime, long endTime, int limit);

  /**
   * Fetches the run records for the particular status. Same as calling
   * {@link #getRuns(ProgramRunStatus, long, long, int, Predicate)
   * getRuns(status, 0, Long.MAX_VALUE, Integer.MAX_VALUE, filter)}
   */
  Map<ProgramRunId, RunRecordDetail> getRuns(ProgramRunStatus status, Predicate<RunRecordDetail> filter);

  /**
   * Fetches run records for the particular status.
   *
   * @param status status of the program to filter the records
   * @param startTime fetch run history that has started after the startTime in seconds
   * @param endTime fetch run history that has started before the endTime in seconds
   * @param limit max number of entries to fetch for this history call
   * @param filter predicate to be passed to filter the records
   * @return a map from {@link ProgramRunId} to the corresponding {@link RunRecordDetail}.
   */
  Map<ProgramRunId, RunRecordDetail> getRuns(ProgramRunStatus status, long startTime,
                                             long endTime, int limit, Predicate<RunRecordDetail> filter);

  /**
   * Fetches run records for the particular status.
   *
   * @param applicationId to filter the records
   * @param status status of the program to filter the records
   * @param limit max number of entries to fetch for this history call
   * @param filter predicate to be passed to filter the records
   * @return a map from {@link ProgramRunId} to the corresponding {@link RunRecordDetail}.
   */
  Map<ProgramRunId, RunRecordDetail> getRuns(ApplicationId applicationId, ProgramRunStatus status,
                                             int limit, Predicate<RunRecordDetail> filter);

  /**
   * Fetches the run records for given ProgramRunIds.
   *
   * @param programRunIds  list of program RunIds to match against
   * @return        map of logged runs
   */
  Map<ProgramRunId, RunRecordDetail> getRuns(Set<ProgramRunId> programRunIds);

  /**
   * Fetches run records for multiple programs.
   *
   * @param programs  the programs to get run records for
   * @param status    status of the program running/completed/failed or all
   * @param startTime fetch run history that has started after the startTime in seconds
   * @param endTime   fetch run history that has started before the endTime in seconds
   * @param limitPerProgram     max number of runs to fetch for each program
   * @return          runs for each program
   */
  List<ProgramHistory> getRuns(Collection<ProgramReference> programs, ProgramRunStatus status,
      long startTime, long endTime, int limitPerProgram);

  /**
   * Fetches the active (i.e STARTING or RUNNING or SUSPENDED) run records across all namespaces.
   *
   * @param limit count at most that many runs, stop if there are more.
   * @return map of logged runs
   */
  int countActiveRuns(@Nullable Integer limit);

  /**
   * Scans for active (i.e STARTING or RUNNING or SUSPENDED) run records
   *
   * @param txBatchSize maximum number of applications to scan in one transaction to
   *                    prevent holding a single transaction for too long
   * @param consumer a {@link Consumer} to consume each application being scanned
   */
  void scanActiveRuns(int txBatchSize, Consumer<RunRecordDetail> consumer);

  /**
   * Fetches the active (i.e STARTING or RUNNING or SUSPENDED) run records against a given NamespaceId.
   *
   * @param namespaceId the namespace id to match against
   * @return map of logged runs
   */
  Map<ProgramRunId, RunRecordDetail> getActiveRuns(NamespaceId namespaceId);

  /**
   * Fetches the active (i.e STARTING or RUNNING or SUSPENDED) run records from the given NamespaceId's.
   *
   * @param namespaces the namespace id's to get active run records from
   * @param filter predicate to be passed to filter the records
   * @return map of logged runs
   */
  Map<ProgramRunId, RunRecordDetail> getActiveRuns(Set<NamespaceId> namespaces, Predicate<RunRecordDetail> filter);

  /**
   * Fetches the active (i.e STARTING or RUNNING or SUSPENDED) run records against a given ApplicationId.
   *
   * @param applicationId the application id to match against
   * @return map of logged runs
   */
  Map<ProgramRunId, RunRecordDetail> getActiveRuns(ApplicationId applicationId);

  /**
   * Fetches the active (i.e STARTING or RUNNING or SUSPENDED) run records against a given ProgramId.
   *
   * @param programId the program id to match against
   * @return map of logged runs
   */
  Map<ProgramRunId, RunRecordDetail> getActiveRuns(ProgramId programId);

  /**
   * Fetches the latest active runs for a set of programs.
   *
   * @param programRefs collection of program ids for fetching active run records.
   * @return a {@link Map} from the {@link ProgramId} to the list of run records; there will be no entry for programs
   *     that do not exist.
   */
  Map<ProgramId, Collection<RunRecordDetail>> getActiveRuns(Collection<ProgramReference> programRefs);

  /**
   * Fetches the active (i.e STARTING or RUNNING or SUSPENDED) run records for the
   * given application.
   *
   * @param applicationReference versionless reference of the application
   * @return map of logged runs. If no active run exists, return an empty map.
   */
  Map<ProgramRunId, RunRecordDetail> getAllActiveRuns(ApplicationReference applicationReference);

  /**
   * Fetches the run record for particular run of a program.
   *
   * @param id        run id of the program
   * @return          run record for the specified program and runid, null if not found
   */
  @Nullable
  RunRecordDetail getRun(ProgramRunId id);

  /**
   * Fetches the run record for particular run of a program without version.
   *
   * @param programRef    versionless program id of the run
   * @param runId         the run id
   * @return          run record for the specified program and runRef, null if not found
   */
  @Nullable
  RunRecordDetail getRun(ProgramReference programRef, String runId);

  /**
   * Creates new application if it doesn't exist. Updates existing one otherwise.
   * Always marks the added application as latest.
   *
   * @param id            application id
   * @param meta          application metadata to store
   * @return              the number of edits to the application. A new application will return 0.
   * @throws ConflictException if the app cannot be deployed when the user provided parent-version doesn't match the
   *     current latest version
   */
  int addLatestApplication(ApplicationId id, ApplicationMeta meta) throws ConflictException;

  /**
   * Creates new application if it doesn't exist. Updates existing one otherwise.
   * Marks the application as latest based on the isLatest param.
   *
   * @param id            application id
   * @param meta          application metadata to store
   * @param isLatest      boolean, indicating if the application should be marked latest
   * @return              the number of edits to the application. A new application will return 0.
   * @throws ConflictException if the app cannot be deployed when the user provided parent-version doesn't match the
   *     current latest version
   */
  int addApplication(ApplicationId id, ApplicationMeta meta, boolean isLatest) throws ConflictException;

  /**
   * Marks existing applications as latest.
   *
   * @param applicationIds List of application ids
   * @throws IOException if the apps cannot be marked latest because of any IO failure
   * @throws ApplicationNotFoundException when any of the applications is not found
   */
  void markApplicationsLatest(Collection<ApplicationId> applicationIds)
      throws IOException, ApplicationNotFoundException;

  /**
   * Return a list of program specifications that are deleted comparing the specification in the store with the
   * spec that is passed.
   *
   * @param appRef               ApplicationReference
   * @param specification        Application specification
   * @return                     List of ProgramSpecifications that are deleted
   */
  List<ProgramSpecification> getDeletedProgramSpecifications(ApplicationReference appRef,
                                                             ApplicationSpecification specification);

  /**
   * Updates source control metadata for one or more applications.
   * If any of the applications in the given collection are not found, this method ignores them.
   *
   * @param updateRequests Map of {@link ApplicationId} to {@link SourceControlMeta}
   * @throws IOException if scm meta update fails
   */
  void updateApplicationSourceControlMeta(Map<ApplicationId, SourceControlMeta> updateRequests)
      throws IOException;

  /**
   * Returns application specification by id.
   *
   * @param id application id
   * @return application specification
   */
  @Nullable
  ApplicationSpecification getApplication(ApplicationId id);

  /**
   * Returns application metadata information by id.
   *
   * @param id application id
   * @return application metadata
   */
  @Nullable
  ApplicationMeta getApplicationMetadata(ApplicationId id);

  /**
   * Returns the latest version of an application in a namespace.
   *
   * @param appRef application reference
   * @return The metadata information of the latest application version.
   */
  @Nullable
  ApplicationMeta getLatest(ApplicationReference appRef);

  /**
   * Scans for the latest applications across all namespaces.
   *
   * @param txBatchSize maximum number of applications to scan in one transaction to
   *                    prevent holding a single transaction for too long
   * @param consumer a {@link BiConsumer} to consume each application being scanned
   */
  void scanApplications(int txBatchSize, BiConsumer<ApplicationId, ApplicationMeta> consumer);

  /**
   * Scans for applications according to the parameters passed in request.
   *
   * @param request  parameters defining filters and sorting
   * @param txBatchSize maximum number of applications to scan in one transaction to
   *                    prevent holding a single transaction for too long
   * @param consumer a {@link BiConsumer} to consume each application being scanned
   * @return if limit was reached (true) or all items were scanned before reaching the limit (false)
   */
  boolean scanApplications(ScanApplicationsRequest request, int txBatchSize,
                           BiConsumer<ApplicationId, ApplicationMeta> consumer);

  /**
   * Returns a Map of {@link ApplicationMeta} for the given set of {@link ApplicationId}.
   *
   * @param ids the list of application ids to get the specs
   * @return collection of application metas. For applications that don't exist, there will be no entry in the result.
   */
  Map<ApplicationId, ApplicationMeta> getApplications(Collection<ApplicationId> ids);

  /**
   * Update an applications with provided SourceControlMeta.
   *
   * @param appId the application ID
   * @param sourceControlMeta the source control metadata of the application synced with linked repository.
   */
  void setAppSourceControlMeta(ApplicationId appId, SourceControlMeta sourceControlMeta);

  /**
   * Get source control metadata of provided application.
   *
   * @param appRef the application reference
   * @return {@link SourceControlMeta}
   */
  SourceControlMeta getAppSourceControlMeta(ApplicationReference appRef);

  /**
   * Returns a map of latest programIds given programReferences.
   *
   * @param references the list of programReferences to get the latest programIds
   * @return collection of programIds. For applications that don't exist, there will be no entry in the result.
   */
  Map<ProgramReference, ProgramId> getPrograms(Collection<ProgramReference> references);

  /**
   * Returns a list of all versions' ApplicationId's of the application by reference.
   *
   * @param appRef application reference
   * @return collection of versionIds of the application's versions
   */
  Collection<ApplicationId> getAllAppVersionsAppIds(ApplicationReference appRef);

  /**
   * Sets the number of instances of a service.
   *
   * @param id id of the program
   * @param instances number of instances
   */
  void setServiceInstances(ProgramId id, int instances);

  /**
   * Returns the number of instances of a service.
   *
   * @param id id of the program
   * @return number of instances
   */
  int getServiceInstances(ProgramId id);

  /**
   * Sets the number of instances of a {@link Worker}.
   *
   * @param id id of the program
   * @param instances number of instances
   */
  void setWorkerInstances(ProgramId id, int instances);

  /**
   * Gets the number of instances of a {@link Worker}.
   *
   * @param id id of the program
   * @return number of instances
   */
  int getWorkerInstances(ProgramId id);

  /**
   * Removes all program under the given application and also the application itself.
   *
   * @param appRef Application reference
   */
  void removeApplication(ApplicationReference appRef);

  /**
   * Removes all program under the given application and also the application itself.
   *
   * @param appId Application Id
   */
  void removeApplication(ApplicationId appId);

  /**
   * Remove all metadata associated with the given namespace.
   *
   * @param id namespace id whose items to remove
   */
  void removeAll(NamespaceId id);

  /**
   * Get run time arguments for a program.
   *
   * @param runId id of the program run
   * @return Map of key, value pairs
   */
  Map<String, String> getRuntimeArguments(ProgramRunId runId);

  /**
   * Deletes data for an application from the WorkflowTable table.
   *
   * @param id id of application to be deleted
   */
  void deleteWorkflowStats(ApplicationId id);

  /**
   * Retrieves the {@link WorkflowToken} for a specified run of a workflow.
   *
   * @param workflowId {@link WorkflowId} of the workflow whose {@link WorkflowToken} is to be retrieved
   * @param workflowRunId Run Id of the workflow for which the {@link WorkflowToken} is to be retrieved
   * @return the {@link WorkflowToken} for the specified workflow run
   */
  WorkflowToken getWorkflowToken(WorkflowId workflowId, String workflowRunId);

  /**
   * Get the node states for a given {@link Workflow} run.
   *
   * @param workflowRunId run of the Workflow.
   * @return {@link List} of {@link WorkflowNodeStateDetail}
   */
  List<WorkflowNodeStateDetail> getWorkflowNodeStates(ProgramRunId workflowRunId);

  /**
   * Used by {@link io.cdap.cdap.gateway.handlers.WorkflowStatsSLAHttpHandler} to get the statistics of all completed
   * workflows in a time range.
   *
   * @param namespaceId The namespace that the workflow belongs to
   * @param appName The application that the workflow belongs to
   * @param workflowName The name of the workflow
   * @param startTime StartTime of the range
   * @param endTime EndTime of the range
   * @param percentiles List of percentiles that the user wants to see
   * @return the statistics for a given workflow
   */
  WorkflowStatistics getWorkflowStatistics(NamespaceId namespaceId, String appName, String workflowName, long startTime,
                                           long endTime, List<Double> percentiles);

  /**
   * Returns the record that represents the run of a workflow.
   *
   * @param workflowId The Workflow whose run needs to be queried
   * @param runId RunId of the workflow run
   * @return A workflow run record corresponding to the runId
   */
  WorkflowTable.WorkflowRunRecord getWorkflowRun(WorkflowId workflowId, String runId);

  /**
   * Returns the record that represents the run of a workflow.
   *
   * @param namespaceId The namespace that the workflow belongs to
   * @param appName The application that the workflow belongs to
   * @param workflowName The Workflow whose run needs to be queried
   * @param runId RunId of the workflow run
   * @return A workflow run record corresponding to the runId
   */
  WorkflowTable.WorkflowRunRecord getWorkflowRun(NamespaceId namespaceId, String appName, String workflowName,
                                                 String runId);

  /**
   * Get a list of workflow runs that are spaced apart by time interval in both directions from the run id provided.
   *
   * @param namespaceId The namespace that the workflow belongs to
   * @param appName The application that the workflow belongs to
   * @param workflowName The name of the workflow
   * @param runId The run id of the workflow
   * @param limit The number of the records that the user wants to compare against on either side of the run
   * @param timeInterval The timeInterval with which the user wants to space out the runs
   * @return Map of runId of Workflow to DetailedStatistics of the run
   */
  Collection<WorkflowTable.WorkflowRunRecord> retrieveSpacedRecords(NamespaceId namespaceId, String appName,
                                                                    String workflowName,  String runId, int limit,
                                                                    long timeInterval);

  /**
   * Get the running Ids in a time range.
   *
   * @return programs that were running between given start and end time.
   */
  Set<RunId> getRunningInRange(long startTimeInSecs, long endTimeInSecs);

  /**
   * Get the run count of the given program.
   *
   * @param programId the program to get the count
   * @return the number of run count
   * @throws NotFoundException if the app or the program does not exist
   */
  long getProgramRunCount(ProgramId programId) throws NotFoundException;

  /**
   * Get the run count of the given program across all versions.
   *
   * @param programReference the program to get the count
   * @return the number of run count
   * @throws NotFoundException if the app or the program does not exist
   */
  long getProgramTotalRunCount(ProgramReference programReference) throws NotFoundException;

  /**
   * Get the run count of the given program collection.
   *
   * @param programRefs collection of program ids to get the count
   * @return the run count result of each program in the collection
   */
  List<RunCountResult> getProgramTotalRunCounts(Collection<ProgramReference> programRefs);

  /**
   * Get application state.
   *
   * @param request a {@link AppStateKey} object.
   * @return state of application
   * @throws ApplicationNotFoundException if application with request.appName is not found.
   */
  Optional<byte[]> getState(AppStateKey request) throws ApplicationNotFoundException;

  /**
   * Save application state.
   *
   * @param request a {@link AppStateKeyValue} object.
   * @throws ApplicationNotFoundException if application with request.appName is not found.
   */
  void saveState(AppStateKeyValue request) throws ApplicationNotFoundException;

  /**
   * Delete application state.
   *
   * @param request a {@link AppStateKey} object.
   * @throws ApplicationNotFoundException if application with request.appName is not found.
   */
  void deleteState(AppStateKey request) throws ApplicationNotFoundException;

  /**
   * Delete all states related to an application.
   *
   * @param namespaceId NamespaceId of the application.
   * @param appName AppName of the application.
   * @throws ApplicationNotFoundException if application with appName is not found.
   */
  void deleteAllStates(NamespaceId namespaceId, String appName) throws ApplicationNotFoundException;

  /**
   * Ensures the given program exists in the given application spec.
   *
   * @throws NotFoundException if the program does not exists.
   */
  static void ensureProgramExists(ProgramId programId,
                                  @Nullable ApplicationSpecification appSpec) throws NotFoundException {
    if (appSpec == null) {
      throw new ApplicationNotFoundException(programId.getParent());
    }
    if (!Objects.equals(programId.getApplication(), appSpec.getName())
      || !Objects.equals(programId.getVersion(), appSpec.getAppVersion())) {
      throw new IllegalArgumentException("Program " + programId + " does not belong to application "
          + appSpec.getName() + ":" + appSpec.getAppVersion());
    }

    if (!appSpec.getProgramsByType(programId.getType().getApiProgramType()).contains(programId.getProgram())) {
      throw new ProgramNotFoundException(programId);
    }
  }
}

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

package co.cask.cdap.app.store;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.internal.app.store.WorkflowDataset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.WorkflowStatistics;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * {@link Store} operates on a {@link Program}. It's responsible
 * for managing the non-runtime lifecycle of a {@link Program}
 */
public interface Store {

  /**
   * Compare and set operation that allow to compare and set expected and update status.
   * Implementation of this method should guarantee that the operation is atomic or in transaction.
   *
   * @param id              Info about program
   * @param pid             The run id
   * @param expectedStatus  The expected value
   * @param updateStatus    The new value
   */
  void compareAndSetStatus(Id.Program id, String pid, ProgramRunStatus expectedStatus, ProgramRunStatus updateStatus);

  /**
   * Loads a given program.
   *
   * @param program id of the program
   * @return An instance of {@link co.cask.cdap.app.program.DefaultProgram} if found.
   * @throws IOException
   */
  Program loadProgram(Id.Program program) throws IOException, ApplicationNotFoundException, ProgramNotFoundException;

  /**
   * Logs start of program run.
   *
   * @param id         Info about program
   * @param pid        run id
   * @param startTime  start timestamp in seconds; if run id is time-based pass the time from the run id
   * @param twillRunId twill run id
   * @param runtimeArgs the runtime arguments for this program run
   * @param systemArgs the system arguments for this program run
   */
  void setStart(Id.Program id, String pid, long startTime, @Nullable String twillRunId,
                Map<String, String> runtimeArgs, Map<String, String> systemArgs);

  /**
   * Logs start of program run. This is a convenience method for testing, actual run starts should be recorded using
   * {@link #setStart(Id.Program, String, long, String, Map, Map)}.
   *
   * @param id        Info about program
   * @param pid       run id
   * @param startTime start timestamp in seconds; if run id is time-based pass the time from the run id
   */
  @VisibleForTesting
  void setStart(Id.Program id, String pid, long startTime);

  /**
   * Logs end of program run.
   *
   * @param id      id of program
   * @param pid     run id
   * @param endTime end timestamp in seconds
   * @param runStatus   {@link ProgramRunStatus} of program run
   */
  void setStop(Id.Program id, String pid, long endTime, ProgramRunStatus runStatus);

  /**
   * Logs end of program run.
   *
   * @param id      id of program
   * @param pid     run id
   * @param endTime end timestamp in seconds
   * @param runStatus   {@link ProgramRunStatus} of program run
   * @param failureCause failure cause if the program failed to execute
   */
  void setStop(Id.Program id, String pid, long endTime, ProgramRunStatus runStatus, @Nullable Throwable failureCause);

  /**
   * Logs suspend of a program run.
   * @param id      id of the program
   * @param pid     run id
   */
  void setSuspend(Id.Program id, String pid);

  /**
   * Logs resume of a program run.
   * @param id      id of the program
   * @param pid     run id
   */
  void setResume(Id.Program id, String pid);

  /**
   * Fetches run records for particular program. Returns only finished runs.
   * Returned ProgramRunRecords are sorted by their startTime.
   *
   * @param id        program id.
   * @param status    status of the program running/completed/failed or all
   * @param startTime fetch run history that has started after the startTime in seconds
   * @param endTime   fetch run history that has started before the endTime in seconds
   * @param limit     max number of entries to fetch for this history call
   * @return          list of logged runs
   */
  List<RunRecordMeta> getRuns(Id.Program id, ProgramRunStatus status, long startTime, long endTime, int limit);

  /**
   * Fetches run records for particular program. Returns only finished runs.
   * Returned ProgramRunRecords are sorted by their startTime.
   *
   * @param id        program id.
   * @param status    status of the program running/completed/failed or all
   * @param startTime fetch run history that has started after the startTime in seconds
   * @param endTime   fetch run history that has started before the endTime in seconds
   * @param limit     max number of entries to fetch for this history call
   * @param filter    predicate to be passed to filter the records
   * @return          list of logged runs
   */
  List<RunRecordMeta> getRuns(Id.Program id, ProgramRunStatus status, long startTime, long endTime, int limit,
                              Predicate<RunRecordMeta> filter);

  /**
   * Fetches the run records for the particular status.
   * @param status  status of the program running/completed/failed or all
   * @param filter  predicate to be passed to filter the records
   * @return        list of logged runs
   */
  List<RunRecordMeta> getRuns(ProgramRunStatus status, Predicate<RunRecordMeta> filter);

  /**
   * Fetches the run record for particular run of a program.
   *
   * @param id        program id
   * @param runid     run id of the program
   * @return          run record for the specified program and runid, null if not found
   */
  @Nullable
  RunRecordMeta getRun(Id.Program id, String runid);

  /**
   * Creates a new stream if it does not exist.
   * @param id the namespace id
   * @param stream the stream to create
   */
  void addStream(Id.Namespace id, StreamSpecification stream);

  /**
   * Get the spec of a named stream.
   * @param id the namespace id
   * @param name the name of the stream
   */
  StreamSpecification getStream(Id.Namespace id, String name);

  /**
   * Get the specs of all streams for a namespace.
   *
   * @param id the namespace id
   */

  Collection<StreamSpecification> getAllStreams(Id.Namespace id);

  /**
   * Creates new application if it doesn't exist. Updates existing one otherwise.
   *
   * @param id            application id
   * @param specification application specification to store
   * @param appArchiveLocation location of the deployed app archive
   */
  void addApplication(Id.Application id,
                      ApplicationSpecification specification, Location appArchiveLocation);


  /**
   * Return a list of program specifications that are deleted comparing the specification in the store with the
   * spec that is passed.
   *
   * @param id                   ApplicationId
   * @param specification        Application specification
   * @return                     List of ProgramSpecifications that are deleted
   */
  List<ProgramSpecification> getDeletedProgramSpecifications(Id.Application id,
                                                             ApplicationSpecification specification);

  /**
   * Returns application specification by id.
   *
   * @param id application id
   * @return application specification
   */
  @Nullable
  ApplicationSpecification getApplication(Id.Application id);

  /**
   * Returns a collection of all application specs in the specified namespace
   *
   * @param id the namespace to get application specs from
   * @return collection of all application specs in the namespace
   */
  Collection<ApplicationSpecification> getAllApplications(Id.Namespace id);

  /**
   * Returns location of the application archive.
   *
   * @param id application id
   * @return application archive location
   */
  @Nullable
  Location getApplicationArchiveLocation(Id.Application id);

  /**
   * Sets number of instances of specific flowlet.
   *
   * @param id flow id
   * @param flowletId flowlet id
   * @param count new number of instances
   * @return The {@link FlowSpecification} before the instance change
   */
  FlowSpecification setFlowletInstances(Id.Program id, String flowletId, int count);

  /**
   * Gets number of instances of specific flowlet.
   *
   * @param id flow id
   * @param flowletId flowlet id
   */
  int getFlowletInstances(Id.Program id, String flowletId);

  /**
   * Sets the number of instances of a service.
   *
   * @param id program id
   * @param instances number of instances
   */
  void setServiceInstances(Id.Program id, int instances);

  /**
   * Returns the number of instances of a service.
   * @param id program id
   * @return number of instances
   */
  int getServiceInstances(Id.Program id);

  /**
   * Sets the number of instances of a {@link Worker}
   *
   * @param id program id
   * @param instances number of instances
   */
  void setWorkerInstances(Id.Program id, int instances);

  /**
   * Gets the number of instances of a {@link Worker}
   *
   * @param id program id
   * @return number of instances
   */
  int getWorkerInstances(Id.Program id);

  /**
   * Removes all program under the given application and also the application itself.
   *
   * @param id Application id
   */
  void removeApplication(Id.Application id);

  /**
   * Removes all applications (with programs) associated with the given namespace.
   *
   * @param id namespace id whose applications to remove
   */
  void removeAllApplications(Id.Namespace id);

  /**
   * Remove all metadata associated with the given namespace.
   *
   * @param id namespace id whose items to remove
   */
  void removeAll(Id.Namespace id);

  /**
   * Get run time arguments for a program.
   *
   * @param runId id of the program run
   * @return Map of key, value pairs
   */
  Map<String, String> getRuntimeArguments(Id.Run runId);

  /**
   * Adds a schedule for a particular program. If the schedule with the name already exists, the method will
   * throw RuntimeException.
   * @param program defines program to which a schedule is being added
   * @param scheduleSpecification defines the schedule to be added for the program
   */
  void addSchedule(Id.Program program, ScheduleSpecification scheduleSpecification);

  /**
   * Deletes a schedules from a particular program
   * @param program defines program from which a schedule is being deleted
   * @param scheduleName the name of the schedule to be removed from the program
   */
  void deleteSchedule(Id.Program program, String scheduleName);

  /**
   * Check if an application exists.
   * @param id id of application.
   * @return true if the application exists, false otherwise.
   */
  boolean applicationExists(Id.Application id);

  /**
   * Check if a program exists.
   * @param id id of program.
   * @return true if the program exists, false otherwise.
   */
  boolean programExists(Id.Program id);

  /**
   * Updates the {@link WorkflowToken} for a specified run of a workflow.
   *
   * @param workflowRunId workflow run for which the {@link WorkflowToken} is to be updated
   * @param token the {@link WorkflowToken} to update
   */
  void updateWorkflowToken(ProgramRunId workflowRunId, WorkflowToken token);

  /**
   * Retrieves the {@link WorkflowToken} for a specified run of a workflow.
   *
   * @param workflowId {@link Id.Workflow} of the workflow whose {@link WorkflowToken} is to be retrieved
   * @param workflowRunId Run Id of the workflow for which the {@link WorkflowToken} is to be retrieved
   * @return the {@link WorkflowToken} for the specified workflow run
   */
  WorkflowToken getWorkflowToken(Id.Workflow workflowId, String workflowRunId);

  /**
   * Add node state for the given {@link Workflow} run. This method is used to update the
   * state of the custom actions started by Workflow.
   * @param workflowRunId the Workflow run
   * @param nodeStateDetail the node state to be added for the Workflow run
   */
  void addWorkflowNodeState(ProgramRunId workflowRunId, WorkflowNodeStateDetail nodeStateDetail);

  /**
   * Get the node states for a given {@link Workflow} run.
   *
   * @param workflowRunId run of the Workflow.
   * @return {@link List} of {@link WorkflowNodeStateDetail}
   */
  List<WorkflowNodeStateDetail> getWorkflowNodeStates(ProgramRunId workflowRunId);

  /**
   * Used by {@link co.cask.cdap.gateway.handlers.WorkflowStatsSLAHttpHandler} to get the statistics of all completed
   * workflows in a time range.
   *
   * @param workflowId Workflow that needs to have its statistics returned
   * @param startTime StartTime of the range
   * @param endTime EndTime of the range
   * @param percentiles List of percentiles that the user wants to see
   * @return the statistics for a given workflow
   */
  WorkflowStatistics getWorkflowStatistics(Id.Workflow workflowId, long startTime,
                                           long endTime, List<Double> percentiles);

  /**
   * Returns the record that represents the run of a workflow.
   *
   * @param workflowId The Workflow whose run needs to be queried
   * @param runId RunId of the workflow run
   * @return A workflow run record corresponding to the runId
   */
  WorkflowDataset.WorkflowRunRecord getWorkflowRun(Id.Workflow workflowId, String runId);

  /**
   * Get a list of workflow runs that are spaced apart by time interval in both directions from the run id provided.
   *
   * @param workflow The workflow whose statistics need to be obtained
   * @param runId The run id of the workflow
   * @param limit The number of the records that the user wants to compare against on either side of the run
   * @param timeInterval The timeInterval with which the user wants to space out the runs
   * @return Map of runId of Workflow to DetailedStatistics of the run
   */
  Collection<WorkflowDataset.WorkflowRunRecord> retrieveSpacedRecords(Id.Workflow workflow, String runId,
                                                                      int limit, long timeInterval);

  /**
   * @return programs that were running between given start and end time.
   */
  Set<RunId> getRunningInRange(long startTimeInSecs, long endTimeInSecs);
}

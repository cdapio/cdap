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

package co.cask.cdap.app.store;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceWorker;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.exception.ApplicationNotFoundException;
import co.cask.cdap.common.exception.BadRequestException;
import co.cask.cdap.common.exception.NamespaceNotFoundException;
import co.cask.cdap.common.exception.ScheduleAlreadyExistsException;
import co.cask.cdap.common.exception.ScheduleNotFoundException;
import co.cask.cdap.internal.app.runtime.adapter.AdapterStatus;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link Store} operates on a {@link Program}. It's responsible
 * for managing the non-runtime lifecycle of a {@link Program}
 */
public interface Store {

  /**
   * Loads a given program.
   *
   * @param program id of the program
   * @param type of program
   * @return An instance of {@link co.cask.cdap.app.program.DefaultProgram} if found.
   * @throws IOException
   */
  Program loadProgram(Id.Program program, ProgramType type) throws IOException;

  /**
   * Logs start of program run.
   *
   * @param id        Info about program
   * @param pid       run id
   * @param startTime start timestamp
   */
  void setStart(Id.Program id, String pid, long startTime);

  /**
   * Logs end of program run.
   *
   * @param id      id of program
   * @param pid     run id
   * @param endTime end timestamp
   * @param state   State of program
   */
  void setStop(Id.Program id, String pid, long endTime, ProgramController.State state);

  /**
   * Fetches run records for particular program. Returns only finished runs.
   * Returned ProgramRunRecords are sorted by their startTime.
   *
   * @param id        program id.
   * @param status    status of the program running/completed/failed or all
   * @param startTime fetch run history that has started after the startTime.
   * @param endTime   fetch run history that has started before the endTime.
   * @param limit     max number of entries to fetch for this history call.
   * @return          list of logged runs
   */
  List<RunRecord> getRuns(Id.Program id, ProgramRunStatus status,
                          long startTime, long endTime, int limit);

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
  List<ProgramSpecification> getDeletedProgramSpecifications (Id.Application id,
                                                              ApplicationSpecification specification);

  /**
   * Returns application specification by id.
   *
   * @param id application id
   * @return application specification
   * @throws ApplicationNotFoundException if the application does not exist
   */
  ApplicationSpecification getApplication(Id.Application id) throws ApplicationNotFoundException;

  /**
   * Returns a collection of all application specs.
   */
  Collection<ApplicationSpecification> getAllApplications(Id.Namespace id) throws NamespaceNotFoundException;

  /**
   * Returns location of the application archive.
   *
   * @param id application id
   * @return application archive location
   */
  @Nullable
  Location getApplicationArchiveLocation(Id.Application id) throws ApplicationNotFoundException;

  /**
   * Sets number of instances of specific flowlet.
   *
   * @param id flow id
   * @param flowletId flowlet id
   * @param count new number of instances
   */
  void setFlowletInstances(Id.Program id, String flowletId, int count);

  /**
   * Gets number of instances of specific flowlet.
   *
   * @param id flow id
   * @param flowletId flowlet id
   */
  int getFlowletInstances(Id.Program id, String flowletId);

  /**
   * Set the number of procedure instances.
   *
   * @param id     program id
   * @param count  new number of instances.
   * @deprecated As of version 2.6.0, replaced by {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  void setProcedureInstances(Id.Program id, int count);

  /**
   * Gets the number of procedure instances.
   *
   * @param id  program id
   * @return    number of instances
   * @deprecated As of version 2.6.0, replaced by {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  int getProcedureInstances(Id.Program id);

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
   * Sets the number of instances of a {@link ServiceWorker}.
   *
   * @param id program id
   * @param workerName name of the {@link ServiceWorker}
   * @param instances number of instances
   */
  void setServiceWorkerInstances(Id.Program id, String workerName, int instances);

  /**
   * Returns the number of instances of a {@link ServiceWorker}.
   *
   * @param id program id
   * @param workerName name of the {@link ServiceWorker}.
   * @return number of instances
   */
  int getServiceWorkerInstances(Id.Program id, String workerName);

  /**
   * Sets the number of instances of a {@link Worker}
   *
   * @param id program id
   * @param instances number of instances
   */
  void setWorkerInstances(Id.Program id, int instances);

  /**
   * Gets the number of instances of a {@link Worker}
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
   * Store the user arguments needed in the run-time.
   *
   * @param programId id of program
   * @param arguments Map of key value arguments
   */
  void storeRunArguments(Id.Program programId, Map<String, String> arguments);

  /**
   * Get run time arguments for a program.
   *
   * @param programId id of the program.
   * @return Map of key, value pairs
   */
  Map<String, String> getRunArguments(Id.Program programId);

  /**
   * Changes input stream for a flowlet connection
   *
   * @param flow defines flow that contains a flowlet which connection to change
   * @param flowletId flowlet which connection to change
   * @param oldValue name of the stream in stream connection to change
   * @param newValue name of the new stream to connect to
   */
  void changeFlowletSteamConnection(Id.Program flow, String flowletId, String oldValue, String newValue) throws BadRequestException;

  /**
   * Adds a schedule for a particular program. If the schedule with the name already exists, the method will
   * throw RuntimeException.
   *
   * @param program defines program to which a schedule is being added
   * @param scheduleSpecification defines the schedule to be added for the program
   */
  void addSchedule(Id.Program program, ScheduleSpecification scheduleSpecification) throws ScheduleAlreadyExistsException;

  /**
   * Deletes a schedule from a particular program.
   *
   * @param schedule the schedule to delete
   */
  void deleteSchedule(Id.Schedule schedule) throws ScheduleNotFoundException;

  /**
   * Check if a program exists.
   *
   * @param id id of program.
   * @param type type of program.
   * @return true if the program exists, false otherwise.
   */
  boolean programExists(Id.Program id, ProgramType type);

  /**
   * Creates a new namespace.
   *
   * @param metadata {@link NamespaceMeta} representing the namespace metadata
   * @return existing {@link NamespaceMeta} if a namespace with the specified name existed already, null if the
   * a namespace with the specified name did not exist, and was created successfully
   * These semantics of return type are borrowed from {@link java.util.concurrent.ConcurrentHashMap#putIfAbsent}
   */
  @Nullable
  NamespaceMeta createNamespace(NamespaceMeta metadata);

  /**
   * Retrieves a namespace from the namespace metadata store.
   *
   * @param id {@link Id.Namespace} of the requested namespace
   * @return {@link NamespaceMeta} of the requested namespace
   */
  @Nullable
  NamespaceMeta getNamespace(Id.Namespace id);

  /**
   * Deletes a namespace from the namespace metadata store.
   *
   * @param id {@link Id.Namespace} of the namespace to delete
   * @return {@link NamespaceMeta} of the namespace if it was found and deleted, null if the specified namespace did not
   * exist
   * These semantics of return type are borrowed from {@link java.util.concurrent.ConcurrentHashMap#remove}
   */
  @Nullable
  NamespaceMeta deleteNamespace(Id.Namespace id);

  /**
   * Lists all registered namespaces.
   *
   * @return a list of all registered namespaces
   */
  List<NamespaceMeta> listNamespaces();

  /**
   * Adds adapter spec to the store, with status = {@link AdapterStatus.STARTED}. Will overwrite the existing spec.
   *
   * @param id Namespace id
   * @param adapterSpec adapter specification of the adapter being added
   */
  void addAdapter(Id.Namespace id, AdapterSpecification adapterSpec);

  /**
   * Fetch the adapter identified by the name in a give namespace.
   *
   * @param id  Namespace id.
   * @param name Adapter name
   * @return an instance of {@link AdapterSpecification}.
   */
  @Nullable
  AdapterSpecification getAdapter(Id.Namespace id, String name);

  /**
   * Fetch the status for an adapter identified by the name in a give namespace.
   *
   * @param id  Namespace id.
   * @param name Adapter name
   * @return status of specified adapter.
   */
  @Nullable
  AdapterStatus getAdapterStatus(Id.Namespace id, String name);

  /**
   * Set the status for an adapter identified by the name in a give namespace.
   *
   * @param id  Namespace id.
   * @param name Adapter name
   * @param status Status to set
   * @return previous status of adapter, or null if specified adapter is not found.
   */
  @Nullable
  AdapterStatus setAdapterStatus(Id.Namespace id, String name, AdapterStatus status);

  /**
   * Fetch all the adapters in a given namespace.
   *
   * @param id Namespace id.
   * @return {@link Collection} of Adapter Specifications.
   */
  Collection<AdapterSpecification> getAllAdapters(Id.Namespace id) throws NamespaceNotFoundException;

  /**
   * Remove the adapter specified by the name in a given namespace.
   *
   * @param id Namespace id.
   * @param name Adapter name.
   */
  void removeAdapter(Id.Namespace id, String name);

  /**
   * Remove all the adapters in a given namespace.
   *
   * @param id Namespace id.
   */
  void removeAllAdapters(Id.Namespace id);

}

/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.store;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Type;
import com.continuuity.data2.OperationException;
import com.google.common.collect.Table;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
   * @return An instance of {@link com.continuuity.app.program.DefaultProgram} if found.
   * @throws IOException
   */
  Program loadProgram(Id.Program program, Type type) throws IOException;

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
  void setStop(Id.Program id, String pid, long endTime, String state);

  /**
   * Fetches run history for particular program. Returns only finished runs.
   * Returned ProgramRunRecords are sorted by their startTime.
   *
   * @param id        program id.
   * @param startTime fetch run history that has started after the startTime.
   * @param endTime   fetch run history that has started before the endTime.
   * @param limit     max number of entries to fetch for this history call.
   * @return          list of logged runs
   * @throws          OperationException
   */
  List<RunRecord> getRunHistory(Id.Program id, long startTime, long endTime, int limit) throws OperationException;

  /**
   * Returns all {@link RunRecord} of the account.
   * @param account account id
   * @return An immutable table of program type, id and run records
   * @throws OperationException
   */
  Table<Type, Id.Program, List<RunRecord>> getAllRunHistory(Id.Account account) throws OperationException;

  /**
   * Creates a new stream if it does not exist.
   * @param id the account id
   * @param stream the stream to create
   * @throws OperationException
   */
  void addStream(Id.Account id, StreamSpecification stream) throws OperationException;

  /**
   * Remove a stream from the metadata of an account.
   * @param id the account id
   * @param name the stream name
   * @throws OperationException
   */
  void removeStream(Id.Account id, String name) throws OperationException;

  /**
   * Get the spec of a named stream.
   * @param id the account id
   * @param name the name of the stream
   * @throws OperationException
   */
  StreamSpecification getStream(Id.Account id, String name) throws OperationException;

  /**
   * Get the specs of all streams for an account.
   *
   * @param id the account id
   * @throws OperationException
   */

  Collection<StreamSpecification> getAllStreams(Id.Account id) throws OperationException;

  /**
   * Creates a new dataset if it does not exist, otherwise updates existing dataset with new spec.
   * @param id the account id
   * @param dsSpec the specification of the dataset
   * @throws OperationException
   */
  void addDataset(Id.Account id, DataSetSpecification dsSpec) throws OperationException;

  /**
   * Remove a dataset from the metadata of an account.
   * @param id the account id
   * @param name the dataset name
   * @throws OperationException
   */
  void removeDataSet(Id.Account id, String name) throws OperationException;

  /**
   * Get the spec of a named dataset.
   * @param id the account id
   * @param name the name of the dataset
   * @throws OperationException
   */
  DataSetSpecification getDataSet(Id.Account id, String name) throws OperationException;

  /**
   * Get the specs of all datasets for an account.
   *
   * @param id the account id
   * @throws OperationException
   */
  Collection<DataSetSpecification> getAllDataSets(Id.Account id) throws OperationException;

  /**
   * Creates new application if it doesn't exist. Updates existing one otherwise.
   *
   * @param id            application id
   * @param specification application specification to store
   * @param appArchiveLocation location of the deployed app archive
   * @throws OperationException
   */
  void addApplication(Id.Application id,
                      ApplicationSpecification specification, Location appArchiveLocation) throws OperationException;


  /**
   * Return a list of program specifications that are deleted comparing the specification in the store with the
   * spec that is passed.
   *
   * @param id                   ApplicationId
   * @param specification        Application specification
   * @return                     List of ProgramSpecifications that are deleted
   * @throws OperationException  on errors
   */
  List<ProgramSpecification> getDeletedProgramSpecifications (Id.Application id,
                                                              ApplicationSpecification specification)
                                                              throws OperationException;

  /**
   * Returns application specification by id.
   *
   * @param id application id
   * @return application specification
   * @throws OperationException
   */
  ApplicationSpecification getApplication(Id.Application id) throws OperationException;

  /**
   * Returns a collection of all application specs.
   */
  Collection<ApplicationSpecification> getAllApplications(Id.Account id) throws OperationException;

  /**
   * Returns location of the application archive.
   *
   * @param id application id
   * @return application archive location
   * @throws OperationException
   */
  Location getApplicationArchiveLocation(Id.Application id) throws OperationException;

  /**
   * Sets number of instances of specific flowlet.
   *
   * @param id flow id
   * @param flowletId flowlet id
   * @param count new number of instances
   * @throws OperationException
   */
  void setFlowletInstances(Id.Program id, String flowletId, int count) throws OperationException;

  /**
   * Gets number of instances of specific flowlet.
   *
   * @param id flow id
   * @param flowletId flowlet id
   * @throws OperationException
   */
  int getFlowletInstances(Id.Program id, String flowletId) throws OperationException;

  /**
   * Set the number of procedure instances.
   *
   * @param id     program id
   * @param count  new number of instances.
   * @throws OperationException
   */
  void setProcedureInstances(Id.Program id, int count) throws OperationException;

  /**
   * Gets the number of procedure instances.
   *
   * @param id  program id
   * @return    number of instances
   * @throws OperationException
   */
  int getProcedureInstances(Id.Program id) throws OperationException;

  /**
   * Sets the number of instances of a service runnable.
   *
   * @param id program id
   * @param runnableName name of the runnable
   * @param instances number of instances
   * @throws OperationException
   */
  void setServiceRunnableInstances(Id.Program id, String runnableName, int instances) throws OperationException;

  /**
   * Gets runnable instances of service.
   *
   * @param id program id
   * @param runnableName name of the runnable
   * @return number of instances
   * @throws OperationException
   */
  int getServiceRunnableInstances(Id.Program id, String runnableName) throws OperationException;

  /**
   * Removes all program under the given application and also the application itself.
   *
   * @param id Application id
   * @return An {@link ApplicationSpecification} of the application being removed.
   * @throws OperationException
   */
  ApplicationSpecification removeApplication(Id.Application id) throws OperationException;

  /**
   * Removes all applications (with programs) of the given account.
   *
   * @param id account id whose applications to remove
   */
  void removeAllApplications(Id.Account id) throws OperationException;

  /**
   * Remove all metadata associated with account.
   *
   * @param id account id whose items to remove
   */
  void removeAll(Id.Account id) throws OperationException;

  /**
   * Store the user arguments needed in the run-time.
   *
   * @param programId id of program
   * @param arguments Map of key value arguments
   * @throws OperationException
   */
  void storeRunArguments(Id.Program programId, Map<String, String> arguments) throws OperationException;

  /**
   * Get run time arguments for a program.
   *
   * @param programId id of the program.
   * @return Map of key, value pairs
   * @throws OperationException
   */
  Map<String, String> getRunArguments(Id.Program programId) throws OperationException;

  /**
   * Changes input stream for a flowlet connection
   * @param flow defines flow that contains a flowlet which connection to change
   * @param flowletId flowlet which connection to change
   * @param oldValue name of the stream in stream connection to change
   * @param newValue name of the new stream to connect to
   * @throws OperationException
   */
  void changeFlowletSteamConnection(Id.Program flow, String flowletId, String oldValue, String newValue)
    throws OperationException;
}

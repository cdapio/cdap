/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.store;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Type;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.common.collect.Table;

import java.io.IOException;
import java.util.List;

/**
 * {@link Store} operates on a {@link com.continuuity.app.program.Program}. It's responsible
 * for managing the non-runtime lifecycle of a {@link com.continuuity.app.program.Program}
 */
public interface Store {

  /**
   * Loads a given program
   *
   * @param program id of the program
   * @param type of program
   * @return An instance of {@link Program} if found.
   * @throws IOException
   */
  Program loadProgram(Id.Program program, Type type) throws IOException;

  /**
   * @return MetaDataService to access program configuration data
   */
  MetadataService.Iface getMetaDataService();

  /**
   * Logs start of program run.
   *
   * @param id        Info about program
   * @param pid       run id
   * @param startTime start timestamp
   */
  void setStart(Id.Program id, String pid, long startTime);

  /**
   * Logs end of program run
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
   * @param id program id
   * @return list of logged runs
   * @throws OperationException
   */
  List<RunRecord> getRunHistory(Id.Program id) throws OperationException;

  /**
   * Returns all {@link RunRecord} of the account.
   * @param account account id
   * @return An immutable table of program type, id and run records
   * @throws OperationException
   */
  Table<Type, Id.Program, List<RunRecord>> getAllRunHistory(Id.Account account) throws OperationException;

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
   * Returns application specification by id.
   *
   * @param id application id
   * @return application specification
   * @throws OperationException
   */
  ApplicationSpecification getApplication(Id.Application id) throws OperationException;

  /**
   * Returns location of the application archive
   * @param id application id
   * @return application archive location
   * @throws OperationException
   */
  Location getApplicationArchiveLocation(Id.Application id) throws OperationException;

  /**
   * Sets number of instances of specific flowlet
   * @param id flow id
   * @param flowletId flowlet id
   * @param count new number of instances
   * @throws OperationException
   */
  void setFlowletInstances(Id.Program id, String flowletId, int count) throws OperationException;

  /**
   * Removes program data
   * @param id program to remove
   */
  void remove(Id.Program id) throws OperationException;

  /**
   * Removes all program under the given application and also the application itself.
   * @param id Application id
   * @return An {@link ApplicationSpecification} of the application being removed.
   * @throws OperationException
   */
  ApplicationSpecification removeApplication(Id.Application id) throws OperationException;

  /**
   * Removes all applications (with programs) of the given account
   * @param id account id whose applications to remove
   */
  void removeAllApplications(Id.Account id) throws OperationException;

  /**
   * Remove all metadata associated with account
   * @param id account id whose items to remove
   */
  void removeAll(Id.Account id) throws OperationException;
}

/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.store;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.app.Id;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Status;
import com.continuuity.metadata.thrift.MetadataService;

import java.util.List;

/**
 * {@link Store} operates on a {@link com.continuuity.app.program.Program}. It's responsible
 * for managing the non-runtime lifecycle of a {@link com.continuuity.app.program.Program}
 */
public interface Store {
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
  void setStart(Id.Program id, String pid, long startTime) throws OperationException;

  /**
   * Logs end of program run
   *
   * @param id      id of program
   * @param pid     run id
   * @param endTime end timestamp
   * @param state   State of program
   */
  void setEnd(Id.Program id, String pid, long endTime, Status state) throws OperationException;

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
   * Stores application specification
   *
   * @param id            application id
   * @param specification application specification to store
   * @throws OperationException
   */
  void addApplication(Id.Application id,
                      ApplicationSpecification specification) throws OperationException;

  /**
   * Returns application specification by id.
   *
   * @param id application id
   * @return application specification
   * @throws OperationException
   */
  ApplicationSpecification getApplication(Id.Application id) throws OperationException;
}

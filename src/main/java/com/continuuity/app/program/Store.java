/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.metadata.thrift.MetadataService;

import java.util.List;

/**
 * {@link Store} operates on a {@link Program}. It's responsible
 * for managing the non-runtime lifecycle of a {@link Program}
 */
public interface Store {
  /**
   * Program Id identifies a given application.
   * Application is global unique if used within context of account.
   */
  public static class ApplicationId {
    private final String accountId;
    private final String applicationId;

    public ApplicationId(final String accountId, final String applicationId) {
      this.accountId = accountId;
      this.applicationId = applicationId;
    }

    public String getAccountId() {
      return accountId;
    }

    public String getApplicationId() {
      return applicationId;
    }
  }

  /**
   * Program Id identifies a given program.
   * Program is global unique if used within context of account and application.
   */
  public static class ProgramId extends ApplicationId {
    private final String programId;

    public ProgramId(final String accountId, final String applicationId, final String programId) {
      super(accountId, applicationId);
      this.programId = programId;
    }

    public String getProgramId() {
      return programId;
    }
  }

  /**
   * @return MetaDataService to access program configuration data
   */
  MetadataService.Iface getMetaDataService();

  /**
   * Logs start of program run.
   *
   * @param id Info about program
   * @param pid  run id
   * @param startTime start timestamp
   */
  void setStart(ProgramId id, String pid, long startTime) throws OperationException;

  /**
   * Logs end of program run
   *
   * @param id id of program
   * @param pid run id
   * @param endTime end timestamp
   * @param state State of program
   */
  void setEnd(ProgramId id, String pid, long endTime, Status state) throws OperationException;

  /**
   * Fetches run history for particular program. Returns only finished runs.
   * Returned ProgramRunRecords are sorted by their startTime.
   * @param id program id
   * @return list of logged runs
   * @throws OperationException
   */
  List<RunRecord> getRunHistory(ProgramId id) throws OperationException;

  /**
   * Stores application specification
   * @param id application id
   * @param specification application specification to store
   * @throws OperationException
   */
  void addApplication(ApplicationId id,
                      ApplicationSpecification specification) throws OperationException;

  /**
   * Returns application specification by id.
   * @param id application id
   * @return application specification
   * @throws OperationException
   */
  ApplicationSpecification getApplication(ApplicationId id) throws OperationException;
}

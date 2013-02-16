/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.api.data.OperationException;
import com.continuuity.metadata.thrift.MetadataService;

/**
 * {@link Store} operates on a {@link Program}. It's responsible
 * for managing the non-runtime lifecycle of a {@link Program}
 */
public interface Store {

  /**
   * Program Id identifies a given program.
   * Program is global unique if used within context of account and application.
   */
  public static class ProgramId {
    private final String accountId;
    private final String applicationId;
    private final String programId;

    public ProgramId(final String accountId, final String applicationId, final String programId) {
      this.accountId = accountId;
      this.applicationId = applicationId;
      this.programId = programId;
    }

    public String getAccountId() {
      return accountId;
    }

    public String getApplicationId() {
      return applicationId;
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
}

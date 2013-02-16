/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.api.data.OperationException;
import com.continuuity.metadata.thrift.MetadataService;

import java.util.List;

/**
 * {@link Store} operates on a {@link Program}. It's responsible
 * for managing the non-runtime lifecycle of a {@link Program}
 */
public interface Store {
  /**
   * @return MetaDataService to access program configuration data
   */
  MetadataService.Iface getMetaDataService();

  /**
   * Logs start of program run
   * @param accountId account this program belongs to
   * @param applicationId application this program belongs to
   * @param programId program id
   * @param runId run id
   * @param startTs start timestamp
   */
  void logProgramStart(String accountId, String applicationId, String programId, String runId,
                          long startTs) throws OperationException;

  /**
   * Logs end of program run
   * @param accountId account this program belongs to
   * @param applicationId application this program belongs to
   * @param programId program id
   * @param runId run id
   * @param endTs end timestamp
   * @param endState program run result
   */
  void logProgramEnd(String accountId, String applicationId, String programId, String runId,
                        long endTs, ProgramRunResult endState) throws OperationException;

  /**
   * @return A list of available version of the program.
   */
  List<Version> getAvailableVersions();

  /**
   * @return Current active version of this {@link Program}
   */
  Version getCurrentVersion();

  /**
   * Deletes a <code>version</code> of this {@link Program}
   *
   * @param version of the {@link Program} to be deleted.
   */
  void delete(Version version);

  /**
   * Deletes all the versions of this {@link Program}
   */
  void deleteAll();


}

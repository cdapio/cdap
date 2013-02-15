/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.program.ProgramRunResult;
import com.continuuity.app.program.Store;
import com.continuuity.app.program.Version;
import com.continuuity.data.metadata.MetaDataEntry;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.inject.Inject;

import java.util.List;

/**
 * Implementation of the Store that ultimately places data into MetaDataStore (thru MetadataService or directly).
 */
public class MDSBasedStore implements Store {
  static final String ENTRY_TYPE_PROGRAM_RUN = "program_run";
  static final String FIELD_PROGRAM_RUN_START_TS = "startTs";
  static final String FIELD_PROGRAM_RUN_END_TS = "endTs";
  static final String FIELD_PROGRAM_RUN_END_STATE = "endState";

  // We re-use metadataService to store configuration type data
  private MetadataService.Iface metaDataService;

  // We use metaDataStore directly to store user actions history
  private MetaDataStore metaDataStore;

  @Inject
  public MDSBasedStore(MetaDataStore metaDataStore,
                       MetadataService.Iface metaDataService) {
    this.metaDataStore = metaDataStore;
    this.metaDataService = metaDataService;
  }

  /**
   * @return MetaDataService to access program configuration data
   */
  public MetadataService.Iface getMetaDataService() {
    return metaDataService;
  }

  @Override
  public void logProgramStart(final String accountId, final String applicationId, final String programId,
                              final String runId, final long startTs) throws OperationException {
    MetaDataEntry entry = new MetaDataEntry(accountId, applicationId, ENTRY_TYPE_PROGRAM_RUN, programId);
    entry.addField(FIELD_PROGRAM_RUN_START_TS, String.valueOf(startTs));

    OperationContext context = new OperationContext(accountId);
    // perform insert, no conflict resolution
    metaDataStore.add(context, entry, false);
  }

  @Override
  public void logProgramEnd(final String accountId, final String applicationId, final String programId,
                            final String runId, final long endTs, final ProgramRunResult endState)
    throws OperationException {

    OperationContext context = new OperationContext(accountId);

    // we want program run info to be in one entry to make things cleaner on reading end
    metaDataStore.updateField(context, accountId, applicationId, ENTRY_TYPE_PROGRAM_RUN, programId,
                              FIELD_PROGRAM_RUN_END_TS, String.valueOf(endTs), -1);
    metaDataStore.updateField(context, accountId, applicationId, ENTRY_TYPE_PROGRAM_RUN, programId,
                              FIELD_PROGRAM_RUN_END_STATE, String.valueOf(endState), -1);
  }

  /**
   * @return A list of available version of the program.
   */
  @Override
  public List<Version> getAvailableVersions() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * @return Current active version of this {@link com.continuuity.app.program.Program}
   */
  @Override
  public Version getCurrentVersion() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Deletes a <code>version</code> of this {@link com.continuuity.app.program.Program}
   *
   * @param version of the {@link com.continuuity.app.program.Program} to be deleted.
   */
  @Override
  public void delete(Version version) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Deletes all the versions of this {@link com.continuuity.app.program.Program}
   */
  @Override
  public void deleteAll() {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}

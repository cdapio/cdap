/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.program.Status;
import com.continuuity.app.program.Store;
import com.continuuity.data.metadata.MetaDataEntry;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.inject.Inject;

/**
 * Implementation of the Store that ultimately places data into
 * MetaDataStore (thru MetadataService or directly).
 */
public class MDSBasedStore implements Store {
  static final String ENTRY_TYPE_PROGRAM_RUN = "run";
  static final String FIELD_PROGRAM_RUN_START_TS = "start";
  static final String FIELD_PROGRAM_RUN_END_TS = "end";
  static final String FIELD_PROGRAM_RUN_END_STATE = "stat";

  /**
   * We re-use metadataService to store configuration type data
   */
  private MetadataService.Iface metaDataService;

  /**
   * We use metaDataStore directly to store user actions history
   */
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

  /**
   * Logs start of program run.
   *
   * @param id Info about program
   * @param pid  run id
   * @param startTime start timestamp
   */
  @Override
  public void setStart(ProgramId id, final String pid, final long startTime) throws OperationException {
    MetaDataEntry entry = new MetaDataEntry(id.getAccountId(), id.getApplicationId(), ENTRY_TYPE_PROGRAM_RUN, pid);
    entry.addField(FIELD_PROGRAM_RUN_START_TS, String.valueOf(startTime));

    OperationContext context = new OperationContext(id.getAccountId());
    // perform insert, no conflict resolution
    metaDataStore.add(context, entry, false);
  }

  /**
   * Logs end of program run
   *
   * @param id id of program
   * @param pid run id
   * @param endTime end timestamp
   * @param state State of program
   */
  @Override
  public void setEnd(ProgramId id, final String pid, final long endTime, final Status state)
    throws OperationException {

    OperationContext context = new OperationContext(id.getAccountId());

    // we want program run info to be in one entry to make things cleaner on reading end
    metaDataStore.updateField(context, id.getAccountId(), id.getApplicationId(), ENTRY_TYPE_PROGRAM_RUN, pid,
                              FIELD_PROGRAM_RUN_END_TS, String.valueOf(endTime), -1);
    metaDataStore.updateField(context, id.getAccountId(), id.getApplicationId(), ENTRY_TYPE_PROGRAM_RUN, pid,
                              FIELD_PROGRAM_RUN_END_STATE, String.valueOf(state), -1);
  }
}

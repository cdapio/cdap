/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.app.program.Id;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Status;
import com.continuuity.app.program.Store;
import com.continuuity.data.metadata.MetaDataEntry;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the Store that ultimately places data into
 * MetaDataStore (thru MetadataService or directly).
 */
public class MDSBasedStore implements Store {
  private static final ProgramRunRecordStartTimeComparator PROGRAM_RUN_RECORD_START_TIME_COMPARATOR =
    new ProgramRunRecordStartTimeComparator();
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
   * @param id        Info about program
   * @param pid       run id
   * @param startTime start timestamp
   */
  @Override
  public void setStart(Id.Program id, final String pid, final long startTime) throws OperationException {
    MetaDataEntry entry = new MetaDataEntry(id.getAccountId(), id.getApplicationId(),
                                            FieldTypes.ProgramRun.ENTRY_TYPE, pid);
    entry.addField(FieldTypes.ProgramRun.PROGRAM, id.getId());
    entry.addField(FieldTypes.ProgramRun.START_TS, String.valueOf(startTime));

    OperationContext context = new OperationContext(id.getAccountId());
    // perform insert, no conflict resolution
    metaDataStore.add(context, entry, false);
  }

  /**
   * Logs end of program run
   *
   * @param id      id of program
   * @param pid     run id
   * @param endTime end timestamp
   * @param state   State of program
   */
  @Override
  public void setEnd(Id.Program id, final String pid, final long endTime, final Status state)
    throws OperationException {
    Preconditions.checkArgument(state != null, "End state of program run should be defined");

    OperationContext context = new OperationContext(id.getAccountId());

    // we want program run info to be in one entry to make things cleaner on reading end
    metaDataStore.updateField(
                               context, id.getAccountId(), id.getApplicationId(),
                               FieldTypes.ProgramRun.ENTRY_TYPE, pid,
                               FieldTypes.ProgramRun.END_TS, String.valueOf(endTime), -1
    );
    metaDataStore.updateField(
                               context, id.getAccountId(), id.getApplicationId(),
                               FieldTypes.ProgramRun.ENTRY_TYPE, pid,
                               FieldTypes.ProgramRun.END_STATE, String.valueOf(state), -1
    );
  }

  @Override
  public List<RunRecord> getRunHistory(final Id.Program id) throws OperationException {
    OperationContext context = new OperationContext(id.getAccountId());
    Map<String, String> filterByFields = new HashMap<String, String>();
    filterByFields.put(FieldTypes.ProgramRun.PROGRAM, id.getId());
    List<MetaDataEntry> entries = metaDataStore.list(context,
                                                     id.getAccountId(),
                                                     id.getApplicationId(),
                                                     FieldTypes.ProgramRun.ENTRY_TYPE, filterByFields
    );

    List<RunRecord> runHistory = new ArrayList<RunRecord>();
    for(MetaDataEntry entry : entries) {
      String endTsStr = entry.getTextField(FieldTypes.ProgramRun.END_TS);
      if(endTsStr == null) {
        // we need to return only those that finished
        continue;
      }
      runHistory.add(
                      new RunRecord(
                                     entry.getId(),
                                     Long.valueOf(entry.getTextField(FieldTypes.ProgramRun.START_TS)),
                                     Long.valueOf(endTsStr),
                                     Status.valueOf(entry.getTextField(FieldTypes.ProgramRun.END_STATE))
                      )
      );
    }

    Collections.sort(runHistory, PROGRAM_RUN_RECORD_START_TIME_COMPARATOR);

    return runHistory;
  }

  /**
   * Compares RunRecord using their start time.
   */
  private static final class ProgramRunRecordStartTimeComparator implements Comparator<RunRecord> {
    @Override
    public int compare(final RunRecord left, final RunRecord right) {
      if(left.getStartTs() > right.getStartTs()) {
        return 1;
      } else {
        return left.getStartTs() < right.getStartTs() ? -1 : 0;
      }
    }
  }

  @Override
  public void addApplication(final Id.Application id,
                             final ApplicationSpecification specification) throws OperationException {
    MetaDataEntry entry = new MetaDataEntry(id.getAccountId(), null, FieldTypes.Application.ENTRY_TYPE,
                                            id.getId());
    ApplicationSpecificationAdapter adapter =
      ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    entry.addField(FieldTypes.Application.SPEC_JSON, adapter.toJson(specification));

    OperationContext context = new OperationContext(id.getAccountId());

    // TODO: add into metadataService all items

    metaDataStore.add(context, entry);
  }

  @Override
  public ApplicationSpecification getApplication(final Id.Application id) throws OperationException {
    OperationContext context = new OperationContext(id.getAccountId());

    MetaDataEntry entry = metaDataStore.get(context, id.getAccountId(), null, FieldTypes.Application.ENTRY_TYPE,
                                            id.getId());

    if(entry == null) {
      return null;
    }

    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create();
    return adapter.fromJson(entry.getTextField(FieldTypes.Application.SPEC_JSON));
  }
}

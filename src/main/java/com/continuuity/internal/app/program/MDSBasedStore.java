/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Id;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Status;
import com.continuuity.app.program.Store;
import com.continuuity.data.metadata.MetaDataEntry;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Application;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.Flow;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.continuuity.metadata.thrift.Query;
import com.continuuity.metadata.thrift.Stream;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of the Store that ultimately places data into
 * MetaDataStore (thru MetadataService or directly).
 */
public class MDSBasedStore implements Store {
  private static final Logger LOG
    = LoggerFactory.getLogger(MDSBasedStore.class);

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
                             final ApplicationSpecification spec) throws OperationException {
    MetaDataEntry entry = new MetaDataEntry(id.getAccountId(), null, FieldTypes.Application.ENTRY_TYPE,
                                            id.getId());
    ApplicationSpecificationAdapter adapter =
      ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    String jsonSpec = adapter.toJson(spec);
    entry.addField(FieldTypes.Application.SPEC_JSON, jsonSpec);

    OperationContext context = new OperationContext(id.getAccountId());

    LOG.trace("Application being added: id: {}: spec: {}", id.getId(), jsonSpec);
    metaDataStore.add(context, entry);
    LOG.trace("Added application to mds: id: {}, spec: {}", id.getId(), jsonSpec);

    try {
      Account account = new Account(id.getAccountId());
      // application
      Application application = new Application(id.getId());
      application.setName(spec.getName());
      application.setDescription(spec.getDescription());
      metaDataService.createApplication(account, application);

      // datasets
      for (DataSetSpecification datasetSpec : spec.getDataSets().values()) {
        Dataset dataset = new Dataset(datasetSpec.getName());
        // no description in datasetSpec
        dataset.setName(datasetSpec.getName());
        dataset.setDescription("");
        dataset.setType(datasetSpec.getType());
        dataset.setSpecification(new Gson().toJson(datasetSpec));
        // NOTE: we ignore result of adding, since it is assumed that all validation has happened before calling
        //       addApplication() and hence the call is successful
        metaDataService.assertDataset(account, dataset);
      }

      // streams
      for (StreamSpecification streamSpec: spec.getStreams().values()) {
        Stream stream = new Stream(streamSpec.getName());
        stream.setName(streamSpec.getName());
        // NOTE: we ignore result of adding, since it is assumed that all validation has happened before calling
        //       addApplication() and hence the call is successful
        metaDataService.assertStream(account, stream);
      }

      // flows
      for (FlowSpecification flowSpec : spec.getFlows().values()) {
        Flow flow = new Flow(flowSpec.getName(), id.getId());
        flow.setName(flowSpec.getName());

        Set<String> streams = new HashSet<String>();
        for (FlowletConnection con : flowSpec.getConnections()) {
          if (FlowletConnection.Type.STREAM == con.getSourceType()) {
            streams.add(con.getSourceName());
          }
        }
        flow.setStreams(new ArrayList<String>(streams));

        Set<String> datasets = new HashSet<String>();
        for (FlowletDefinition flowlet : flowSpec.getFlowlets().values()) {
          datasets.addAll(flowlet.getDatasets());
        }
        flow.setDatasets(new ArrayList<String>(datasets));
        metaDataService.createFlow(id.getAccountId(), flow);
      }

      // procedures
      for (ProcedureSpecification procedureSpec : spec.getProcedures().values()) {
        Query query = new Query(procedureSpec.getName(), id.getId());
        query.setName(procedureSpec.getName());
        query.setServiceName(procedureSpec.getName());
        // TODO: datasets are missing in ProcedureSpecification
        query.setDatasets(new ArrayList<String>());
        metaDataService.createQuery(account, query);
      }
    } catch(MetadataServiceException e) {
      throw Throwables.propagate(e);
    } catch(TException e) {
      throw Throwables.propagate(e);
    }
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

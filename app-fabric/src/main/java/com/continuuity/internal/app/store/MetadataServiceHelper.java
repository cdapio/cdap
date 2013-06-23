/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.store;


import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.Id;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Application;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.Flow;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.continuuity.metadata.thrift.Query;
import com.continuuity.metadata.thrift.Stream;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hack hack hack: time constraints
 * This is needed for updating info in metadataService which we have to keep in sync with new stuff which came with
 * new App Fabric since old UI still relies on it.
 */
class MetadataServiceHelper {
  /**
   * We re-use metadataService to store configuration type data
   */
  private MetadataService.Iface metaDataService;

  public MetadataServiceHelper(MetadataService.Iface metaDataService) {
    this.metaDataService = metaDataService;
  }

  public void updateInMetadataService(final Id.Application id, final ApplicationSpecification spec) {
    Account account = new Account(id.getAccountId());
    try {
      // application
      updateApplicationInMetadataService(id, spec);

      // datasets
      for (DataSetSpecification datasetSpec : spec.getDataSets().values()) {
        updateInMetadataService(account, datasetSpec);
      }

      // streams
      for (StreamSpecification streamSpec: spec.getStreams().values()) {
        updateInMetadataService(account, streamSpec);
      }

      // flows & mapreduce jobs
      updateFlowsAndMapReducesInMetadataService(id, spec);

      // procedures
      updateProceduresInMetadataService(id, spec);

    } catch(MetadataServiceException e) {
      throw Throwables.propagate(e);
    } catch(TException e) {
      throw Throwables.propagate(e);
    }
  }

  private void updateApplicationInMetadataService(Id.Application id, ApplicationSpecification spec)
    throws MetadataServiceException, TException {
    Account account = new Account(id.getAccountId());
    Application application = new Application(id.getId());
    application.setName(spec.getName());
    application.setDescription(spec.getDescription());
    Application existing = metaDataService.getApplication(account, application);
    if (existing.isExists()) {
      metaDataService.updateApplication(account, application);
    } else {
      metaDataService.createApplication(account, application);
    }
  }

  private void updateProceduresInMetadataService(Id.Application id, ApplicationSpecification spec)
    throws MetadataServiceException, TException {
    // Basic logic: we need to remove procedures that were removed from the app, add those that were added and
    //              update those that remained in the application.
    Account account = new Account(id.getAccountId());
    Map<String, Query> toStore = new HashMap<String, Query>();
    for (ProcedureSpecification procedureSpec : spec.getProcedures().values()) {
      Query query = new Query(procedureSpec.getName(), id.getId());
      query.setName(procedureSpec.getName());
      query.setServiceName(procedureSpec.getName());
      // TODO: datasets are missing in ProcedureSpecification
      query.setDatasets(new ArrayList<String>());

      toStore.put(query.getId(), query);
    }

    List<Query> toUpdate = new ArrayList<Query>();
    List<Query> toDelete = new ArrayList<Query>();

    List<Query> existingQueries = metaDataService.getQueries(account);
    for (Query existing : existingQueries) {
      if (id.getId().equals(existing.getApplication())) {
        String queryId = existing.getId();
        if (toStore.containsKey(queryId)) {
          toUpdate.add(toStore.get(queryId));
          toStore.remove(queryId);
        } else {
          toDelete.add(existing);
        }
      }
    }
    for (Query query : toDelete) {
      metaDataService.deleteQuery(account, query);
    }
    for (Query query : toUpdate) {
      metaDataService.updateQuery(account, query);
    }
    // all flows that remain in toStore are going to be created
    for (Query query : toStore.values()) {
      metaDataService.createQuery(account, query);
    }
  }

  private void updateFlowsAndMapReducesInMetadataService(Id.Application id, ApplicationSpecification spec)
    throws MetadataServiceException, TException {
    // Basic logic: we need to remove flows that were removed from the app, add those that were added and
    //              update those that remained in the application.
    Map<String, Flow> toStore = new HashMap<String, Flow>();
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
      toStore.put(flow.getId(), flow);
    }
    // also treating mapreduce jobs as flows:
    // we re-use the ability of existing UI to display flows as a way to display and run mapreduce jobs (for now)
    for (MapReduceSpecification mrSpec : spec.getMapReduces().values()) {
      Flow flow = new Flow(mrSpec.getName(), id.getId());
      flow.setName(mrSpec.getName());

      // no streams
      flow.setStreams(new ArrayList<String>());

      flow.setDatasets(new ArrayList<String>(mrSpec.getDataSets()));
      toStore.put(flow.getId(), flow);
    }

    List<Flow> toUpdate = new ArrayList<Flow>();
    List<Flow> toDelete = new ArrayList<Flow>();

    List<Flow> existingFlows = metaDataService.getFlows(id.getAccountId());
    for (Flow existing : existingFlows) {
      if (id.getId().equals(existing.getApplication())) {
        String flowId = existing.getId();
        if (toStore.containsKey(flowId)) {
          toUpdate.add(toStore.get(flowId));
          toStore.remove(flowId);
        } else {
          toDelete.add(existing);
        }
      }
    }
    for (Flow flow : toDelete) {
      metaDataService.deleteFlow(id.getAccountId(), id.getId(), flow.getId());
    }
    for (Flow flow : toUpdate) {
      metaDataService.updateFlow(id.getAccountId(), flow);
    }
    // all flows that remain in toStore are going to be created
    for (Flow flow : toStore.values()) {
      metaDataService.createFlow(id.getAccountId(), flow);
    }
  }

  private void updateInMetadataService(final Account account, final StreamSpecification streamSpec)
    throws MetadataServiceException, TException {
    Stream stream = new Stream(streamSpec.getName());
    stream.setName(streamSpec.getName());
    // NOTE: we ignore result of adding, since it is assumed that all validation has happened before calling
    //       addApplication() and hence the call is successful
    metaDataService.assertStream(account, stream);
  }

  private void updateInMetadataService(final Account account, final DataSetSpecification datasetSpec)
    throws MetadataServiceException, TException {
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

  public void deleteFlow(Id.Program id) throws MetadataServiceException {
    // unregister this flow in the meta data service
    Throwable toThrow = null;
    try {
      // we don't know whether this is a flow or query -> delete both
      metaDataService.deleteFlow(id.getAccountId(), id.getApplicationId(), id.getId());
    } catch (Throwable e) {
      toThrow = e;
    }
    try {
      metaDataService.deleteQuery(new Account(id.getAccountId()), new Query(id.getId(), id.getApplicationId()));
      toThrow = null;
    } catch (Throwable e) {
      toThrow = e;
    }
    if (toThrow != null) {
      String message = String.format("Error deleting program %s meta data for " +
                                       "account %s: %s", id.getId(), id.getAccountId(),
                                     toThrow.getMessage());

      throw new MetadataServiceException(message);
    }

  }

  public void deleteAll(Id.Account id) throws TException, MetadataServiceException {
    metaDataService.deleteAll(id.getId());
  }
}

/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.store;


import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.Id;
import com.continuuity.metadata.MetaDataStore;
import com.continuuity.metadata.MetadataServiceException;
import com.continuuity.metadata.types.Application;
import com.continuuity.metadata.types.Dataset;
import com.continuuity.metadata.types.Flow;
import com.continuuity.metadata.types.Mapreduce;
import com.continuuity.metadata.types.Procedure;
import com.continuuity.metadata.types.Stream;
import com.continuuity.metadata.types.Workflow;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(MetadataServiceHelper.class);
  /**
   * We re-use metadataService to store configuration type data
   */
  private MetaDataStore metaDataService;

  public MetadataServiceHelper(MetaDataStore metaDataService) {
    this.metaDataService = metaDataService;
  }

  public void updateInMetadataService(final Id.Application id, final ApplicationSpecification spec) {
    String account = id.getAccountId();
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

      // flows
      updateFlowsInMetadataService(id, spec);

      // mapreduce jobs
      updateMapReducesInMetadataService(id, spec);

      // procedures
      updateProceduresInMetadataService(id, spec);

      //workflows
      updateWorkflowsInMetadataService(id, spec);

    } catch (MetadataServiceException e) {
      throw Throwables.propagate(e);
    } catch (TException e) {
      throw Throwables.propagate(e);
    }
  }

  private void updateApplicationInMetadataService(Id.Application id, ApplicationSpecification spec)
    throws MetadataServiceException, TException {
    String account = id.getAccountId();
    Application application = new Application(id.getId());
    application.setName(spec.getName());
    application.setDescription(spec.getDescription());
    Application existing = metaDataService.getApplication(account, id.getId());
    if (existing != null) {
      metaDataService.updateApplication(account, application);
    } else {
      metaDataService.createApplication(account, application);
    }
  }

  private void updateProceduresInMetadataService(Id.Application id, ApplicationSpecification spec)
    throws MetadataServiceException, TException {
    // Basic logic: we need to remove procedures that were removed from the app, add those that were added and
    //              update those that remained in the application.
    String account = id.getAccountId();
    Map<String, Procedure> toStore = new HashMap<String, Procedure>();
    for (ProcedureSpecification procedureSpec : spec.getProcedures().values()) {
      Procedure procedure = new Procedure(procedureSpec.getName(), id.getId());
      procedure.setName(procedureSpec.getName());
      procedure.setServiceName(procedureSpec.getName());
      // TODO: datasets are missing in ProcedureSpecification
      procedure.setDatasets(new ArrayList<String>());

      toStore.put(procedure.getId(), procedure);
    }

    List<Procedure> toUpdate = new ArrayList<Procedure>();
    List<Procedure> toDelete = new ArrayList<Procedure>();

    List<Procedure> existingQueries = metaDataService.getProcedures(account);
    for (Procedure existing : existingQueries) {
      if (id.getId().equals(existing.getApplication())) {
        String procId = existing.getId();
        if (toStore.containsKey(procId)) {
          toUpdate.add(toStore.get(procId));
          toStore.remove(procId);
        } else {
          toDelete.add(existing);
        }
      }
    }
    for (Procedure procedure : toDelete) {
      metaDataService.deleteProcedure(account, procedure.getApplication(), procedure.getId());
    }
    for (Procedure procedure : toUpdate) {
      metaDataService.updateProcedure(account, procedure);
    }
    // all flows that remain in toStore are going to be created
    for (Procedure procedure : toStore.values()) {
      metaDataService.createProcedure(account, procedure);
    }
  }

  private void updateFlowsInMetadataService(Id.Application id, ApplicationSpecification spec)
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

  private void updateWorkflowsInMetadataService(Id.Application id, ApplicationSpecification spec)
    throws MetadataServiceException, TException {
    // Basic logic: we need to remove flows that were removed from the app, add those that were added and
    //              update those that remained in the application.
    Map<String, Workflow> toStore = Maps.newHashMap();
    for (WorkflowSpecification workflowSpec : spec.getWorkflows().values()) {
      Workflow workflow = new Workflow(workflowSpec.getName(), id.getId());
      workflow.setName(workflowSpec.getName());
      toStore.put(workflow.getId(), workflow);
    }

    List<Workflow> toUpdate = Lists.newArrayList();
    List<Workflow> toDelete = Lists.newArrayList();

    List<Workflow> existingWorkflows = metaDataService.getWorkflows(id.getAccountId());
    for (Workflow existing : existingWorkflows) {
      if (id.getId().equals(existing.getApplication())) {
        String workflowId = existing.getId();
        if (toStore.containsKey(workflowId)) {
          toUpdate.add(toStore.get(workflowId));
          toStore.remove(workflowId);
        } else {
          toDelete.add(existing);
        }
      }
    }
    for (Workflow workflow : toDelete) {
      metaDataService.deleteWorkflow(id.getAccountId(), id.getId(), workflow.getId());
    }
    for (Workflow workflow : toUpdate) {
      metaDataService.updateWorkflow(id.getAccountId(), workflow);
    }

    for (Workflow workflow : toStore.values()) {
      metaDataService.createWorkflow(id.getAccountId(), workflow);
    }
  }

  private void updateMapReducesInMetadataService(Id.Application id, ApplicationSpecification spec)
    throws MetadataServiceException, TException {
    // Basic logic: we need to remove mapreduces that were removed from the app, add those that were added and
    //              update those that remained in the application.
    String account = id.getAccountId();
    Map<String, Mapreduce> toStore = Maps.newHashMap();
    for (MapReduceSpecification mrSpec : spec.getMapReduces().values()) {
      Mapreduce mapreduce = new Mapreduce(mrSpec.getName(), id.getId());
      mapreduce.setName(mrSpec.getName());

      mapreduce.setDatasets(new ArrayList<String>(mrSpec.getDataSets()));
      toStore.put(mapreduce.getId(), mapreduce);
    }

    List<Mapreduce> toUpdate = Lists.newArrayList();
    List<Mapreduce> toDelete = Lists.newArrayList();

    List<Mapreduce> existingMapreduces = metaDataService.getMapreduces(account);
    for (Mapreduce existing : existingMapreduces) {
      if (id.getId().equals(existing.getApplication())) {
        String mapreduceId = existing.getId();
        if (toStore.containsKey(mapreduceId)) {
          toUpdate.add(toStore.get(mapreduceId));
          toStore.remove(mapreduceId);
        } else {
          toDelete.add(existing);
        }
      }
    }
    for (Mapreduce mapreduce : toDelete) {
      metaDataService.deleteMapreduce(account, mapreduce.getApplication(), mapreduce.getId());
    }
    for (Mapreduce mapreduce : toUpdate) {
      metaDataService.updateMapreduce(account, mapreduce);
    }
    // all mapreduces that remain in toStore are going to be created
    for (Mapreduce mapreduce : toStore.values()) {
      metaDataService.createMapreduce(account, mapreduce);
    }
  }

  private void updateInMetadataService(final String account, final StreamSpecification streamSpec)
    throws MetadataServiceException, TException {
    Stream stream = new Stream(streamSpec.getName());
    stream.setName(streamSpec.getName());
    // NOTE: we ignore result of adding, since it is assumed that all validation has happened before calling
    //       addApplication() and hence the call is successful
    metaDataService.assertStream(account, stream);
  }

  private void updateInMetadataService(final String account, final DataSetSpecification datasetSpec)
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
    try {
      metaDataService.deleteFlow(id.getAccountId(), id.getApplicationId(), id.getId());
    } catch (Throwable e) {
      String message = String.format("Error deleting program %s meta data for " +
                                       "account %s: %s", id.getId(), id.getAccountId(),
                                     e.getMessage());

      LOG.error(message, e);
      throw new MetadataServiceException(message);
    }

  }

  public void deleteQuery(Id.Program id) throws MetadataServiceException {
    try {
      metaDataService.deleteProcedure(id.getAccountId(), id.getApplicationId(), id.getId());
    } catch (Throwable e) {
      String message = String.format("Error deleting program %s meta data for " +
                                       "account %s: %s", id.getId(), id.getAccountId(),
                                     e.getMessage());

      LOG.error(message, e);
      throw new MetadataServiceException(message);
    }

  }

  public void deleteMapReduce(Id.Program id) throws MetadataServiceException {
    // unregister this mapreduce in the meta data service
    try {
      metaDataService.deleteMapreduce(id.getAccountId(), id.getApplicationId(), id.getId());
    } catch (Throwable e) {
      String message = String.format("Error deleting program %s meta data for " +
                                       "account %s: %s", id.getId(), id.getAccountId(),
                                     e.getMessage());

      LOG.error(message, e);
      throw new MetadataServiceException(message);
    }
  }

  public void deleteApplication(String account, String app) throws OperationException {
    // unregister this application in the meta data service
    try {
      metaDataService.deleteApplication(account, app);
    } catch (Throwable e) {
      String message = String.format("Error deleting application %s meta data for " +
                                       "account %s: %s", app, account, e.getMessage());

      LOG.error(message, e);
      throw new OperationException(StatusCode.INTERNAL_ERROR, message, e);
    }
  }

  public void deleteAll(Id.Account id) throws TException, MetadataServiceException {
    metaDataService.deleteAll(id.getId());
  }
}

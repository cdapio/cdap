/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Id;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Store;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.app.services.*;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.app.services.legacy.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.commons.lang.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A basic implementation of the FlowService interface.
 */
public final class RuntimeServiceImpl implements RuntimeService.Iface {
  private final Store store;

  @Inject
  public RuntimeServiceImpl(final Store store) {
    this.store = store;
  }

  @Override
  public RunIdentifier start(AuthToken token, final FlowDescriptor descriptor) throws RuntimeServiceException {
    Preconditions.checkNotNull(token);
    throw new NotImplementedException();
  }

  @Override
  public RunIdentifier stop(AuthToken token, final FlowIdentifier identifier) throws RuntimeServiceException {
    Preconditions.checkNotNull(token);
    throw new NotImplementedException();
  }

  @Override
  public FlowStatus status(AuthToken token, final FlowIdentifier identifier) throws RuntimeServiceException {
    Preconditions.checkNotNull(token);
    throw new NotImplementedException();
  }

  @Override
  public void setInstances(AuthToken token, final FlowIdentifier identifier,
                           final String flowletId, final short instances) throws RuntimeServiceException {
    Preconditions.checkNotNull(token);
    throw new NotImplementedException();
  }

  @Override
  public List<ActiveFlow> getFlows(final String accountId) {
    throw new NotImplementedException();
  }

  @Override
  public String getFlowDefinition(final FlowIdentifier identifier) throws RuntimeServiceException {
    Preconditions.checkNotNull(identifier);

    // in this code XXXspec is new API class, XXXdef is legacy

    if(identifier.getType() == EntityType.FLOW) {
      FlowDefinitionImpl flowDef = getFlowDef(identifier);
      return new Gson().toJson(flowDef);

    } else if(identifier.getType() == EntityType.QUERY) {
      QueryDefinitionImpl queryDef = getQueryDefinition(identifier);
      return new Gson().toJson(queryDef);
    }

    return null;
  }

  private QueryDefinitionImpl getQueryDefinition(final FlowIdentifier identifier) throws RuntimeServiceException {ApplicationSpecification appSpec = null;
    try {
      appSpec = store.getApplication(new Id.Application(new Id.Account(identifier.getAccountId()),
                                                        identifier.getApplicationId()));
    } catch(OperationException e) {
      throw  new RuntimeServiceException("Could NOT retrieve application spec for " +
                                           identifier.toString() + ", reason: " + e.getMessage());
    }

    ProcedureSpecification procedureSpec = appSpec.getProcedures().get(identifier.getFlowId());
    QueryDefinitionImpl queryDef = new QueryDefinitionImpl();

    // TODO: fill values (incl. list of datasets ) once they are added to ProcedureSpecification
    queryDef.setServiceName(procedureSpec.getName());
    return queryDef;
  }

  private FlowDefinitionImpl getFlowDef(final FlowIdentifier identifier) throws RuntimeServiceException {ApplicationSpecification appSpec = null;
    try {
      appSpec = store.getApplication(new Id.Application(new Id.Account(identifier.getAccountId()),
                                                        identifier.getApplicationId()));
    } catch(OperationException e) {
      throw  new RuntimeServiceException("Could NOT retrieve application spec for " +
                                           identifier.toString() + ", reason: " + e.getMessage());
    }

    FlowSpecification flowSpec = appSpec.getFlows().get(identifier.getFlowId());

    FlowDefinitionImpl flowDef = new FlowDefinitionImpl();

    fillFlowletsAndDataSets(flowSpec, flowDef);

    fillConnectionsAndStreams(identifier, flowSpec, flowDef);

    MetaDefinitionImpl metaDefinition = new MetaDefinitionImpl();
    metaDefinition.setApp(identifier.getApplicationId());

    // user info (email, company, etc.) is left empty
    return flowDef;
  }

  private void fillConnectionsAndStreams(final FlowIdentifier identifier, final FlowSpecification flowSpec, final FlowDefinitionImpl flowDef) {List<ConnectionDefinitionImpl> connections = new ArrayList<ConnectionDefinitionImpl>();
    // we gather streams across all connections, hence we need to eliminate duplicate streams hence using map
    Map<String, FlowStreamDefinitionImpl> flowStreams = new HashMap<String, FlowStreamDefinitionImpl>();

    QueueSpecificationGenerator generator = new SimpleQueueSpecificationGenerator(identifier.getAccountId());
    Table<String, String, QueueSpecification> queues =  generator.create(flowSpec);

    for (Table.Cell<String, String, QueueSpecification> con : queues.cellSet()) {
      String srcName = con.getRowKey();
      String destName = con.getColumnKey();
      FlowletStreamDefinitionImpl from;
      // TODO: put check on stream type here
      if (false) {
        // stream source
        from =  new FlowletStreamDefinitionImpl(srcName);
        flowStreams.put("srcName", new FlowStreamDefinitionImpl(srcName, null));
      } else {
        // flowlet source
        from =  new FlowletStreamDefinitionImpl(srcName, con.getValue().getQueueName().getSimpleName());
      }

      FlowletStreamDefinitionImpl to = new FlowletStreamDefinitionImpl(destName,
                                                                       con.getValue().getQueueName().getSimpleName());

      connections.add(new ConnectionDefinitionImpl(from, to));
    }

    flowDef.setConnections(connections);
    flowDef.setFlowStreams(new ArrayList<FlowStreamDefinitionImpl>(flowStreams.values()));
  }

  private void fillFlowletsAndDataSets(final FlowSpecification flowSpec, final FlowDefinitionImpl flowDef) {Set<String> datasets = new HashSet<String>();
    List<FlowletDefinitionImpl> flowlets = new ArrayList<FlowletDefinitionImpl>();

    for (FlowletDefinition flowletSpec : flowSpec.getFlowlets().values()) {
      datasets.addAll(flowletSpec.getDatasets());

      FlowletDefinitionImpl flowletDef = new FlowletDefinitionImpl();
      flowletDef.setClassName(flowletSpec.getFlowletSpec().getClassName());
      if (flowletSpec.getInputs().isEmpty()) {
        flowletDef.setFlowletType(FlowletType.SOURCE);
      } else if (flowletSpec.getOutputs().isEmpty()) {
        flowletDef.setFlowletType(FlowletType.SINK);
      } else {
        flowletDef.setFlowletType(FlowletType.COMPUTE);
      }

      flowletDef.setInstances(flowletSpec.getInstances());
      flowletDef.setName(flowletSpec.getFlowletSpec().getName());

      flowlets.add(flowletDef);
    }

    flowDef.setFlowlets(flowlets);
    flowDef.setDatasets(datasets);
  }

  @Override
  public List<FlowRunRecord> getFlowHistory(final FlowIdentifier id) throws RuntimeServiceException {
    List<RunRecord> log;
    try {
      log = store.getRunHistory(new Id.Program(new Id.Application(new Id.Account(id.getAccountId()),
                                                                  id.getApplicationId()),
                                               id.getFlowId()));
    } catch(OperationException e) {
      throw  new RuntimeServiceException("Could NOT retrieve application spec for " +
                                           id.toString() + ", reason: " + e.getMessage());

    }

    List<FlowRunRecord> history = new ArrayList<FlowRunRecord>();
    for (RunRecord runRecord : log) {
      history.add(new FlowRunRecord(runRecord.getPid(),
                                    runRecord.getStartTs(), runRecord.getStopTs(),
                                    runRecord.getEndStatus().name()));
    }

    return history;
  }

  @Override
  public void stopAll(final String account) throws RuntimeServiceException {
    throw new NotImplementedException();
  }
}

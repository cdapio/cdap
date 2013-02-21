/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.store;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Status;
import com.continuuity.app.store.Store;
import com.continuuity.data.metadata.MetaDataEntry;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.ForwardingApplicationSpecification;
import com.continuuity.internal.app.ForwardingFlowSpecification;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG
    = LoggerFactory.getLogger(MDSBasedStore.class);

  private static final ProgramRunRecordStartTimeComparator PROGRAM_RUN_RECORD_START_TIME_COMPARATOR =
    new ProgramRunRecordStartTimeComparator();
  /**
   * We re-use metadataService to store configuration type data
   */
  private final MetadataService.Iface metaDataService;

  private final MetadataServiceHelper metadataServiceHelper;

  /**
   * We use metaDataStore directly to store user actions history
   */
  private MetaDataStore metaDataStore;

  @Inject
  public MDSBasedStore(MetaDataStore metaDataStore,
                       MetadataService.Iface metaDataService) {
    this.metaDataStore = metaDataStore;
    this.metaDataService = metaDataService;
    this.metadataServiceHelper = new MetadataServiceHelper(metaDataService);
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
    metaDataStore.updateField(context, id.getAccountId(), id.getApplicationId(),
                              FieldTypes.ProgramRun.ENTRY_TYPE, pid,
                              FieldTypes.ProgramRun.END_TS, String.valueOf(endTime), -1);
    metaDataStore.updateField(context, id.getAccountId(), id.getApplicationId(),
                              FieldTypes.ProgramRun.ENTRY_TYPE, pid,
                              FieldTypes.ProgramRun.END_STATE, String.valueOf(state), -1);
  }

  @Override
  public List<RunRecord> getRunHistory(final Id.Program id) throws OperationException {
    OperationContext context = new OperationContext(id.getAccountId());
    Map<String, String> filterByFields = new HashMap<String, String>();
    filterByFields.put(FieldTypes.ProgramRun.PROGRAM, id.getId());
    List<MetaDataEntry> entries = metaDataStore.list(context,
                                                     id.getAccountId(),
                                                     id.getApplicationId(),
                                                     FieldTypes.ProgramRun.ENTRY_TYPE, filterByFields);

    List<RunRecord> runHistory = new ArrayList<RunRecord>();
    for(MetaDataEntry entry : entries) {
      String endTsStr = entry.getTextField(FieldTypes.ProgramRun.END_TS);
      if(endTsStr == null) {
        // we need to return only those that finished
        continue;
      }
      runHistory.add(new RunRecord(entry.getId(),
                                   Long.valueOf(entry.getTextField(FieldTypes.ProgramRun.START_TS)),
                                   Long.valueOf(endTsStr),
                                   Status.valueOf(entry.getTextField(FieldTypes.ProgramRun.END_STATE))));
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
    ApplicationSpecificationAdapter adapter =
      ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    String jsonSpec = adapter.toJson(spec);

    OperationContext context = new OperationContext(id.getAccountId());
    LOG.trace("Application being stored: id: {}: spec: {}", id.getId(), jsonSpec);
    MetaDataEntry existing = metaDataStore.get(context, id.getAccountId(), null,
                                               FieldTypes.Application.ENTRY_TYPE, id.getId());
    if (existing == null) {
      MetaDataEntry entry = new MetaDataEntry(id.getAccountId(), null, FieldTypes.Application.ENTRY_TYPE, id.getId());
      entry.addField(FieldTypes.Application.SPEC_JSON, jsonSpec);

      metaDataStore.add(context, entry);
      LOG.trace("Added application to mds: id: {}, spec: {}", id.getId(), jsonSpec);
    } else {
      LOG.trace("Application exists in mds: id: {}, spec: {}",
                id.getId(), existing.getTextField(FieldTypes.Application.SPEC_JSON));
      MetaDataEntry entry = new MetaDataEntry(id.getAccountId(), null, FieldTypes.Application.ENTRY_TYPE, id.getId());
      entry.addField(FieldTypes.Application.SPEC_JSON, jsonSpec);

      metaDataStore.updateField(context, id.getAccountId(), null,
                                FieldTypes.Application.ENTRY_TYPE, id.getId(),
                                FieldTypes.Application.SPEC_JSON, jsonSpec, -1);
      LOG.trace("Updated application in mds: id: {}, spec: {}", id.getId(), jsonSpec);
    }

    // hack hack hack: time constraints. See details in metadataServiceHelper javadoc
    metadataServiceHelper.updateInMetadataService(id, spec);
  }

  @Override
  public int incFlowletInstances(final Id.Program id, final String flowletId, int delta)
    throws OperationException {

    LOG.trace("Increasing flowlet instances: account: {}, application: {}, flow: {}, flowlet: {}, instances to add: {}",
              id.getAccountId(), id.getApplicationId(), id.getId(), flowletId, delta);
    ApplicationSpecification appSpec = getAppSpecSafely(id);
    FlowSpecification flowSpec = getFlowSpecSafely(id, appSpec);
    FlowletDefinition flowletDef = getFlowletDefinitionSafely(flowSpec, flowletId, id);

    int instances = flowletDef.getInstances() + delta;
    if (instances < 0) {
      throw new IllegalArgumentException("cannot change number of flowlet instances to " + instances +
                                 ", current number: " + flowletDef.getInstances() + ", attempted to inc by: " + delta);
    }

    final FlowletDefinition adjustedFlowletDef = new FlowletDefinition(flowletDef, instances);
    ApplicationSpecification newAppSpec = replaceFlowletInAppSpec(appSpec, id, flowSpec, adjustedFlowletDef);

    addApplication(id.getApplication(), newAppSpec);

    LOG.trace("Increased flowlet instances: account: {}, application: {}, flow: {}, flowlet: {}, instances now: {}", id.getAccountId(), id.getApplicationId(), id.getId(), flowletId, instances);

    return instances;
  }

  private FlowletDefinition getFlowletDefinitionSafely(FlowSpecification flowSpec, String flowletId, Id.Program id) {
    FlowletDefinition flowletDef = flowSpec.getFlowlets().get(flowletId);
    if (flowletDef == null) {
      throw new IllegalArgumentException("no such flowlet @ account id: " + id.getAccountId() +
                                           ", app id: " + id.getApplication() +
                                           ", flow id: " + id.getId() +
                                           ", flowlet id: " + id.getId());
    }
    return flowletDef;
  }

  private FlowSpecification getFlowSpecSafely(Id.Program id, ApplicationSpecification appSpec) {
    FlowSpecification flowSpec = appSpec.getFlows().get(id.getId());
    if (flowSpec == null) {
      throw new IllegalArgumentException("no such flow @ account id: " + id.getAccountId() +
                                           ", app id: " + id.getApplication() +
                                           ", flow id: " + id.getId());
    }
    return flowSpec;
  }

  @Override
  public void remove(Id.Program id) throws OperationException {
    LOG.trace("Removing program: account: {}, application: {}, program: {}", id.getAccountId(), id.getApplicationId(), id.getId());
    ApplicationSpecification appSpec = getAppSpecSafely(id);
    ApplicationSpecification newAppSpec = removeFlowFromAppSpec(appSpec, id);
    addApplication(id.getApplication(), newAppSpec);

    try {
      metadataServiceHelper.deleteFlow(id);
    } catch (MetadataServiceException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void removeAllApplications(Id.Account id) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    LOG.trace("Removing all applications of account with id: {}", id.getId());
    List<MetaDataEntry> applications =
      metaDataStore.list(context, id.getId(), null, FieldTypes.Application.ENTRY_TYPE, null);

    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create();

    for (MetaDataEntry entry : applications) {
      ApplicationSpecification appSpec = adapter.fromJson(entry.getTextField(FieldTypes.Application.SPEC_JSON));
      removeAllFlowsFromMetadataStore(id, appSpec);
      removeAllProceduresFromMetadataStore(id, appSpec);
      metaDataStore.delete(context, id.getId(), null, FieldTypes.Application.ENTRY_TYPE, entry.getId());
    }
  }

  @Override
  public void removeAll(Id.Account id) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    LOG.trace("Removing all metadata of account with id: {}", id.getId());
    List<MetaDataEntry> applications =
      metaDataStore.list(context, id.getId(), null, FieldTypes.Application.ENTRY_TYPE, null);

    // removing apps
    for (MetaDataEntry entry : applications) {
      metaDataStore.delete(context, id.getId(), null, FieldTypes.Application.ENTRY_TYPE, entry.getId());
    }

    try {
      metadataServiceHelper.deleteAll(id);
    } catch (TException e) {
      throw Throwables.propagate(e);
    } catch (MetadataServiceException e) {
      throw Throwables.propagate(e);
    }
  }

  private void removeAllProceduresFromMetadataStore(Id.Account id, ApplicationSpecification appSpec) throws OperationException {
    for (ProcedureSpecification procedure : appSpec.getProcedures().values()) {
      try {
        metadataServiceHelper.deleteFlow(Id.Program.from(id.getId(), appSpec.getName(), procedure.getName()));
      } catch (MetadataServiceException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private void removeAllFlowsFromMetadataStore(Id.Account id, ApplicationSpecification appSpec) throws OperationException {
    for (FlowSpecification flow : appSpec.getFlows().values()) {
      try {
        metadataServiceHelper.deleteFlow(Id.Program.from(id.getId(), appSpec.getName(), flow.getName()));
      } catch (MetadataServiceException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private ApplicationSpecification getAppSpecSafely(Id.Program id) throws OperationException {
    ApplicationSpecification appSpec = getApplication(id.getApplication());
    if (appSpec == null) {
      throw new IllegalArgumentException("no such application @ account id: " + id.getAccountId() +
                                           ", app id: " + id.getApplication());
    }
    return appSpec;
  }

  private ApplicationSpecification replaceFlowletInAppSpec(final ApplicationSpecification appSpec,
                                                           final Id.Program id,
                                                           final FlowSpecification flowSpec,
                                                           final FlowletDefinition adjustedFlowletDef) {
    // as app spec is immutable we have to do this trick
    return replaceFlowInAppSpec(appSpec, id, new ForwardingFlowSpecification(flowSpec) {
      @Override
      public Map<String, FlowletDefinition> getFlowlets() {
        Map<String, FlowletDefinition> flowlets = Maps.newHashMap(super.getFlowlets());
        flowlets.put(adjustedFlowletDef.getFlowletSpec().getName(), adjustedFlowletDef);
        return flowlets;
      }
    });
  }

  private ApplicationSpecification replaceFlowInAppSpec(final ApplicationSpecification appSpec, final Id.Program id,
                                                        final FlowSpecification newFlowSpec) {
    // as app spec is immutable we have to do this trick
    return new ForwardingApplicationSpecification(appSpec) {
      @Override
      public Map<String, FlowSpecification> getFlows() {
        Map<String, FlowSpecification> flows = Maps.newHashMap(super.getFlows());
        flows.put(id.getId(), newFlowSpec);
        return flows;
      }
    };
  }

  private ApplicationSpecification removeFlowFromAppSpec(final ApplicationSpecification appSpec,
                                                           final Id.Program id) {
    return new ForwardingApplicationSpecification(appSpec) {
      @Override
      public Map<String, FlowSpecification> getFlows() {
        Map<String, FlowSpecification> flows = Maps.newHashMap(super.getFlows());
        flows.remove(id.getId());
        return flows;
      }
    };
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

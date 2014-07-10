/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.store;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Type;
import com.continuuity.app.store.Store;
import com.continuuity.archive.ArchiveBundler;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.OperationException;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.ForwardingApplicationSpecification;
import com.continuuity.internal.app.ForwardingFlowSpecification;
import com.continuuity.internal.app.ForwardingResourceSpecification;
import com.continuuity.internal.app.ForwardingRuntimeSpecification;
import com.continuuity.internal.app.ForwardingTwillSpecification;
import com.continuuity.internal.app.program.ProgramBundle;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.procedure.DefaultProcedureSpecification;
import com.continuuity.internal.service.DefaultServiceSpecification;
import com.continuuity.metadata.MetaDataEntry;
import com.continuuity.metadata.MetaDataTable;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the Store that ultimately places data into MetaDataTable.
 */
public class MDTBasedStore implements Store {
  private static final Logger LOG
    = LoggerFactory.getLogger(MDTBasedStore.class);

  /**
   * Helper class.
   */
  private final LocationFactory locationFactory;
  private final CConfiguration configuration;
  private final Gson gson = new Gson();

  /**
   * We use metaDataTable directly to store user actions history.
   */
  private MetaDataTable metaDataTable;

  @Inject
  public MDTBasedStore(CConfiguration configuration,
                       MetaDataTable metaDataTable,
                       LocationFactory locationFactory) {
    this.metaDataTable = metaDataTable;
    this.locationFactory = locationFactory;
    this.configuration = configuration;
  }

  /**
   * Loads a given program.
   *
   * @param id of the program
   * @param type of program
   * @return An instance of {@link Program} if found, null otherwise.
   * @throws IOException
   */
  @Override
  public Program loadProgram(Id.Program id, Type type) throws IOException {
    try {
      MetaDataEntry entry = metaDataTable.get(new OperationContext(id.getAccountId()), id.getAccountId(),
                                              null, FieldTypes.Application.ENTRY_TYPE, id.getApplicationId());
      if (entry == null) {
        return null;
      }

      String specTimestamp = entry.getTextField(FieldTypes.Application.TIMESTAMP);
      Preconditions.checkNotNull(specTimestamp);

      Location programLocation = getProgramLocation(id, type);
      Preconditions.checkArgument(Long.parseLong(specTimestamp) >= programLocation.lastModified(),
                                  "Newer program update time than the specification update time. " +
                                  "Application must be redeployed");

      return Programs.create(programLocation);
    } catch (OperationException e) {
      throw new IOException(e);
    }
  }

  /**
   * @return The {@link Location} of the given program.
   * @throws RuntimeException if program can't be found.
   */
  private Location getProgramLocation(Id.Program id, Type type) throws IOException {
    String appFabricOutputDir = configuration.get(Constants.AppFabric.OUTPUT_DIR,
                                                  System.getProperty("java.io.tmpdir"));
    return Programs.programLocation(locationFactory, appFabricOutputDir, id, type);
  }

  /**
   * Logs start of program run.
   *
   * @param id        Info about program
   * @param pid       run id
   * @param startTime start timestamp
   */
  @Override
  public void setStart(Id.Program id, final String pid, final long startTime) {
    // Create a temp entry that is keyed by accountId, applicationId and program run id.
    MetaDataEntry entry = new MetaDataEntry(id.getAccountId(), id.getApplicationId(),
                                            FieldTypes.ProgramRun.ENTRY_TYPE, pid);
    entry.addField(FieldTypes.ProgramRun.PROGRAM, id.getId());
    entry.addField(FieldTypes.ProgramRun.START_TS, String.valueOf(startTime));

    OperationContext context = new OperationContext(id.getAccountId());
    // perform insert, no conflict resolution
    try {
      metaDataTable.add(context, entry, false);
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Logs end of program run.
   *
   * @param id      id of program
   * @param pid     program run id
   * @param endTime end timestamp
   * @param state   State of program
   */
  @Override
  public void setStop(Id.Program id, final String pid, final long endTime, final String state) {
    Preconditions.checkArgument(state != null, "End state of program run should be defined");

    OperationContext context = new OperationContext(id.getAccountId());

    // During setStop the following actions are performed
    // 1. Read the temp entry that is keyed by accountId, applicationId and program run id.
    // 2. Add a new entry that is keyed by accountId, applicationId, ProgramId:ReverseTimestamp:ProgramRunId
    //     - This is done so that the program history can be scanned by reverse chronological order.
    // 3. Delete the temp entry that was created during start - since we no longer read the entry that is keyed
    //    only by runId during program history lookup.
    try {
      //Read the metadata entry that is keyed of accountId, applicationId, program run id.
      MetaDataEntry entry = metaDataTable.get(context, id.getAccountId(),
                                              id.getApplicationId(),
                                              FieldTypes.ProgramRun.ENTRY_TYPE,
                                              pid);
      Preconditions.checkNotNull(entry);
      String startTime = entry.getTextField(FieldTypes.ProgramRun.START_TS);

      Preconditions.checkNotNull(startTime);
      String timestampedProgramId = getTimestampedId(id.getId(), pid, Long.MAX_VALUE - Long.parseLong(startTime));
      //update new entry that is ordered by time.
      MetaDataEntry timeStampedEntry = new MetaDataEntry(id.getAccountId(),
                                               id.getApplicationId(),
                                               FieldTypes.ProgramRun.ENTRY_TYPE,
                                               timestampedProgramId);
      timeStampedEntry.addField(FieldTypes.ProgramRun.START_TS, startTime);
      timeStampedEntry.addField(FieldTypes.ProgramRun.END_TS, String.valueOf(endTime));
      timeStampedEntry.addField(FieldTypes.ProgramRun.END_STATE, state);
      timeStampedEntry.addField(FieldTypes.ProgramRun.RUN_ID, pid);

      metaDataTable.add(context, timeStampedEntry);

      //delete the entry with pid as one of the column values.
      metaDataTable.delete(context, id.getAccountId(), id.getApplicationId(), FieldTypes.ProgramRun.ENTRY_TYPE, pid);

      try {
        //delete old history data and ignore exceptions since it will be cleaned up in the next run.
        deleteOlderMetadataHistory(context, id);
      } catch (OperationException e) {
        LOG.warn("Operation exception while deleting older run history with pid {}", pid, e);
      }
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<RunRecord> getRunHistory(final Id.Program id, final long startTime, final long endTime, int limit)
                                       throws OperationException {
    OperationContext context = new OperationContext(id.getAccountId());
    List<MetaDataEntry> entries = metaDataTable.list(context,
                                                     id.getAccountId(),
                                                     id.getApplicationId(),
                                                     FieldTypes.ProgramRun.ENTRY_TYPE,
                                                     getTimestampedId(id.getId(), startTime),
                                                     getTimestampedId(id.getId(), endTime),
                                                     limit);
    List<RunRecord> runHistory = Lists.newArrayList();
    for (MetaDataEntry entry : entries) {
      String endTsStr = entry.getTextField(FieldTypes.ProgramRun.END_TS);
      String runId = entry.getTextField(FieldTypes.ProgramRun.RUN_ID);
      runHistory.add(new RunRecord(runId,
                                   Long.valueOf(entry.getTextField(FieldTypes.ProgramRun.START_TS)),
                                   Long.valueOf(endTsStr),
                                   entry.getTextField(FieldTypes.ProgramRun.END_STATE)));
    }
    return runHistory;
  }

  @Override
  public Table<Type, Id.Program, List<RunRecord>> getAllRunHistory(Id.Account account) throws OperationException {
    OperationContext context = new OperationContext(account.getId());
    LOG.trace("Removing all applications of account with id: {}", account.getId());
    List<MetaDataEntry> applications =
      metaDataTable.list(context, account.getId(), null, FieldTypes.Application.ENTRY_TYPE, null);

    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create();

    ImmutableTable.Builder<Type, Id.Program, List<RunRecord>> builder = ImmutableTable.builder();
    for (MetaDataEntry entry : applications) {
      ApplicationSpecification appSpec = adapter.fromJson(entry.getTextField(FieldTypes.Application.SPEC_JSON));
      for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
        Id.Program programId = Id.Program.from(account.getId(), appSpec.getName(), flowSpec.getName());
        List<RunRecord> runRecords = getRunRecords(programId);
        builder.put(Type.FLOW, programId, runRecords);
      }
      for (ProcedureSpecification procedureSpec : appSpec.getProcedures().values()) {
        Id.Program programId = Id.Program.from(account.getId(), appSpec.getName(), procedureSpec.getName());
        List<RunRecord> runRecords = getRunRecords(programId);
        builder.put(Type.PROCEDURE, programId, runRecords);
      }
    }
    return builder.build();
  }

  private List<RunRecord> getRunRecords(Id.Program programId) throws OperationException {
    return getRunRecords(programId, Integer.MAX_VALUE);
  }

  private List<RunRecord> getRunRecords(Id.Program programId, int limit) throws OperationException {
    List<RunRecord> runRecords = Lists.newArrayList();
    for (RunRecord runRecord : getRunHistory(programId, limit)) {
      runRecords.add(runRecord);
    }
    return runRecords;
  }

  private List<RunRecord> getRunHistory(Id.Program programId, int limit) throws OperationException {
    return getRunHistory(programId, Long.MIN_VALUE, Long.MAX_VALUE, limit);
  }

  @Override
  public void addApplication(final Id.Application id,
                             final ApplicationSpecification spec, Location appArchiveLocation)
    throws OperationException {

    storeAppToArchiveLocationMapping(id, appArchiveLocation);

    long updateTime = System.currentTimeMillis();
    storeAppSpec(id, spec, updateTime);
  }


  @Override
  public List<ProgramSpecification> getDeletedProgramSpecifications(Id.Application id,
                                                                    ApplicationSpecification appSpec)
                                                                    throws OperationException {

    List<ProgramSpecification> deletedProgramSpecs = Lists.newArrayList();

    OperationContext context = new OperationContext(id.getAccountId());
    MetaDataEntry existing = metaDataTable.get(context, id.getAccountId(), null,
                                               FieldTypes.Application.ENTRY_TYPE, id.getId());

    if (existing != null) {
      String json = existing.getTextField(FieldTypes.Application.SPEC_JSON);
      Preconditions.checkNotNull(json);

      ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create();
      ApplicationSpecification existingAppSpec = adapter.fromJson(json);

      ImmutableMap<String, ProgramSpecification> existingSpec = new ImmutableMap.Builder<String, ProgramSpecification>()
                                                                      .putAll(existingAppSpec.getMapReduce())
                                                                      .putAll(existingAppSpec.getWorkflows())
                                                                      .putAll(existingAppSpec.getFlows())
                                                                      .putAll(existingAppSpec.getProcedures())
                                                                      .putAll(existingAppSpec.getServices())
                                                                      .build();

      ImmutableMap<String, ProgramSpecification> newSpec = new ImmutableMap.Builder<String, ProgramSpecification>()
                                                                      .putAll(appSpec.getMapReduce())
                                                                      .putAll(appSpec.getWorkflows())
                                                                      .putAll(appSpec.getFlows())
                                                                      .putAll(appSpec.getProcedures())
                                                                      .putAll(appSpec.getServices())
                                                                      .build();


      MapDifference<String, ProgramSpecification> mapDiff = Maps.difference(existingSpec, newSpec);
      deletedProgramSpecs.addAll(mapDiff.entriesOnlyOnLeft().values());
    }

    return deletedProgramSpecs;
  }

  private void storeAppToArchiveLocationMapping(Id.Application id, Location appArchiveLocation)
    throws OperationException {
    // there always be an entry for application
    LOG.trace("Updating id to app archive location mapping: app id: {}, app location: {}",
              id.getId(), appArchiveLocation.toURI());

    OperationContext context = new OperationContext(id.getAccountId());
    MetaDataEntry existing = metaDataTable.get(context, id.getAccountId(), null,
                                               FieldTypes.Application.ENTRY_TYPE, id.getId());
    if (existing == null) {
      MetaDataEntry entry = new MetaDataEntry(id.getAccountId(), null, FieldTypes.Application.ENTRY_TYPE, id.getId());
      entry.addField(FieldTypes.Application.ARCHIVE_LOCATION, appArchiveLocation.toURI().toString());
      metaDataTable.add(context, entry);
    } else {
      metaDataTable.updateField(context, id.getAccountId(), null,
                                FieldTypes.Application.ENTRY_TYPE, id.getId(),
                                FieldTypes.Application.ARCHIVE_LOCATION, appArchiveLocation.toURI().toString(), -1);
    }

    LOG.trace("Updated id to app archive location mapping: app id: {}, app location: {}",
              id.getId(), appArchiveLocation.toURI());
  }

  private void storeAppSpec(Id.Application id, ApplicationSpecification spec, long timestamp)
    throws OperationException {
    ApplicationSpecificationAdapter adapter =
      ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    String jsonSpec = adapter.toJson(spec);

    OperationContext context = new OperationContext(id.getAccountId());
    LOG.trace("Application being stored: id: {}: spec: {}", id.getId(), jsonSpec);
    MetaDataEntry existing = metaDataTable.get(context, id.getAccountId(), null,
                                               FieldTypes.Application.ENTRY_TYPE, id.getId());
    if (existing == null) {
      MetaDataEntry entry = new MetaDataEntry(id.getAccountId(), null, FieldTypes.Application.ENTRY_TYPE, id.getId());
      entry.addField(FieldTypes.Application.SPEC_JSON, jsonSpec);
      entry.addField(FieldTypes.Application.TIMESTAMP, Long.toString(timestamp));
      metaDataTable.add(context, entry);
      LOG.trace("Added application to mds: id: {}, spec: {}", id.getId(), jsonSpec);
    } else {
      LOG.trace("Application exists in mds: id: {}, spec: {}", id.getId(),
                existing.getTextField(FieldTypes.Application.SPEC_JSON));

      metaDataTable.updateField(context, id.getAccountId(), null,
                                FieldTypes.Application.ENTRY_TYPE, id.getId(),
                                FieldTypes.Application.SPEC_JSON, jsonSpec, -1);
      metaDataTable.updateField(context, id.getAccountId(), null,
                                FieldTypes.Application.ENTRY_TYPE, id.getId(),
                                FieldTypes.Application.TIMESTAMP, Long.toString(timestamp), -1);
      LOG.trace("Updated application in mds: id: {}, spec: {}", id.getId(), jsonSpec);
    }
    for (DataSetSpecification dsSpec : spec.getDataSets().values()) {
      addDataset(id.getAccount(), dsSpec);
    }
    for (StreamSpecification stream : spec.getStreams().values()) {
      addStream(id.getAccount(), stream);
    }
  }

  @Override
  public void addDataset(Id.Account id, DataSetSpecification dsSpec) throws OperationException {
    String json = new Gson().toJson(dsSpec);
    OperationContext context = new OperationContext(id.getId());
    MetaDataEntry existing = metaDataTable.get(context, id.getId(), null,
                                               FieldTypes.DataSet.ENTRY_TYPE, dsSpec.getName());
    if (existing == null) {
      MetaDataEntry entry = new MetaDataEntry(id.getId(), null, FieldTypes.DataSet.ENTRY_TYPE, dsSpec.getName());
      entry.addField(FieldTypes.DataSet.SPEC_JSON, json);
      metaDataTable.add(context, entry, true);
    } else {
      metaDataTable.updateField(context, id.getId(), null, FieldTypes.DataSet.ENTRY_TYPE, dsSpec.getName(),
                                FieldTypes.DataSet.SPEC_JSON, json, -1);
    }
  }

  @Override
  public void removeDataSet(Id.Account id, String name) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    metaDataTable.delete(context, id.getId(), null, FieldTypes.DataSet.ENTRY_TYPE, name);
  }

  @Override
  public DataSetSpecification getDataSet(Id.Account id, String name) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    MetaDataEntry entry = metaDataTable.get(context, id.getId(), null, FieldTypes.DataSet.ENTRY_TYPE, name);
    return entry == null ? null : new Gson().fromJson(entry.getTextField(FieldTypes.DataSet.SPEC_JSON),
                                                      DataSetSpecification.class);
  }

  @Override
  public Collection<DataSetSpecification> getAllDataSets(Id.Account id) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    List<MetaDataEntry> entries = metaDataTable.list(context, id.getId(), null, FieldTypes.DataSet.ENTRY_TYPE, null);
    List<DataSetSpecification> specs = Lists.newArrayListWithExpectedSize(entries.size());
    for (MetaDataEntry entry : entries) {
      specs.add(new Gson().fromJson(entry.getTextField(FieldTypes.DataSet.SPEC_JSON), DataSetSpecification.class));
    }
    return specs;
  }

  @Override
    public void addStream(Id.Account id, StreamSpecification streamSpec) throws OperationException {
    String json = new Gson().toJson(streamSpec);
    OperationContext context = new OperationContext(id.getId());
    MetaDataEntry existing = metaDataTable.get(context, id.getId(), null,
                                               FieldTypes.Stream.ENTRY_TYPE, streamSpec.getName());
    if (existing == null) {
      MetaDataEntry entry = new MetaDataEntry(id.getId(), null, FieldTypes.Stream.ENTRY_TYPE, streamSpec.getName());
      entry.addField(FieldTypes.Stream.SPEC_JSON, json);
      metaDataTable.add(context, entry);
    } else {
      metaDataTable.updateField(context, id.getId(), null, FieldTypes.Stream.ENTRY_TYPE, streamSpec.getName(),
                                FieldTypes.Stream.SPEC_JSON, json, -1);
    }
  }

  @Override
  public void removeStream(Id.Account id, String name) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    metaDataTable.delete(context, id.getId(), null, FieldTypes.Stream.ENTRY_TYPE, name);
  }

  @Override
  public StreamSpecification getStream(Id.Account id, String name) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    MetaDataEntry entry = metaDataTable.get(context, id.getId(), null, FieldTypes.Stream.ENTRY_TYPE, name);
    return entry == null ? null : new Gson().fromJson(entry.getTextField(FieldTypes.Stream.SPEC_JSON),
                                                      StreamSpecification.class);
  }

  @Override
  public Collection<StreamSpecification> getAllStreams(Id.Account id) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    List<MetaDataEntry> entries = metaDataTable.list(context, id.getId(), null, FieldTypes.Stream.ENTRY_TYPE, null);
    List<StreamSpecification> specs = Lists.newArrayListWithExpectedSize(entries.size());
    for (MetaDataEntry entry : entries) {
      specs.add(new Gson().fromJson(entry.getTextField(FieldTypes.Stream.SPEC_JSON), StreamSpecification.class));
    }
    return specs;
  }

  @Override
  public void setFlowletInstances(final Id.Program id, final String flowletId, int count)
    throws OperationException {
    Preconditions.checkArgument(count > 0, "cannot change number of flowlet instances to negative number: " + count);

    LOG.trace("Setting flowlet instances: account: {}, application: {}, flow: {}, flowlet: {}, new instances count: {}",
              id.getAccountId(), id.getApplicationId(), id.getId(), flowletId, count);

    ApplicationSpecification newAppSpec = updateFlowletInstancesInAppSpec(id, flowletId, count);
    replaceAppSpecInProgramJar(id, newAppSpec, Type.FLOW);

    long timestamp = System.currentTimeMillis();
    storeAppSpec(id.getApplication(), newAppSpec, timestamp);

    LOG.trace("Set flowlet instances: account: {}, application: {}, flow: {}, flowlet: {}, instances now: {}",
              id.getAccountId(), id.getApplicationId(), id.getId(), flowletId, count);
  }

  /**
   * Gets number of instances of specific flowlet.
   *
   * @param id        flow id
   * @param flowletId flowlet id
   * @throws com.continuuity.data2.OperationException
   *
   */
  @Override
  public int getFlowletInstances(Id.Program id, String flowletId) throws OperationException {
    ApplicationSpecification appSpec = getAppSpecSafely(id);
    FlowSpecification flowSpec = getFlowSpecSafely(id, appSpec);
    FlowletDefinition flowletDef = getFlowletDefinitionSafely(flowSpec, flowletId, id);
    return flowletDef.getInstances();
  }

  private ApplicationSpecification updateFlowletInstancesInAppSpec(Id.Program id, String flowletId,
                                                                     int count)
    throws OperationException {
    ApplicationSpecification appSpec = getAppSpecSafely(id);


    FlowSpecification flowSpec = getFlowSpecSafely(id, appSpec);
    FlowletDefinition flowletDef = getFlowletDefinitionSafely(flowSpec, flowletId, id);

    final FlowletDefinition adjustedFlowletDef = new FlowletDefinition(flowletDef, count);
    return replaceFlowletInAppSpec(appSpec, id, flowSpec, adjustedFlowletDef);
  }

  @Override
  public int getProcedureInstances(Id.Program id) throws OperationException {
    ApplicationSpecification appSpec = getAppSpecSafely(id);
    ProcedureSpecification specification = getProcedureSpecSafely(id, appSpec);
    return specification.getInstances();
  }

  @Override
  public void setProcedureInstances(Id.Program id, int count) throws OperationException {
    Preconditions.checkArgument(count > 0, "cannot change number of program instances to negative number: " + count);

    ApplicationSpecification appSpec = getAppSpecSafely(id);
    ProcedureSpecification specification = getProcedureSpecSafely(id, appSpec);

    ProcedureSpecification newSpecification =  new DefaultProcedureSpecification(specification.getClassName(),
                                                                                 specification.getName(),
                                                                                 specification.getDescription(),
                                                                                 specification.getDataSets(),
                                                                                 specification.getProperties(),
                                                                                 specification.getResources(),
                                                                                 count);

    ApplicationSpecification newAppSpec = replaceProcedureInAppSpec(appSpec, id, newSpecification);
    replaceAppSpecInProgramJar(id, newAppSpec, Type.PROCEDURE);

    long timestamp = System.currentTimeMillis();
    storeAppSpec(id.getApplication(), newAppSpec, timestamp);

    LOG.trace("Setting program instances: account: {}, application: {}, procedure: {}, new instances count: {}",
              id.getAccountId(), id.getApplicationId(), id.getId(), count);

  }

  @Override
  public int getServiceRunnableInstances(Id.Program id, String runnable) throws OperationException {
    ApplicationSpecification appSpec = getAppSpecSafely(id);
    ServiceSpecification serviceSpec = getServiceSpecSafely(id, appSpec);
    RuntimeSpecification runtimeSpec = serviceSpec.getRunnables().get(runnable);
    return runtimeSpec.getResourceSpecification().getInstances();
  }

  @Override
  public void setServiceRunnableInstances(Id.Program id, String runnable, int count) throws OperationException {

    Preconditions.checkArgument(count > 0, "cannot change number of program instances to negative number: " + count);

    ApplicationSpecification appSpec = getAppSpecSafely(id);
    ServiceSpecification serviceSpec = getServiceSpecSafely(id, appSpec);

    RuntimeSpecification runtimeSpec = serviceSpec.getRunnables().get(runnable);
    if (runtimeSpec == null) {
      throw new IllegalArgumentException(String.format("Runnable not found, app: %s, service: %s, runnable %s",
                                                       id.getApplication(), id.getId(), runnable));
    }

    ResourceSpecification resourceSpec = replaceInstanceCount(runtimeSpec.getResourceSpecification(), count);
    RuntimeSpecification newRuntimeSpec = replaceResourceSpec(runtimeSpec, resourceSpec);

    Preconditions.checkNotNull(newRuntimeSpec);

    ApplicationSpecification newAppSpec = replaceServiceSpec(appSpec, id.getId(),
                                                             replaceRuntimeSpec(runnable, serviceSpec, newRuntimeSpec));
    replaceAppSpecInProgramJar(id, newAppSpec, Type.SERVICE);

    long timestamp = System.currentTimeMillis();
    storeAppSpec(id.getApplication(), newAppSpec, timestamp);

    LOG.trace("Setting program instances: account: {}, application: {}, service: {}, runnable: {}," +
              " new instances count: {}",
              id.getAccountId(), id.getApplicationId(), id.getId(), runnable, count);

  }

  private ResourceSpecification replaceInstanceCount(final ResourceSpecification spec,
                                                    final int instanceCount) {
    return new ForwardingResourceSpecification(spec) {
      @Override
      public int getInstances() {
        return instanceCount;
      }
    };
  }

  private RuntimeSpecification replaceResourceSpec(final RuntimeSpecification runtimeSpec,
                                                   final ResourceSpecification resourceSpec) {
    return new ForwardingRuntimeSpecification(runtimeSpec) {
      @Override
      public ResourceSpecification getResourceSpecification() {
        return resourceSpec;
      }
    };
  }

  private ApplicationSpecification replaceServiceSpec(final ApplicationSpecification appSpec,
                                                      final String serviceName,
                                                      final ServiceSpecification serviceSpecification) {
    return new ForwardingApplicationSpecification(appSpec) {
      @Override
      public Map<String, ServiceSpecification> getServices() {
        Map<String, ServiceSpecification> services = Maps.newHashMap(super.getServices());
        services.put(serviceName, serviceSpecification);
        return services;
      }
    };
  }

  private ServiceSpecification replaceRuntimeSpec(final String runnable, final ServiceSpecification spec,
                                                  final RuntimeSpecification runtimeSpec) {
    return new DefaultServiceSpecification(spec.getName(),
                                           new ForwardingTwillSpecification(spec) {
                                             @Override
                                             public Map<String, RuntimeSpecification> getRunnables() {
                                               Map<String, RuntimeSpecification> specs = Maps.newHashMap(
                                                                                          super.getRunnables());
                                               specs.put(runnable, runtimeSpec);
                                               return specs;
                                             }
                                           });
  }

  private void replaceAppSpecInProgramJar(Id.Program id, ApplicationSpecification appSpec, Type type) {
    try {
      Location programLocation = getProgramLocation(id, type);
      ArchiveBundler bundler = new ArchiveBundler(programLocation);

      Program program = Programs.create(programLocation);
      String className = program.getMainClassName();

      Location tmpProgramLocation = programLocation.getTempFile("");
      try {
        ProgramBundle.create(id.getApplication(), bundler, tmpProgramLocation, id.getId(), className, type, appSpec);

        Location movedTo = tmpProgramLocation.renameTo(programLocation);
        if (movedTo == null) {
          throw new RuntimeException("Could not replace program jar with the one with updated app spec, " +
                                       "original program file: " + programLocation.toURI() +
                                       ", was trying to replace with file: " + tmpProgramLocation.toURI());
        }
      } finally {
        if (tmpProgramLocation != null && tmpProgramLocation.exists()) {
          tmpProgramLocation.delete();
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
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

  private static FlowSpecification getFlowSpecSafely(Id.Program id, ApplicationSpecification appSpec) {
    FlowSpecification flowSpec = appSpec.getFlows().get(id.getId());
    if (flowSpec == null) {
      throw new IllegalArgumentException("no such flow @ account id: " + id.getAccountId() +
                                           ", app id: " + id.getApplication() +
                                           ", flow id: " + id.getId());
    }
    return flowSpec;
  }

  private ServiceSpecification getServiceSpecSafely(Id.Program id, ApplicationSpecification appSpec) {
    ServiceSpecification spec = appSpec.getServices().get(id.getId());
    if (spec == null) {
      throw new IllegalArgumentException("no such service @ account id: " + id.getAccountId() +
                                           ", app id: " + id.getApplication() +
                                           ", service id: " + id.getId());
    }
    return spec;
  }

  private ProcedureSpecification getProcedureSpecSafely(Id.Program id, ApplicationSpecification appSpec) {
    ProcedureSpecification procedureSpecification = appSpec.getProcedures().get(id.getId());
    if (procedureSpecification == null) {
      throw new IllegalArgumentException("no such procedure @ account id: " + id.getAccountId() +
                                           ", app id: " + id.getApplication() +
                                           ", procedure id: " + id.getId());
    }
    return procedureSpecification;
  }

  @Override
  public ApplicationSpecification removeApplication(Id.Application id) throws OperationException {
    LOG.trace("Removing application: account: {}, application: {}", id.getAccountId(), id.getId());
    ApplicationSpecification appSpec = getApplication(id);
    Preconditions.checkNotNull(appSpec, "No such application: %s", id.getId());
    removeApplicationFromAppSpec(id.getAccount(), appSpec);
    return appSpec;
  }

  @Override
  public void removeAllApplications(Id.Account id) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    LOG.trace("Removing all applications of account with id: {}", id.getId());
    List<MetaDataEntry> applications =
      metaDataTable.list(context, id.getId(), null, FieldTypes.Application.ENTRY_TYPE, null);

    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create();

    for (MetaDataEntry entry : applications) {
      removeApplicationFromAppSpec(id, adapter.fromJson(entry.getTextField(FieldTypes.Application.SPEC_JSON)));
    }
  }

  @Override
  public void removeAll(Id.Account id) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    LOG.trace("Removing all metadata of account with id: {}", id.getId());
    List<MetaDataEntry> applications =
      metaDataTable.list(context, id.getId(), null, FieldTypes.Application.ENTRY_TYPE, null);

    // removing apps
    for (MetaDataEntry entry : applications) {
      metaDataTable.delete(context, id.getId(), null, FieldTypes.Application.ENTRY_TYPE, entry.getId());
    }
  }

  @Override
  public void storeRunArguments(Id.Program id, Map<String, String> arguments)  throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    MetaDataEntry existing = metaDataTable.get(context, id.getAccountId(), id.getApplicationId(),
                                               FieldTypes.ProgramRun.ARGS, id.getId());
    if (existing == null) {

      MetaDataEntry entry = new MetaDataEntry(id.getAccountId(), id.getApplicationId(),
                                              FieldTypes.ProgramRun.ARGS, id.getId());
      entry.addField(FieldTypes.ProgramRun.ENTRY_TYPE, gson.toJson(arguments));
      metaDataTable.add(context, entry);
      LOG.trace("Added run time arguments to mds: id: {}, app: {}, prog: {} ", id.getAccountId(),
                id.getApplicationId(), id.getId());
    } else {
      LOG.trace("Run time args exists in mds: id: {}, app: {}, prog: {}", id.getAccountId(),
                id.getApplicationId(), id.getId());

      metaDataTable.updateField(context, id.getAccountId(), id.getApplicationId(),
                                FieldTypes.ProgramRun.ARGS, id.getId(),
                                FieldTypes.ProgramRun.ENTRY_TYPE, gson.toJson(arguments), -1);
      LOG.trace("Updated application in mds: id: {}, app: {}, prog: {}", id.getId(),
                id.getApplicationId(), id.getId());
    }
  }

  @Override
  public Map<String, String> getRunArguments(Id.Program id) throws OperationException {

    OperationContext context = new OperationContext(id.getId());
    MetaDataEntry existing = metaDataTable.get(context, id.getAccountId(), id.getApplicationId(),
                                               FieldTypes.ProgramRun.ARGS, id.getId());
    Map<String, String> args = Maps.newHashMap();
    if (existing != null) {
      java.lang.reflect.Type type = new TypeToken<Map<String, String>>() { }.getType();
      args = gson.fromJson(existing.getTextField(FieldTypes.ProgramRun.ENTRY_TYPE), type);
    }
    return args;
  }

  private void removeApplicationFromAppSpec(Id.Account id, ApplicationSpecification appSpec) throws OperationException {
    OperationContext context = new OperationContext(id.getId());

    for (String flow : appSpec.getFlows().keySet()) {
      metaDataTable.delete(context, id.getId(), appSpec.getName(), FieldTypes.ProgramRun.ARGS, flow);
    }

    for (String mapreduce : appSpec.getMapReduce().keySet()) {
      metaDataTable.delete(context, id.getId(), appSpec.getName(), FieldTypes.ProgramRun.ARGS, mapreduce);
    }

    for (String procedure : appSpec.getProcedures().keySet()) {
      metaDataTable.delete(context, id.getId(), appSpec.getName(), FieldTypes.ProgramRun.ARGS, procedure);
    }

    for (String workflow : appSpec.getWorkflows().keySet()) {
      metaDataTable.delete(context, id.getId(), appSpec.getName(), FieldTypes.ProgramRun.ARGS, workflow);
    }

    metaDataTable.delete(context, id.getId(), null, FieldTypes.Application.ENTRY_TYPE, appSpec.getName());
  }

  private ApplicationSpecification getAppSpecSafely(Id.Program id) throws OperationException {
    ApplicationSpecification appSpec = getApplication(id.getApplication());
    if (appSpec == null) {
      throw new IllegalArgumentException("no such application @ account id: " + id.getAccountId() +
                                           ", app id: " + id.getApplication().getId());
    }
    return appSpec;
  }

  private ApplicationSpecification replaceInAppSpec(final ApplicationSpecification appSpec,
                                                    final Id.Program id,
                                                    final FlowSpecification flowSpec,
                                                    final FlowletDefinition adjustedFlowletDef,
                                                    final List<FlowletConnection> connections) {
    // as app spec is immutable we have to do this trick
    return replaceFlowInAppSpec(appSpec, id, new ForwardingFlowSpecification(flowSpec) {
      @Override
      public List<FlowletConnection> getConnections() {
        return connections;
      }

      @Override
      public Map<String, FlowletDefinition> getFlowlets() {
        Map<String, FlowletDefinition> flowlets = Maps.newHashMap(super.getFlowlets());
        flowlets.put(adjustedFlowletDef.getFlowletSpec().getName(), adjustedFlowletDef);
        return flowlets;
      }
    });
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

  private ApplicationSpecification replaceProcedureInAppSpec(final ApplicationSpecification appSpec,
                                                             final Id.Program id,
                                                             final ProcedureSpecification procedureSpecification) {
    // replace the new procedure spec.
    return new ForwardingApplicationSpecification(appSpec) {
      @Override
      public Map<String, ProcedureSpecification> getProcedures() {
        Map<String, ProcedureSpecification> procedures = Maps.newHashMap(super.getProcedures());
        procedures.put(id.getId(), procedureSpecification);
        return procedures;
      }
    };
  }


  @Override
  public ApplicationSpecification getApplication(final Id.Application id) throws OperationException {
    OperationContext context = new OperationContext(id.getAccountId());
    MetaDataEntry entry = metaDataTable.get(context, id.getAccountId(), null, FieldTypes.Application.ENTRY_TYPE,
                                            id.getId());
    if (entry == null) {
      return null;
    }
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create();
    return adapter.fromJson(entry.getTextField(FieldTypes.Application.SPEC_JSON));
  }

  @Override
  public Collection<ApplicationSpecification> getAllApplications(final Id.Account id) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    List<MetaDataEntry> entries = metaDataTable.list(context, id.getId(), null, FieldTypes.Application.ENTRY_TYPE,
                                                     null);
    if (entries == null) {
      return null;
    }
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create();
    List<ApplicationSpecification> specs = Lists.newArrayListWithExpectedSize(entries.size());
    for (MetaDataEntry entry : entries) {
      specs.add(adapter.fromJson(entry.getTextField(FieldTypes.Application.SPEC_JSON)));
    }
    return specs;
  }

  @Override
  public Location getApplicationArchiveLocation(Id.Application id) throws OperationException {
    OperationContext context = new OperationContext(id.getAccountId());
    MetaDataEntry entry = metaDataTable.get(context, id.getAccountId(), null, FieldTypes.Application.ENTRY_TYPE,
                                            id.getId());

    if (entry == null) {
      return null;
    }

    return locationFactory.create(URI.create(entry.getTextField(FieldTypes.Application.ARCHIVE_LOCATION)));
  }

  private String getTimestampedId(String id, long timestamp) {
    return String.format("%s:%d", id, timestamp);
  }

  private String getTimestampedId(String id, String pid,  long timestamp) {
    return String.format("%s:%d:%s", id, timestamp, pid);
  }

  //delete history for older dates
  private void deleteOlderMetadataHistory(OperationContext context, Id.Program id) throws OperationException {
    //delete stale history
    // Delete all entries that are greater than RUN_HISTORY_KEEP_DAYS to Long.MAX_VALUE

    int historyKeepDays = configuration.getInt(Constants.CFG_RUN_HISTORY_KEEP_DAYS,
                                               Constants.DEFAULT_RUN_HISTORY_KEEP_DAYS);

    long deleteStartTime = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS) -
                           (historyKeepDays * 24 * 60 * 60L);

    String deleteStartKey = getTimestampedId(id.getId(), Long.MAX_VALUE - deleteStartTime);
    String deleteStopKey = getTimestampedId(id.getId(), Long.MAX_VALUE);

    List<MetaDataEntry> entries = metaDataTable.list(context, id.getAccountId(), id.getApplicationId(),
                                                     FieldTypes.ProgramRun.ENTRY_TYPE, deleteStartKey,
                                                     deleteStopKey, Integer.MAX_VALUE);
    if (entries.size() > 0) {
      metaDataTable.delete(id.getAccountId(), entries);
    }
  }

  @Override
  public void changeFlowletSteamConnection(Id.Program flow, String flowletId, String oldValue, String newValue)
    throws OperationException {

    Preconditions.checkArgument(flow != null, "flow cannot be null");
    Preconditions.checkArgument(flowletId != null, "flowletId cannot be null");
    Preconditions.checkArgument(oldValue != null, "oldValue cannot be null");
    Preconditions.checkArgument(newValue != null, "newValue cannot be null");

    LOG.trace("Changing flowlet stream connection: account: {}, application: {}, flow: {}, flowlet: {}," +
                " old coonnected stream: {}, new connected stream: {}",
               flow.getAccountId(), flow.getApplicationId(), flow.getId(), flowletId, oldValue, newValue);

    ApplicationSpecification appSpec = getAppSpecSafely(flow);

    FlowSpecification flowSpec = getFlowSpecSafely(flow, appSpec);

    boolean adjusted = false;
    List<FlowletConnection> conns = Lists.newArrayList();
    for (FlowletConnection con : flowSpec.getConnections()) {
      if (FlowletConnection.Type.STREAM == con.getSourceType() &&
          flowletId.equals(con.getTargetName()) &&
          oldValue.equals(con.getSourceName())) {

        conns.add(new FlowletConnection(con.getSourceType(), newValue, con.getTargetName()));
        adjusted = true;
      } else {
        conns.add(con);
      }
    }

    if (!adjusted) {
      throw new IllegalArgumentException(
        String.format("Cannot change stream connection to %s, the connection to be changed is not found," +
          " account: %s, application: %s, flow: %s, flowlet: %s, source stream: %s",
                      newValue, flow.getAccountId(), flow.getApplicationId(), flow.getId(), flowletId, oldValue));
    }

    FlowletDefinition flowletDef = getFlowletDefinitionSafely(flowSpec, flowletId, flow);
    FlowletDefinition newFlowletDef = new FlowletDefinition(flowletDef, oldValue, newValue);
    ApplicationSpecification newAppSpec = replaceInAppSpec(appSpec, flow, flowSpec, newFlowletDef, conns);

    replaceAppSpecInProgramJar(flow, newAppSpec, Type.FLOW);

    long timestamp = System.currentTimeMillis();
    storeAppSpec(flow.getApplication(), newAppSpec, timestamp);

    LOG.trace("Changed flowlet stream connection: account: {}, application: {}, flow: {}, flowlet: {}," +
                " old coonnected stream: {}, new connected stream: {}",
              flow.getAccountId(), flow.getApplicationId(), flow.getId(), flowletId, oldValue, newValue);

    // todo: change stream "used by" flow mapping in metadata?
  }
}

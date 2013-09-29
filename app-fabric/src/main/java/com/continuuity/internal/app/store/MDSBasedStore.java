/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.store;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Type;
import com.continuuity.app.store.Store;
import com.continuuity.archive.ArchiveBundler;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.metadata.MetaDataEntry;
import com.continuuity.metadata.MetaDataStore;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.ForwardingApplicationSpecification;
import com.continuuity.internal.app.ForwardingFlowSpecification;
import com.continuuity.internal.app.program.ProgramBundle;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.metadata.MetadataHelper;
import com.continuuity.metadata.MetadataServiceException;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Implementation of the Store that ultimately places data into
 * MetaDataTable (thru MetaDataStore or directly).
 */
public class MDSBasedStore implements Store {
  private static final Logger LOG
    = LoggerFactory.getLogger(MDSBasedStore.class);

  private static final RunRecordComparator PROGRAM_RUN_RECORD_START_TIME_COMPARATOR =
    new RunRecordComparator();

  /**
   * Helper class.
   */
  private final MetadataServiceHelper metadataServiceHelper;

  private final LocationFactory locationFactory;

  private final CConfiguration configuration;

  private final Gson gson;
  /**
   * We use metaDataTable directly to store user actions history.
   */
  private MetaDataTable metaDataTable;

  @Inject
  public MDSBasedStore(CConfiguration configuration,
                       MetaDataTable metaDataTable,
                       MetaDataStore metaDataStore,
                       LocationFactory locationFactory) {
    this.metaDataTable = metaDataTable;
    this.metadataServiceHelper = new MetadataServiceHelper(metaDataStore);
    this.locationFactory = locationFactory;
    this.configuration = configuration;
    gson = new Gson();
  }

  /**
   * Loads a given program.
   *
   * @param id of the program
   * @param type of program
   * @return An instance of {@link Program} if found.
   * @throws IOException
   */
  @Override
  public Program loadProgram(Id.Program id, Type type) throws IOException {
    Location programLocation = getProgramLocation(id, type);
    return Programs.create(programLocation);
  }

  /**
   * @return The {@link Location} of the given program.
   * @throws RuntimeException if program can't be found.
   */
  private Location getProgramLocation(Id.Program id, Type type) throws IOException {
    Location allAppsLocation = locationFactory.create(configuration.get(Constants.AppFabric.OUTPUT_DIR,
                                                                        System.getProperty("java.io.tmpdir")));
    Location accountAppsLocation = allAppsLocation.append(id.getAccountId());
    String name = String.format(Locale.ENGLISH, "%s/%s", type.toString(), id.getApplicationId());
    Location applicationProgramsLocation = accountAppsLocation.append(name);
    if (!applicationProgramsLocation.exists()) {
      throw new RuntimeException("Unable to locate the Program,  location doesn't exist: "
                                   + applicationProgramsLocation.toURI().getPath());
    }
    Location programLocation = applicationProgramsLocation.append(String.format("%s.jar", id.getId()));
    if (!programLocation.exists()) {
      throw new RuntimeException(String.format("Program %s.%s of type %s does not exists.",
                                               id.getApplication(), id.getId(), type));
    }
    return programLocation;
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
   * @param pid     run id
   * @param endTime end timestamp
   * @param state   State of program
   */
  @Override
  public void setStop(Id.Program id, final String pid, final long endTime, final String state) {
    Preconditions.checkArgument(state != null, "End state of program run should be defined");

    OperationContext context = new OperationContext(id.getAccountId());

    // we want program run info to be in one entry to make things cleaner on reading end
    try {
      metaDataTable.updateField(context, id.getAccountId(), id.getApplicationId(),
                                FieldTypes.ProgramRun.ENTRY_TYPE, pid,
                                FieldTypes.ProgramRun.END_TS, String.valueOf(endTime), -1);
      metaDataTable.updateField(context, id.getAccountId(), id.getApplicationId(), FieldTypes.ProgramRun.ENTRY_TYPE,
                                pid, FieldTypes.ProgramRun.END_STATE, state, -1);
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Given a program returns the history of it's run.
   *
   * @param id program id
   * @return list of run record.
   * @throws OperationException
   */
  @Override
  public List<RunRecord> getRunHistory(final Id.Program id) throws OperationException {
    OperationContext context = new OperationContext(id.getAccountId());
    Map<String, String> filterByFields = new HashMap<String, String>();
    filterByFields.put(FieldTypes.ProgramRun.PROGRAM, id.getId());
    List<MetaDataEntry> entries = metaDataTable.list(context,
                                                     id.getAccountId(),
                                                     id.getApplicationId(),
                                                     FieldTypes.ProgramRun.ENTRY_TYPE, filterByFields);
    List<RunRecord> runHistory = Lists.newArrayList();
    for (MetaDataEntry entry : entries) {
      String endTsStr = entry.getTextField(FieldTypes.ProgramRun.END_TS);
      if (endTsStr == null) {
        // we need to return only those that finished
        continue;
      }
      runHistory.add(new RunRecord(entry.getId(),
                                   Long.valueOf(entry.getTextField(FieldTypes.ProgramRun.START_TS)),
                                   Long.valueOf(endTsStr),
                                   entry.getTextField(FieldTypes.ProgramRun.END_STATE)));
    }
    Collections.sort(runHistory, PROGRAM_RUN_RECORD_START_TIME_COMPARATOR);
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
    List<RunRecord> runRecords = Lists.newArrayList();
    for (RunRecord runRecord : getRunHistory(programId)) {
      runRecords.add(runRecord);
    }
    return runRecords;
  }

  /**
   * Compares RunRecord using their start time.
   */
  private static final class RunRecordComparator implements Comparator<RunRecord> {
    @Override
    public int compare(final RunRecord left, final RunRecord right) {
      if (left.getStartTs() > right.getStartTs()) {
        return 1;
      } else {
        return left.getStartTs() < right.getStartTs() ? -1 : 0;
      }
    }
  }

  @Override
  public void addApplication(final Id.Application id,
                             final ApplicationSpecification spec, Location appArchiveLocation)
    throws OperationException {
    storeAppSpec(id, spec);
    storeAppToArchiveLocationMapping(id, appArchiveLocation);
  }

  private void storeAppToArchiveLocationMapping(Id.Application id, Location appArchiveLocation)
    throws OperationException {
    // there always be an entry for application
    LOG.trace("Updating id to app archive location mapping: app id: {}, app location: {}",
              id.getId(), appArchiveLocation.toURI());

    OperationContext context = new OperationContext(id.getAccountId());
    metaDataTable.updateField(context, id.getAccountId(), null,
                              FieldTypes.Application.ENTRY_TYPE, id.getId(),
                              FieldTypes.Application.ARCHIVE_LOCATION, appArchiveLocation.toURI().getPath(), -1);
    LOG.trace("Updated id to app archive location mapping: app id: {}, app location: {}",
              id.getId(), appArchiveLocation.toURI());
  }

  private void storeAppSpec(Id.Application id, ApplicationSpecification spec) throws OperationException {
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

      metaDataTable.add(context, entry);
      LOG.trace("Added application to mds: id: {}, spec: {}", id.getId(), jsonSpec);
    } else {
      LOG.trace("Application exists in mds: id: {}, spec: {}", id.getId(),
                existing.getTextField(FieldTypes.Application.SPEC_JSON));

      metaDataTable.updateField(context, id.getAccountId(), null,
                                FieldTypes.Application.ENTRY_TYPE, id.getId(),
                                FieldTypes.Application.SPEC_JSON, jsonSpec, -1);
      LOG.trace("Updated application in mds: id: {}, spec: {}", id.getId(), jsonSpec);
    }

    // hack hack hack: time constraints. See details in metadataServiceHelper javadoc
    metadataServiceHelper.updateInMetadataService(id, spec);
  }

  @Override
  public void setFlowletInstances(final Id.Program id, final String flowletId, int count)
    throws OperationException {
    Preconditions.checkArgument(count > 0, "cannot change number of flowlet instances to negative number: " + count);

    LOG.trace("Setting flowlet instances: account: {}, application: {}, flow: {}, flowlet: {}, new instances count: {}",
              id.getAccountId(), id.getApplicationId(), id.getId(), flowletId, count);

    ApplicationSpecification newAppSpec = setFlowletInstancesInAppSpecInMDS(id, flowletId, count);
    replaceAppSpecInProgramJar(id, newAppSpec, Type.FLOW);

    LOG.trace("Set flowlet instances: account: {}, application: {}, flow: {}, flowlet: {}, instances now: {}",
              id.getAccountId(), id.getApplicationId(), id.getId(), flowletId, count);
  }

  /**
   * Gets number of instances of specific flowlet.
   *
   * @param id        flow id
   * @param flowletId flowlet id
   * @throws com.continuuity.api.data.OperationException
   *
   */
  @Override
  public int getFlowletInstances(Id.Program id, String flowletId) throws OperationException {
    ApplicationSpecification appSpec = getAppSpecSafely(id);
    FlowSpecification flowSpec = getFlowSpecSafely(id, appSpec);
    FlowletDefinition flowletDef = getFlowletDefinitionSafely(flowSpec, flowletId, id);
    return flowletDef.getInstances();
  }

  private ApplicationSpecification setFlowletInstancesInAppSpecInMDS(Id.Program id, String flowletId, int count)
    throws OperationException {
    ApplicationSpecification appSpec = getAppSpecSafely(id);


    FlowSpecification flowSpec = getFlowSpecSafely(id, appSpec);
    FlowletDefinition flowletDef = getFlowletDefinitionSafely(flowSpec, flowletId, id);

    final FlowletDefinition adjustedFlowletDef = new FlowletDefinition(flowletDef, count);
    ApplicationSpecification newAppSpec = replaceFlowletInAppSpec(appSpec, id, flowSpec, adjustedFlowletDef);

    storeAppSpec(id.getApplication(), newAppSpec);
    return newAppSpec;
  }

  private void replaceAppSpecInProgramJar(Id.Program id, ApplicationSpecification appSpec, Type type) {
    Location programLocation;
    try {
      programLocation = getProgramLocation(id, Type.FLOW);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    ArchiveBundler bundler = new ArchiveBundler(programLocation);

    String className = appSpec.getFlows().get(id.getId()).getClassName();
    try {
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
    LOG.trace("Removing program: account: {}, application: {}, program: {}", id.getAccountId(), id.getApplicationId(),
              id.getId());
    ApplicationSpecification appSpec = getAppSpecSafely(id);
    ApplicationSpecification newAppSpec = removeProgramFromAppSpec(appSpec, id);
    storeAppSpec(id.getApplication(), newAppSpec);

    // we don't know the type of the program so we'll try to remove any of Flow, Procedure or Mapreduce
    StringBuilder errorMessage = new StringBuilder(
      String.format("Removing program: account: %s, application: %s, program: %s. Trying every type of program... ",
                    id.getAccountId(), id.getApplicationId(), id.getId()));
    // Unfortunately with current MDS there's no way to say if we deleted anything. So we'll just rely on "no errors in
    // all attempts means we deleted smth". And yes, we show only latest error. And yes, we have to try remove
    // every type.
    MetadataServiceException error;
    try {
      metadataServiceHelper.deleteFlow(id);
      error = null;
    } catch (MetadataServiceException e) {
      error = e;
      LOG.warn(
        String.format("Error while trying to remove program (account: %s, application: %s, program: %s) as flow ",
                      id.getAccountId(), id.getApplicationId(), id.getId()),
        e);
      errorMessage.append("Could not remove as Flow (").append(e.getMessage()).append(")...");
    }

    try {
      metadataServiceHelper.deleteQuery(id);
      error = null;
    } catch (MetadataServiceException e) {
      if (error != null) {
        error = e;
      }
      LOG.warn(
        String.format("Error while trying to remove program (account: %s, application: %s, program: %s) as query ",
                      id.getAccountId(), id.getApplicationId(), id.getId()),
        e);
      errorMessage.append("Could not remove as Procedure (").append(e.getMessage()).append(")...");
    }

    try {
      metadataServiceHelper.deleteMapReduce(id);
      error = null;
    } catch (MetadataServiceException e) {
      if (error != null) {
        error = e;
      }
      LOG.warn(
        String.format("Error while trying to remove program (account: %s, application: %s, program: %s) as mapreduce ",
                      id.getAccountId(), id.getApplicationId(), id.getId()),
        e);
      errorMessage.append("Could not remove as Mapreduce (").append(e.getMessage()).append(")");
    }

    if (error != null) {
      throw new OperationException(StatusCode.ENTRY_NOT_FOUND, errorMessage.toString(), error);
    }
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

    try {
      metadataServiceHelper.deleteAll(id);
    } catch (TException e) {
      throw Throwables.propagate(e);
    } catch (MetadataServiceException e) {
      throw Throwables.propagate(e);
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
      java.lang.reflect.Type type = new TypeToken<Map<String, String>>(){}.getType();
      args = gson.fromJson(existing.getTextField(FieldTypes.ProgramRun.ENTRY_TYPE), type);
    }
    return args;
  }

  private void removeAllProceduresFromMetadataStore(Id.Account id, ApplicationSpecification appSpec)
    throws OperationException {
    for (ProcedureSpecification procedure : appSpec.getProcedures().values()) {
      try {
        metadataServiceHelper.deleteQuery(Id.Program.from(id.getId(), appSpec.getName(), procedure.getName()));
      } catch (MetadataServiceException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private void removeAllFlowsFromMetadataStore(Id.Account id, ApplicationSpecification appSpec)
    throws OperationException {
    for (FlowSpecification flow : appSpec.getFlows().values()) {
      try {
        metadataServiceHelper.deleteFlow(Id.Program.from(id.getId(), appSpec.getName(), flow.getName()));
      } catch (MetadataServiceException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private void removeAllMapreducesFromMetadataStore(Id.Account id, ApplicationSpecification appSpec)
    throws OperationException {
    for (MapReduceSpecification mrSpec : appSpec.getMapReduces().values()) {
      try {
        metadataServiceHelper.deleteMapReduce(Id.Program.from(id.getId(), appSpec.getName(), mrSpec.getName()));
      } catch (MetadataServiceException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private void removeApplicationFromAppSpec(Id.Account id, ApplicationSpecification appSpec) throws OperationException {
    OperationContext context = new OperationContext(id.getId());
    removeAllFlowsFromMetadataStore(id, appSpec);
    removeAllMapreducesFromMetadataStore(id, appSpec);
    removeAllProceduresFromMetadataStore(id, appSpec);
    metaDataTable.delete(context, id.getId(), null, FieldTypes.Application.ENTRY_TYPE, appSpec.getName());
    // make sure to also delete the "application" entry of MDS (by-passing MDS here). this will go away with MDS
    metadataServiceHelper.deleteApplication(id.getId(), appSpec.getName());
  }

  private ApplicationSpecification getAppSpecSafely(Id.Program id) throws OperationException {
    ApplicationSpecification appSpec = getApplication(id.getApplication());
    if (appSpec == null) {
      throw new IllegalArgumentException("no such application @ account id: " + id.getAccountId() +
                                           ", app id: " + id.getApplication().getId());
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

  private ApplicationSpecification removeProgramFromAppSpec(final ApplicationSpecification appSpec,
                                                            final Id.Program id) {
    // we try to remove from both procedures and flows as both of them are "programs"
    // this somewhat ugly api dictated by old UI
    return new ForwardingApplicationSpecification(appSpec) {
      @Override
      public Map<String, FlowSpecification> getFlows() {
        Map<String, FlowSpecification> flows = Maps.newHashMap(super.getFlows());
        flows.remove(id.getId());
        return flows;
      }

      @Override
      public Map<String, ProcedureSpecification> getProcedures() {
        Map<String, ProcedureSpecification> procedures = Maps.newHashMap(super.getProcedures());
        procedures.remove(id.getId());
        return procedures;
      }

      @Override
      public Map<String, MapReduceSpecification> getMapReduces() {
        Map<String, MapReduceSpecification> procedures = Maps.newHashMap(super.getMapReduces());
        procedures.remove(id.getId());
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
  public Location getApplicationArchiveLocation(Id.Application id) throws OperationException {
    OperationContext context = new OperationContext(id.getAccountId());
    MetaDataEntry entry = metaDataTable.get(context, id.getAccountId(), null, FieldTypes.Application.ENTRY_TYPE,
                                            id.getId());

    if (entry == null) {
      return null;
    }

    return locationFactory.create(entry.getTextField(FieldTypes.Application.ARCHIVE_LOCATION));
  }

}

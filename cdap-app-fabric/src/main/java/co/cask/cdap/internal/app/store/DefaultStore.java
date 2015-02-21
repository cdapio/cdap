/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.archive.ArchiveBundler;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.internal.app.ForwardingApplicationSpecification;
import co.cask.cdap.internal.app.ForwardingFlowSpecification;
import co.cask.cdap.internal.app.program.ProgramBundle;
import co.cask.cdap.internal.app.runtime.adapter.AdapterStatus;
import co.cask.cdap.internal.procedure.DefaultProcedureSpecification;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/**
 * Implementation of the Store that ultimately places data into MetaDataTable.
 */
public class DefaultStore implements Store {
  public static final String APP_META_TABLE = "app.meta";
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStore.class);
  private static final Id.DatasetInstance appMetaDatasetInstanceId =
    Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE, APP_META_TABLE);

  private final LocationFactory locationFactory;
  private final CConfiguration configuration;
  private final DatasetFramework dsFramework;

  private Transactional<AppMds, AppMetadataStore> txnl;

  @Inject
  public DefaultStore(CConfiguration conf,
                      LocationFactory locationFactory,
                      final TransactionSystemClient txClient,
                      DatasetFramework framework) {

    this.locationFactory = locationFactory;
    this.configuration = conf;
    this.dsFramework = new NamespacedDatasetFramework(framework, new DefaultDatasetNamespace(conf));

    txnl =
      Transactional.of(
        new TransactionExecutorFactory() {
          @Override
          public TransactionExecutor createExecutor(Iterable<TransactionAware> transactionAwares) {
            return new DefaultTransactionExecutor(txClient, transactionAwares);
          }},
        new Supplier<AppMds>() {
          @Override
          public AppMds get() {
            try {
              Table mdsTable = DatasetsUtil.getOrCreateDataset(dsFramework, appMetaDatasetInstanceId, "table",
                                                               DatasetProperties.EMPTY,
                                                               DatasetDefinition.NO_ARGUMENTS, null);
              return new AppMds(mdsTable);
            } catch (Exception e) {
              LOG.error("Failed to access app.meta table", e);
              throw Throwables.propagate(e);
            }
          }
        });
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by app mds.
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(Table.class.getName(), appMetaDatasetInstanceId, DatasetProperties.EMPTY);
  }

  @Nullable
  @Override
  public Program loadProgram(final Id.Program id, ProgramType type) throws IOException {
    ApplicationMeta appMeta = txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, ApplicationMeta>() {
      @Override
      public ApplicationMeta apply(AppMds mds) throws Exception {
        return mds.apps.getApplication(id.getNamespaceId(), id.getApplicationId());
      }
    });

    if (appMeta == null) {
      return null;
    }

    Location programLocation = getProgramLocation(id, type);
    // I guess this can happen when app is being deployed at the moment... todo: should be prevented by framework
    Preconditions.checkArgument(appMeta.getLastUpdateTs() >= programLocation.lastModified(),
                                "Newer program update time than the specification update time. " +
                                "Application must be redeployed");

    return Programs.create(programLocation);
  }

  @Override
  public void setStart(final Id.Program id, final String pid, final long startTime) {
    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        mds.apps.recordProgramStart(id.getNamespaceId(), id.getApplicationId(), id.getId(), pid, startTime);
        return null;
      }
    });
  }

  @Override
  public void setStop(final Id.Program id, final String pid, final long endTime, final ProgramController.State state) {
    Preconditions.checkArgument(state != null, "End state of program run should be defined");

    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        mds.apps.recordProgramStop(id.getNamespaceId(), id.getApplicationId(), id.getId(), pid, endTime, state);
        return null;
      }
    });



    // todo: delete old history data
  }

  @Override
  public List<RunRecord> getRuns(final Id.Program id, final ProgramRunStatus status,
                                 final long startTime, final long endTime, final int limit) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, List<RunRecord>>() {
      @Override
      public List<RunRecord> apply(AppMds mds) throws Exception {
        return mds.apps.getRuns(id.getNamespaceId(), id.getApplicationId(), id.getId(), status,
                                startTime, endTime, limit);
      }
    });
  }

  @Override
  public void addApplication(final Id.Application id,
                             final ApplicationSpecification spec, final Location appArchiveLocation) {

    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        mds.apps.writeApplication(id.getNamespaceId(), id.getId(), spec, appArchiveLocation.toURI().toString());

        for (StreamSpecification stream : spec.getStreams().values()) {
          mds.apps.writeStream(id.getNamespaceId(), stream);
        }

        return null;
      }
    });

  }

  // todo: this method should be moved into DeletedProgramHandlerState, bad design otherwise
  @Override
  public List<ProgramSpecification> getDeletedProgramSpecifications(final Id.Application id,
                                                                    ApplicationSpecification appSpec) {

    ApplicationMeta existing = txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, ApplicationMeta>() {
      @Override
      public ApplicationMeta apply(AppMds mds) throws Exception {
        return mds.apps.getApplication(id.getNamespaceId(), id.getId());
      }
    });

    List<ProgramSpecification> deletedProgramSpecs = Lists.newArrayList();

    if (existing != null) {
      ApplicationSpecification existingAppSpec = existing.getSpec();

      ImmutableMap<String, ProgramSpecification> existingSpec = new ImmutableMap.Builder<String, ProgramSpecification>()
                                                                      .putAll(existingAppSpec.getMapReduce())
                                                                      .putAll(existingAppSpec.getSpark())
                                                                      .putAll(existingAppSpec.getWorkflows())
                                                                      .putAll(existingAppSpec.getFlows())
                                                                      .putAll(existingAppSpec.getProcedures())
                                                                      .putAll(existingAppSpec.getServices())
                                                                      .build();

      ImmutableMap<String, ProgramSpecification> newSpec = new ImmutableMap.Builder<String, ProgramSpecification>()
                                                                      .putAll(appSpec.getMapReduce())
                                                                      .putAll(existingAppSpec.getSpark())
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

  @Override
  public void addStream(final Id.Namespace id, final StreamSpecification streamSpec) {
    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        mds.apps.writeStream(id.getId(), streamSpec);
        return null;
      }
    });
  }

  @Override
  public StreamSpecification getStream(final Id.Namespace id, final String name) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, StreamSpecification>() {
      @Override
      public StreamSpecification apply(AppMds mds) throws Exception {
        return mds.apps.getStream(id.getId(), name);
      }
    });
  }

  @Override
  public Collection<StreamSpecification> getAllStreams(final Id.Namespace id) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Collection<StreamSpecification>>() {
      @Override
      public Collection<StreamSpecification> apply(AppMds mds) throws Exception {
        return mds.apps.getAllStreams(id.getId());
      }
    });
  }

  @Override
  public void setFlowletInstances(final Id.Program id, final String flowletId, final int count) {
    Preconditions.checkArgument(count > 0, "cannot change number of flowlet instances to negative number: " + count);

    LOG.trace("Setting flowlet instances: namespace: {}, application: {}, flow: {}, flowlet: {}, " +
                "new instances count: {}", id.getNamespaceId(), id.getApplicationId(), id.getId(), flowletId, count);

    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
        ApplicationSpecification newAppSpec = updateFlowletInstancesInAppSpec(appSpec, id, flowletId, count);
        replaceAppSpecInProgramJar(id, newAppSpec, ProgramType.FLOW);

        mds.apps.updateAppSpec(id.getNamespaceId(), id.getApplicationId(), newAppSpec);
        return null;
      }
    });

    LOG.trace("Set flowlet instances: namespace: {}, application: {}, flow: {}, flowlet: {}, instances now: {}",
              id.getNamespaceId(), id.getApplicationId(), id.getId(), flowletId, count);
  }

  @Override
  public int getFlowletInstances(final Id.Program id, final String flowletId) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Integer>() {
      @Override
      public Integer apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
        FlowSpecification flowSpec = getFlowSpecOrFail(id, appSpec);
        FlowletDefinition flowletDef = getFlowletDefinitionOrFail(flowSpec, flowletId, id);
        return flowletDef.getInstances();
      }
    });

  }

  @Override
  public int getProcedureInstances(final Id.Program id) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Integer>() {
      @Override
      public Integer apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
        ProcedureSpecification specification = getProcedureSpecOrFail(id, appSpec);
        return specification.getInstances();
      }
    });
  }

  @Override
  public void setWorkerInstances(final Id.Program id, final int instances) {
    Preconditions.checkArgument(instances > 0, "cannot change number of program " +
      "instances to negative number: " + instances);
    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
        WorkerSpecification workerSpec = getWorkerSpecOrFail(id, appSpec);
        WorkerSpecification newSpecification = new WorkerSpecification(workerSpec.getClassName(),
                                                                       workerSpec.getName(),
                                                                       workerSpec.getDescription(),
                                                                       workerSpec.getProperties(),
                                                                       workerSpec.getDatasets(),
                                                                       workerSpec.getResources(),
                                                                       instances);
        ApplicationSpecification newAppSpec = replaceWorkerInAppSpec(appSpec, id, newSpecification);
        replaceAppSpecInProgramJar(id, newAppSpec, ProgramType.WORKER);
        mds.apps.updateAppSpec(id.getNamespaceId(), id.getApplicationId(), newAppSpec);
        return null;
      }
    });
  }

  @Override
  public void setProcedureInstances(final Id.Program id, final int count) {
    Preconditions.checkArgument(count > 0, "cannot change number of program instances to negative number: " + count);

    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
        ProcedureSpecification specification = getProcedureSpecOrFail(id, appSpec);

        ProcedureSpecification newSpecification =  new DefaultProcedureSpecification(specification.getClassName(),
                                                                                     specification.getName(),
                                                                                     specification.getDescription(),
                                                                                     specification.getDataSets(),
                                                                                     specification.getProperties(),
                                                                                     specification.getResources(),
                                                                                     count);

        ApplicationSpecification newAppSpec = replaceProcedureInAppSpec(appSpec, id, newSpecification);
        replaceAppSpecInProgramJar(id, newAppSpec, ProgramType.PROCEDURE);

        mds.apps.updateAppSpec(id.getNamespaceId(), id.getApplicationId(), newAppSpec);
        return null;
      }
    });

    LOG.trace("Setting program instances: namespace: {}, application: {}, procedure: {}, new instances count: {}",
              id.getNamespaceId(), id.getApplicationId(), id.getId(), count);
  }

  @Override
  public void setServiceInstances(final Id.Program id, final int instances) {
    Preconditions.checkArgument(instances > 0,
                                "cannot change number of program instances to negative number: %s", instances);

    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
        ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);

        // Create a new spec copy from the old one, except with updated instances number
        serviceSpec = new ServiceSpecification(serviceSpec.getClassName(), serviceSpec.getName(),
                                               serviceSpec.getDescription(), serviceSpec.getHandlers(),
                                               serviceSpec.getWorkers(), serviceSpec.getResources(), instances);

        ApplicationSpecification newAppSpec = replaceServiceSpec(appSpec, id.getId(), serviceSpec);
        replaceAppSpecInProgramJar(id, newAppSpec, ProgramType.SERVICE);

        mds.apps.updateAppSpec(id.getNamespaceId(), id.getApplicationId(), newAppSpec);
        return null;
      }
    });

    LOG.trace("Setting program instances: namespace: {}, application: {}, service: {}, new instances count: {}",
              id.getNamespaceId(), id.getApplicationId(), id.getId(), instances);
  }

  @Override
  public int getServiceInstances(final Id.Program id) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Integer>() {
      @Override
      public Integer apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
        ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);
        return serviceSpec.getInstances();
      }
    });
  }

  @Override
  public void setServiceWorkerInstances(final Id.Program id,
                                        final String workerName, final int instances) {
    Preconditions.checkArgument(instances > 0,
                                "cannot change number of program instances to negative number: %s", instances);

    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
        ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);
        ServiceWorkerSpecification workerSpec = getServiceWorkerSpecOrFail(id, serviceSpec, workerName);

        // Create a new worker spec copy from the old one, except with updated instances number
        workerSpec = new ServiceWorkerSpecification(workerSpec.getClassName(), workerSpec.getName(),
                                                    workerSpec.getDescription(), workerSpec.getProperties(),
                                                    workerSpec.getDatasets(), workerSpec.getResources(),
                                                    instances);

        // Create a new spec copy from the old one, except with updated worker spec
        Map<String, ServiceWorkerSpecification> updatedWorkers = Maps.newHashMap(serviceSpec.getWorkers());
        updatedWorkers.put(workerName, workerSpec);
        serviceSpec = new ServiceSpecification(serviceSpec.getClassName(), serviceSpec.getName(),
                                               serviceSpec.getDescription(), serviceSpec.getHandlers(),
                                               updatedWorkers, serviceSpec.getResources(), serviceSpec.getInstances());

        ApplicationSpecification newAppSpec = replaceServiceSpec(appSpec, id.getId(), serviceSpec);
        replaceAppSpecInProgramJar(id, newAppSpec, ProgramType.SERVICE);

        mds.apps.updateAppSpec(id.getNamespaceId(), id.getApplicationId(), newAppSpec);
        return null;
      }
    });

    LOG.trace("Setting program instances: namespace: {}, application: {}, service: {}, new instances count: {}",
              id.getNamespaceId(), id.getApplicationId(), id.getId(), instances);
  }

  @Override
  public int getWorkerInstances(final Id.Program id) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Integer>() {
      @Override
      public Integer apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
        WorkerSpecification workerSpec = getWorkerSpecOrFail(id, appSpec);
        return workerSpec.getInstances();
      }
    });
  }

  @Override
  public int getServiceWorkerInstances(final Id.Program id, final String workerName) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Integer>() {
      @Override
      public Integer apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
        ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);
        ServiceWorkerSpecification workerSpec = getServiceWorkerSpecOrFail(id, serviceSpec, workerName);
        return workerSpec.getInstances();
      }
    });
  }

  @Override
  public void removeApplication(final Id.Application id) {
    LOG.trace("Removing application: namespace: {}, application: {}", id.getNamespaceId(), id.getId());

    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        mds.apps.deleteApplication(id.getNamespaceId(), id.getId());
        mds.apps.deleteProgramArgs(id.getNamespaceId(), id.getId());
        mds.apps.deleteProgramHistory(id.getNamespaceId(), id.getId());
        return null;
      }
    });
  }

  @Override
  public void removeAllApplications(final Id.Namespace id) {
    LOG.trace("Removing all applications of namespace with id: {}", id.getId());

    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        mds.apps.deleteApplications(id.getId());
        mds.apps.deleteProgramArgs(id.getId());
        mds.apps.deleteProgramHistory(id.getId());
        return null;
      }
    });
  }

  @Override
  public void removeAll(final Id.Namespace id) {
    LOG.trace("Removing all applications of namespace with id: {}", id.getId());

    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        mds.apps.deleteApplications(id.getId());
        mds.apps.deleteProgramArgs(id.getId());
        mds.apps.deleteAllStreams(id.getId());
        mds.apps.deleteProgramHistory(id.getId());
        return null;
      }
    });
  }

  @Override
  public void storeRunArguments(final Id.Program id, final Map<String, String> arguments) {
    LOG.trace("Updated program args in mds: id: {}, app: {}, prog: {}, args: {}",
              id.getId(), id.getApplicationId(), id.getId(), Joiner.on(",").withKeyValueSeparator("=").join(arguments));

    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        mds.apps.writeProgramArgs(id.getNamespaceId(), id.getApplicationId(), id.getId(), arguments);
        return null;
      }
    });
  }

  @Override
  public Map<String, String> getRunArguments(final Id.Program id) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Map<String, String>>() {
      @Override
      public Map<String, String> apply(AppMds mds) throws Exception {
        ProgramArgs programArgs = mds.apps.getProgramArgs(id.getNamespaceId(), id.getApplicationId(), id.getId());
        return programArgs == null ? Maps.<String, String>newHashMap() : programArgs.getArgs();
      }
    });
  }

  @Nullable
  @Override
  public ApplicationSpecification getApplication(final Id.Application id) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, ApplicationSpecification>() {
      @Override
      public ApplicationSpecification apply(AppMds mds) throws Exception {
        return getApplicationSpec(mds, id);
      }
    });
  }

  @Override
  public Collection<ApplicationSpecification> getAllApplications(final Id.Namespace id) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Collection<ApplicationSpecification>>() {
      @Override
      public Collection<ApplicationSpecification> apply(AppMds mds) throws Exception {
        return Lists.transform(mds.apps.getAllApplications(id.getId()),
                               new Function<ApplicationMeta, ApplicationSpecification>() {
                                 @Override
                                 public ApplicationSpecification apply(ApplicationMeta input) {
                                   return input.getSpec();
                                 }
                               });
      }
    });
  }

  @Nullable
  @Override
  public Location getApplicationArchiveLocation(final Id.Application id) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Location>() {
      @Override
      public Location apply(AppMds mds) throws Exception {
        ApplicationMeta meta = mds.apps.getApplication(id.getNamespaceId(), id.getId());
        return meta == null ? null : locationFactory.create(URI.create(meta.getArchiveLocation()));
      }
    });
  }

  @Override
  public void changeFlowletSteamConnection(final Id.Program flow, final String flowletId,
                                           final String oldValue, final String newValue) {

    Preconditions.checkArgument(flow != null, "flow cannot be null");
    Preconditions.checkArgument(flowletId != null, "flowletId cannot be null");
    Preconditions.checkArgument(oldValue != null, "oldValue cannot be null");
    Preconditions.checkArgument(newValue != null, "newValue cannot be null");

    LOG.trace("Changing flowlet stream connection: namespace: {}, application: {}, flow: {}, flowlet: {}," +
                " old coonnected stream: {}, new connected stream: {}",
              flow.getNamespaceId(), flow.getApplicationId(), flow.getId(), flowletId, oldValue, newValue);

    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, flow);

        FlowSpecification flowSpec = getFlowSpecOrFail(flow, appSpec);

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
                            " namespace: %s, application: %s, flow: %s, flowlet: %s, source stream: %s",
                          newValue, flow.getNamespaceId(), flow.getApplicationId(), flow.getId(), flowletId, oldValue));
        }

        FlowletDefinition flowletDef = getFlowletDefinitionOrFail(flowSpec, flowletId, flow);
        FlowletDefinition newFlowletDef = new FlowletDefinition(flowletDef, oldValue, newValue);
        ApplicationSpecification newAppSpec = replaceInAppSpec(appSpec, flow, flowSpec, newFlowletDef, conns);

        replaceAppSpecInProgramJar(flow, newAppSpec, ProgramType.FLOW);

        Id.Application app = flow.getApplication();
        mds.apps.updateAppSpec(app.getNamespaceId(), app.getId(), newAppSpec);
        return null;
      }
    });


    LOG.trace("Changed flowlet stream connection: namespace: {}, application: {}, flow: {}, flowlet: {}," +
                " old coonnected stream: {}, new connected stream: {}",
              flow.getNamespaceId(), flow.getApplicationId(), flow.getId(), flowletId, oldValue, newValue);

    // todo: change stream "used by" flow mapping in metadata?
  }

  @Override
  public void addSchedule(final Id.Program program, final ScheduleSpecification scheduleSpecification) {
    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, program);
        Map<String, ScheduleSpecification> schedules = Maps.newHashMap(appSpec.getSchedules());
        String scheduleName = scheduleSpecification.getSchedule().getName();
        Preconditions.checkArgument(!schedules.containsKey(scheduleName), "Schedule with the name '" +
          scheduleName  + "' already exists.");
        schedules.put(scheduleSpecification.getSchedule().getName(), scheduleSpecification);
        ApplicationSpecification newAppSpec = new AppSpecificationWithChangedSchedules(appSpec, schedules);
        // TODO: double check this ProgramType.valueOf()
        replaceAppSpecInProgramJar(program, newAppSpec,
                                   ProgramType.valueOf(scheduleSpecification.getProgram().getProgramType().name()));
        mds.apps.updateAppSpec(program.getNamespaceId(), program.getApplicationId(), newAppSpec);
        return null;
      }
    });
  }

  @Override
  public void deleteSchedule(final Id.Program program, final SchedulableProgramType programType,
                             final String scheduleName) {
    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getAppSpecOrFail(mds, program);
        Map<String, ScheduleSpecification> schedules = Maps.newHashMap(appSpec.getSchedules());
        ScheduleSpecification removed = schedules.remove(scheduleName);
        if (removed == null) {
          throw new NoSuchElementException("no such schedule @ account id: " + program.getNamespaceId() +
                                             ", app id: " + program.getApplication() +
                                             ", program id: " + program.getId() +
                                             ", schedule name: " + scheduleName);
        }

        ApplicationSpecification newAppSpec = new AppSpecificationWithChangedSchedules(appSpec, schedules);
        // TODO: double check this ProgramType.valueOf()
        replaceAppSpecInProgramJar(program, newAppSpec, ProgramType.valueOf(programType.name()));
        mds.apps.updateAppSpec(program.getNamespaceId(), program.getApplicationId(), newAppSpec);
        return null;
      }
    });
  }

  private static class AppSpecificationWithChangedSchedules extends ForwardingApplicationSpecification {
    private final Map<String, ScheduleSpecification> newSchedules;

    private AppSpecificationWithChangedSchedules(ApplicationSpecification delegate,
                                                 Map<String, ScheduleSpecification> newSchedules) {
      super(delegate);
      this.newSchedules = newSchedules;
    }

    @Override
    public Map<String, ScheduleSpecification> getSchedules() {
      return newSchedules;
    }
  }


  @Override
  public boolean programExists(final Id.Program id, final ProgramType type) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Boolean>() {
      @Override
      public Boolean apply(AppMds mds) throws Exception {
        ApplicationSpecification appSpec = getApplicationSpec(mds, id.getApplication());
        if (appSpec == null) {
          return false;
        }
        ProgramSpecification programSpecification = null;
        try {
          if (type == ProgramType.FLOW) {
            programSpecification = getFlowSpecOrFail(id, appSpec);
          } else if (type == ProgramType.PROCEDURE) {
            programSpecification = getProcedureSpecOrFail(id, appSpec);
          } else if (type == ProgramType.SERVICE) {
            programSpecification = getServiceSpecOrFail(id, appSpec);
          } else if (type == ProgramType.WORKFLOW) {
            programSpecification = appSpec.getWorkflows().get(id.getId());
          } else if (type == ProgramType.MAPREDUCE) {
            programSpecification = appSpec.getMapReduce().get(id.getId());
          } else if (type == ProgramType.SPARK) {
            programSpecification = appSpec.getSpark().get(id.getId());
          } else if (type == ProgramType.WORKER) {
            programSpecification = appSpec.getWorkers().get(id.getId());
          } else if (type == ProgramType.WEBAPP) {
            // no-op
          } else {
            throw new IllegalArgumentException("Invalid ProgramType");
          }
        } catch (NoSuchElementException e) {
          programSpecification = null;
        } catch (Exception e) {
          Throwables.propagate(e);
        }
        return (programSpecification != null);
      }
    });
  }

  @Override
  @Nullable
  public NamespaceMeta createNamespace(final NamespaceMeta metadata) {
    Preconditions.checkArgument(metadata != null, "Namespace metadata cannot be null.");
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, NamespaceMeta>() {
      @Override
      public NamespaceMeta apply(AppMds input) throws Exception {
        Id.Namespace namespaceId = Id.Namespace.from(metadata.getId());
        NamespaceMeta existing = input.apps.getNamespace(namespaceId);
        if (existing != null) {
          return existing;
        }
        input.apps.createNamespace(metadata);
        return null;
      }
    });
  }

  @Override
  @Nullable
  public NamespaceMeta getNamespace(final Id.Namespace id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, NamespaceMeta>() {
      @Override
      public NamespaceMeta apply(AppMds input) throws Exception {
        return input.apps.getNamespace(id);
      }
    });
  }

  @Override
  @Nullable
  public NamespaceMeta deleteNamespace(final Id.Namespace id) {
    Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, NamespaceMeta>() {
      @Override
      public NamespaceMeta apply(AppMds input) throws Exception {
        NamespaceMeta existing = input.apps.getNamespace(id);
        if (existing != null) {
          input.apps.deleteNamespace(id);
        }
        return existing;
      }
    });
  }

  @Override
  public List<NamespaceMeta> listNamespaces() {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, List<NamespaceMeta>>() {
      @Override
      public List<NamespaceMeta> apply(AppMds input) throws Exception {
        return input.apps.listNamespaces();
      }
    });
  }


  @Override
  public void addAdapter(final Id.Namespace id, final AdapterSpecification adapterSpec) {
    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        mds.apps.writeAdapter(id, adapterSpec, AdapterStatus.STARTED);
        return null;
      }
    });
  }


  @Nullable
  @Override
  public AdapterSpecification getAdapter(final Id.Namespace id, final String name) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, AdapterSpecification>() {
      @Override
      public AdapterSpecification apply(AppMds mds) throws Exception {
        return mds.apps.getAdapter(id, name);
      }
    });
  }


  @Nullable
  @Override
  public AdapterStatus getAdapterStatus(final Id.Namespace id, final String name) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, AdapterStatus>() {
      @Override
      public AdapterStatus apply(AppMds mds) throws Exception {
        return mds.apps.getAdapterStatus(id, name);
      }
    });
  }

  @Nullable
  @Override
  public AdapterStatus setAdapterStatus(final Id.Namespace id, final String name, final AdapterStatus status) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, AdapterStatus>() {
      @Override
      public AdapterStatus apply(AppMds mds) throws Exception {
        return mds.apps.setAdapterStatus(id, name, status);
      }
    });
  }

  @Override
  public Collection<AdapterSpecification> getAllAdapters(final Id.Namespace id) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Collection<AdapterSpecification>>() {
      @Override
      public Collection<AdapterSpecification> apply(AppMds mds) throws Exception {
        return mds.apps.getAllAdapters(id);
      }
    });
  }

  @Override
  public void removeAdapter(final Id.Namespace id, final String name) {
    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        mds.apps.deleteAdapter(id, name);
        return null;
      }
    });
  }

  @Override
  public void removeAllAdapters(final Id.Namespace id) {
    txnl.executeUnchecked(new TransactionExecutor.Function<AppMds, Void>() {
      @Override
      public Void apply(AppMds mds) throws Exception {
        mds.apps.deleteAllAdapters(id);
        return null;
      }
    });
  }

  @VisibleForTesting
  void clear() throws Exception {
    DatasetAdmin admin = dsFramework.getAdmin(appMetaDatasetInstanceId, null);
    if (admin != null) {
      admin.truncate();
    }
  }

  /**
   * @return The {@link Location} of the given program.
   * @throws RuntimeException if program can't be found.
   */
  private Location getProgramLocation(Id.Program id, ProgramType type) throws IOException {
    String appFabricOutputDir = configuration.get(Constants.AppFabric.OUTPUT_DIR,
                                                  System.getProperty("java.io.tmpdir"));
    return Programs.programLocation(locationFactory, appFabricOutputDir, id, type);
  }

  private ApplicationSpecification getApplicationSpec(AppMds mds, Id.Application id) {
    ApplicationMeta meta = mds.apps.getApplication(id.getNamespaceId(), id.getId());
    return meta == null ? null : meta.getSpec();
  }

  private static ApplicationSpecification replaceServiceSpec(ApplicationSpecification appSpec,
                                                             String serviceName,
                                                             ServiceSpecification serviceSpecification) {
    return new ApplicationSpecificationWithChangedServices(appSpec, serviceName, serviceSpecification);
  }

  private static final class ApplicationSpecificationWithChangedServices extends ForwardingApplicationSpecification {
    private final String serviceName;
    private final ServiceSpecification serviceSpecification;

    private ApplicationSpecificationWithChangedServices(ApplicationSpecification delegate,
                                                        String serviceName, ServiceSpecification serviceSpecification) {
      super(delegate);
      this.serviceName = serviceName;
      this.serviceSpecification = serviceSpecification;
    }

    @Override
    public Map<String, ServiceSpecification> getServices() {
      Map<String, ServiceSpecification> services = Maps.newHashMap(super.getServices());
      services.put(serviceName, serviceSpecification);
      return services;
    }
  }

  private void replaceAppSpecInProgramJar(Id.Program id, ApplicationSpecification appSpec, ProgramType type) {
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

  private ServiceWorkerSpecification getServiceWorkerSpecOrFail(Id.Program id, ServiceSpecification serviceSpec,
                                                                String workerName) {
    ServiceWorkerSpecification workerSpec = serviceSpec.getWorkers().get(workerName);
    if (workerSpec == null) {
      throw new NoSuchElementException("no such worker @ namespace id: " + id.getNamespaceId() +
                                         ", app id: " + id.getApplication() +
                                         ", service id: " + id.getId() +
                                         ", worker id: " + workerName);
    }
    return workerSpec;
  }

  private static FlowletDefinition getFlowletDefinitionOrFail(FlowSpecification flowSpec,
                                                              String flowletId, Id.Program id) {
    FlowletDefinition flowletDef = flowSpec.getFlowlets().get(flowletId);
    if (flowletDef == null) {
      throw new NoSuchElementException("no such flowlet @ namespace id: " + id.getNamespaceId() +
                                           ", app id: " + id.getApplication() +
                                           ", flow id: " + id.getId() +
                                           ", flowlet id: " + flowletId);
    }
    return flowletDef;
  }

  private static FlowSpecification getFlowSpecOrFail(Id.Program id, ApplicationSpecification appSpec) {
    FlowSpecification flowSpec = appSpec.getFlows().get(id.getId());
    if (flowSpec == null) {
      throw new NoSuchElementException("no such flow @ namespace id: " + id.getNamespaceId() +
                                           ", app id: " + id.getApplication() +
                                           ", flow id: " + id.getId());
    }
    return flowSpec;
  }

  private static ServiceSpecification getServiceSpecOrFail(Id.Program id, ApplicationSpecification appSpec) {
    ServiceSpecification spec = appSpec.getServices().get(id.getId());
    if (spec == null) {
      throw new NoSuchElementException("no such service @ namespace id: " + id.getNamespaceId() +
                                           ", app id: " + id.getApplication() +
                                           ", service id: " + id.getId());
    }
    return spec;
  }

  private static WorkerSpecification getWorkerSpecOrFail(Id.Program id, ApplicationSpecification appSpec) {
    WorkerSpecification workerSpecification = appSpec.getWorkers().get(id.getId());
    if (workerSpecification == null) {
      throw new NoSuchElementException("no such worker @ namespace id: " + id.getNamespaceId() +
                                         ", app id: " + id.getApplication() +
                                         ", worker id: " + id.getId());
    }
    return workerSpecification;
  }

  private static ProcedureSpecification getProcedureSpecOrFail(Id.Program id, ApplicationSpecification appSpec) {
    ProcedureSpecification procedureSpecification = appSpec.getProcedures().get(id.getId());
    if (procedureSpecification == null) {
      throw new NoSuchElementException("no such procedure @ namespace id: " + id.getNamespaceId() +
                                           ", app id: " + id.getApplication() +
                                           ", procedure id: " + id.getId());
    }
    return procedureSpecification;
  }

  private static ApplicationSpecification updateFlowletInstancesInAppSpec(ApplicationSpecification appSpec,
                                                                          Id.Program id, String flowletId, int count) {

    FlowSpecification flowSpec = getFlowSpecOrFail(id, appSpec);
    FlowletDefinition flowletDef = getFlowletDefinitionOrFail(flowSpec, flowletId, id);

    final FlowletDefinition adjustedFlowletDef = new FlowletDefinition(flowletDef, count);
    return replaceFlowletInAppSpec(appSpec, id, flowSpec, adjustedFlowletDef);
  }

  private ApplicationSpecification getAppSpecOrFail(AppMds mds, Id.Program id) {
    ApplicationSpecification appSpec = getApplicationSpec(mds, id.getApplication());
    if (appSpec == null) {
      throw new NoSuchElementException("no such application @ namespace id: " + id.getNamespaceId() +
                                           ", app id: " + id.getApplication().getId());
    }
    return appSpec;
  }

  private static ApplicationSpecification replaceInAppSpec(final ApplicationSpecification appSpec,
                                                    final Id.Program id,
                                                    final FlowSpecification flowSpec,
                                                    final FlowletDefinition adjustedFlowletDef,
                                                    final List<FlowletConnection> connections) {
    // as app spec is immutable we have to do this trick
    return replaceFlowInAppSpec(appSpec, id,
                                new FlowSpecificationWithChangedFlowletsAndConnections(flowSpec,
                                                                                       adjustedFlowletDef,
                                                                                       connections));
  }

  private static class FlowSpecificationWithChangedFlowlets extends ForwardingFlowSpecification {
    private final FlowletDefinition adjustedFlowletDef;

    private FlowSpecificationWithChangedFlowlets(FlowSpecification delegate,
                                                 FlowletDefinition adjustedFlowletDef) {
      super(delegate);
      this.adjustedFlowletDef = adjustedFlowletDef;
    }

    @Override
    public Map<String, FlowletDefinition> getFlowlets() {
      Map<String, FlowletDefinition> flowlets = Maps.newHashMap(super.getFlowlets());
      flowlets.put(adjustedFlowletDef.getFlowletSpec().getName(), adjustedFlowletDef);
      return flowlets;
    }
  }

  private static final class FlowSpecificationWithChangedFlowletsAndConnections
    extends FlowSpecificationWithChangedFlowlets {

    private final List<FlowletConnection> connections;

    private FlowSpecificationWithChangedFlowletsAndConnections(FlowSpecification delegate,
                                                               FlowletDefinition adjustedFlowletDef,
                                                               List<FlowletConnection> connections) {
      super(delegate, adjustedFlowletDef);
      this.connections = connections;
    }

    @Override
    public List<FlowletConnection> getConnections() {
      return connections;
    }
  }

  private static ApplicationSpecification replaceFlowletInAppSpec(final ApplicationSpecification appSpec,
                                                           final Id.Program id,
                                                           final FlowSpecification flowSpec,
                                                           final FlowletDefinition adjustedFlowletDef) {
    // as app spec is immutable we have to do this trick
    return replaceFlowInAppSpec(appSpec, id, new FlowSpecificationWithChangedFlowlets(flowSpec, adjustedFlowletDef));
  }

  private static ApplicationSpecification replaceFlowInAppSpec(final ApplicationSpecification appSpec,
                                                               final Id.Program id,
                                                               final FlowSpecification newFlowSpec) {
    // as app spec is immutable we have to do this trick
    return new ApplicationSpecificationWithChangedFlows(appSpec, id.getId(), newFlowSpec);
  }

  private static final class ApplicationSpecificationWithChangedFlows extends ForwardingApplicationSpecification {
    private final FlowSpecification newFlowSpec;
    private final String flowId;

    private ApplicationSpecificationWithChangedFlows(ApplicationSpecification delegate,
                                                     String flowId, FlowSpecification newFlowSpec) {
      super(delegate);
      this.newFlowSpec = newFlowSpec;
      this.flowId = flowId;
    }

    @Override
    public Map<String, FlowSpecification> getFlows() {
      Map<String, FlowSpecification> flows = Maps.newHashMap(super.getFlows());
      flows.put(flowId, newFlowSpec);
      return flows;
    }
  }

  private static ApplicationSpecification replaceWorkerInAppSpec(final ApplicationSpecification appSpec,
                                                                 final Id.Program id,
                                                                 final WorkerSpecification workerSpecification) {
    return new ApplicationSpecificationWithChangedWorkers(appSpec, id.getId(), workerSpecification);
  }

  private static final class ApplicationSpecificationWithChangedWorkers extends ForwardingApplicationSpecification {
    private final String workerId;
    private final WorkerSpecification workerSpecification;

    private ApplicationSpecificationWithChangedWorkers(ApplicationSpecification delegate, String workerId,
                                                       WorkerSpecification workerSpec) {
      super(delegate);
      this.workerId = workerId;
      this.workerSpecification = workerSpec;
    }

    @Override
    public Map<String, WorkerSpecification> getWorkers() {
      Map<String, WorkerSpecification> workers = Maps.newHashMap(super.getWorkers());
      workers.put(workerId, workerSpecification);
      return workers;
    }
  }

  private static ApplicationSpecification replaceProcedureInAppSpec(
                                                             final ApplicationSpecification appSpec,
                                                             final Id.Program id,
                                                             final ProcedureSpecification procedureSpecification) {
    // replace the new procedure spec.
    return new ApplicationSpecificationWithChangedProcedure(appSpec, id.getId(), procedureSpecification);
  }

  private static final class ApplicationSpecificationWithChangedProcedure extends ForwardingApplicationSpecification {
    private final String procedureId;
    private final ProcedureSpecification procedureSpecification;

    private ApplicationSpecificationWithChangedProcedure(ApplicationSpecification delegate,
                                                         String procedureId,
                                                         ProcedureSpecification procedureSpecification) {
      super(delegate);
      this.procedureId = procedureId;
      this.procedureSpecification = procedureSpecification;
    }

    @Override
    public Map<String, ProcedureSpecification> getProcedures() {
      Map<String, ProcedureSpecification> procedures = Maps.newHashMap(super.getProcedures());
       procedures.put(procedureId, procedureSpecification);
      return procedures;
    }
  }

  private static final class AppMds implements Iterable<AppMetadataStore> {
    private final AppMetadataStore apps;

    private AppMds(Table mdsTable) {
      this.apps = new AppMetadataStore(mdsTable);
    }

    @Override
    public Iterator<AppMetadataStore> iterator() {
      return Iterators.singletonIterator(apps);
    }
  }
}

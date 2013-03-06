package com.continuuity.internal.test;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.DataSet;
import com.continuuity.app.Id;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.EntityType;
import com.continuuity.app.services.FlowDescriptor;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.archive.JarClassLoader;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SynchronousTransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.filesystem.Location;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.ProcedureClient;
import com.continuuity.test.ProcedureManager;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class DefaultApplicationManager implements ApplicationManager {

  private final ConcurrentMap<String, FlowIdentifier> runningProcessses = Maps.newConcurrentMap();
  private final AuthToken token;
  private final String accountId;
  private final String applicationId;
  private final AppFabricService.Iface appFabricServer;
  private final DataSetInstantiator dataSetInstantiator;
  private final StreamWriterFactory streamWriterFactory;
  private final ProcedureClientFactory procedureClientFactory;

  @Inject
  public DefaultApplicationManager(OperationExecutor opex,
                                   StreamWriterFactory streamWriterFactory,
                                   ProcedureClientFactory procedureClientFactory,
                                   @Assisted AuthToken token,
                                   @Assisted("accountId") String accountId,
                                   @Assisted("applicationId") String applicationId,
                                   @Assisted AppFabricService.Iface appFabricServer,
                                   @Assisted Location deployedJar,
                                   @Assisted ApplicationSpecification appSpec) {
    this.token = token;
    this.accountId = accountId;
    this.applicationId = applicationId;
    this.appFabricServer = appFabricServer;
    this.streamWriterFactory = streamWriterFactory;
    this.procedureClientFactory = procedureClientFactory;

    try {
      OperationContext ctx = new OperationContext(accountId, applicationId);
      DataFabric dataFabric = new DataFabricImpl(opex, ctx);
      TransactionProxy proxy = new TransactionProxy();
      proxy.setTransactionAgent(new SynchronousTransactionAgent(opex, ctx));

      this.dataSetInstantiator = new DataSetInstantiator(dataFabric, proxy, new JarClassLoader(deployedJar));
      this.dataSetInstantiator.setDataSets(ImmutableList.copyOf(appSpec.getDataSets().values()));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public FlowManager startFlow(final String flowName) {
    try {
      final FlowIdentifier flowId = new FlowIdentifier(accountId, applicationId, flowName, 0);
      Preconditions.checkState(runningProcessses.putIfAbsent(flowName, flowId) == null,
                               "Flow %s is already running", flowName);
      try {
        appFabricServer.start(token, new FlowDescriptor(flowId, ImmutableList.<String>of()));
      } catch (Exception e) {
        runningProcessses.remove(flowName);
        throw Throwables.propagate(e);
      }

      return new FlowManager() {
        @Override
        public void setFlowletInstances(String flowletName, int instances) {
          Preconditions.checkArgument(instances > 0, "Instance counter should be > 0.");
          try {
            appFabricServer.setInstances(token, flowId, flowletName, (short)instances);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public void stop() {
          try {
            if (runningProcessses.remove(flowName, flowId)) {
              appFabricServer.stop(token, flowId);
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ProcedureManager startProcedure(final String procedureName) {
    try {
      final FlowIdentifier procedureId = new FlowIdentifier(accountId, applicationId, procedureName, 0);
      procedureId.setType(EntityType.QUERY);
      Preconditions.checkState(runningProcessses.putIfAbsent(procedureName, procedureId) == null,
                               "Flow %s is already running", procedureName);
      try {
        appFabricServer.start(token, new FlowDescriptor(procedureId, ImmutableList.<String>of()));
      } catch (Exception e) {
        runningProcessses.remove(procedureName);
        throw Throwables.propagate(e);
      }

      return new ProcedureManager() {
        @Override
        public void stop() {
          try {
            if (runningProcessses.remove(procedureName, procedureId)) {
              appFabricServer.stop(token, procedureId);
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public ProcedureClient getClient() {
          return procedureClientFactory.create(accountId, applicationId, procedureName);
        }
      };
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public StreamWriter getStreamWriter(String streamName) {
    QueueName queueName = QueueName.fromStream(Id.Account.from(accountId), streamName);
    return streamWriterFactory.create(queueName, accountId, applicationId);
  }

  @Override
  public <T extends DataSet> T getDataSet(String dataSetName) {
    return dataSetInstantiator.getDataSet(dataSetName);
  }

  @Override
  public void stopAll() {
    try {
      for (Map.Entry<String, FlowIdentifier> entry : Iterables.consumingIterable(runningProcessses.entrySet())) {
        appFabricServer.stop(token, entry.getValue());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      RuntimeStats.clearStats(applicationId);
    }
  }
}

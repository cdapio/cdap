package com.continuuity.test.internal;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.DataSet;
import com.continuuity.app.services.ProgramDescriptor;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ProgramStatus;
import com.continuuity.common.queue.QueueName;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.EntityType;
import com.continuuity.archive.JarClassLoader;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.DataSetManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.MapReduceManager;
import com.continuuity.test.ProcedureClient;
import com.continuuity.test.ProcedureManager;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class DefaultApplicationManager implements ApplicationManager {

  private final ConcurrentMap<String, ProgramId> runningProcessses = Maps.newConcurrentMap();
  private final AuthToken token;
  private final String accountId;
  private final String applicationId;
  private final AppFabricService.Iface appFabricServer;
  private final TransactionSystemClient txSystemClient;
  private final DataSetInstantiator dataSetInstantiator;
  private final StreamWriterFactory streamWriterFactory;
  private final ProcedureClientFactory procedureClientFactory;

  @Inject
  public DefaultApplicationManager(LocationFactory locationFactory,
                                   DataSetAccessor dataSetAccessor,
                                   TransactionSystemClient txSystemClient,
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
    this.txSystemClient = txSystemClient;

    DataFabric dataFabric = new DataFabric2Impl(locationFactory, dataSetAccessor);

    try {
      // Since we expose the DataSet class, it has to be loaded using ClassLoader delegation.
      // The drawback is we'll not be able to instrument DataSet classes using ASM.
      this.dataSetInstantiator = new DataSetInstantiator(dataFabric,
                                                         new DataSetClassLoader(new JarClassLoader(deployedJar)));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    this.dataSetInstantiator.setDataSets(appSpec.getDataSets().values());
  }

  @Override
  public FlowManager startFlow(final String flowName) {
    return startFlow(flowName, ImmutableMap.<String, String>of());
  }

  @Override
  public FlowManager startFlow(final String flowName, Map<String, String> arguments) {
    try {
      final ProgramId flowId = new ProgramId(accountId, applicationId, flowName);
      Preconditions.checkState(runningProcessses.putIfAbsent(flowName, flowId) == null,
                               "Flow %s is already running", flowName);
      try {
        appFabricServer.start(token, new ProgramDescriptor(flowId, arguments));
      } catch (Exception e) {
        runningProcessses.remove(flowName);
        throw Throwables.propagate(e);
      }

      return new FlowManager() {
        @Override
        public void setFlowletInstances(String flowletName, int instances) {
          Preconditions.checkArgument(instances > 0, "Instance counter should be > 0.");
          try {
            appFabricServer.setFlowletInstances(token, flowId, flowletName, (short) instances);
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
  public MapReduceManager startMapReduce(final String jobName) {
    return startMapReduce(jobName, ImmutableMap.<String, String>of());
  }

  @Override
  public MapReduceManager startMapReduce(final String jobName, Map<String, String> arguments) {
    try {
      final ProgramId jobId = new ProgramId(accountId, applicationId, jobName);
      jobId.setType(EntityType.MAPREDUCE);

      // mapreduce job can stop by itself, so refreshing info about its state
      if (!isRunning(jobId)) {
        runningProcessses.remove(jobName);
      }

      Preconditions.checkState(runningProcessses.putIfAbsent(jobName, jobId) == null,
                               "MapReduce job %s is already running", jobName);
      try {
        appFabricServer.start(token, new ProgramDescriptor(jobId, arguments));
      } catch (Exception e) {
        runningProcessses.remove(jobName);
        throw Throwables.propagate(e);
      }

      return new MapReduceManager() {
        @Override
        public void stop() {
          try {
            if (runningProcessses.remove(jobName, jobId)) {
              appFabricServer.stop(token, jobId);
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public void waitForFinish(long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException {
          while (timeout > 0 && isRunning(jobId)) {
            timeoutUnit.sleep(1);
            timeout--;
          }

          if (timeout == 0 && isRunning(jobId)) {
            throw new TimeoutException("Time limit reached.");
          }

        }
      };
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ProcedureManager startProcedure(final String procedureName) {
    return startProcedure(procedureName, ImmutableMap.<String, String>of());
  }

  @Override
  public ProcedureManager startProcedure(final String procedureName, Map<String, String> arguments) {
    try {
      final ProgramId procedureId = new ProgramId(accountId, applicationId, procedureName);
      procedureId.setType(EntityType.PROCEDURE);
      Preconditions.checkState(runningProcessses.putIfAbsent(procedureName, procedureId) == null,
                               "Procedure %s is already running", procedureName);
      try {
        appFabricServer.start(token, new ProgramDescriptor(procedureId, arguments));
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
    QueueName queueName = QueueName.fromStream(streamName);
    return streamWriterFactory.create(queueName, accountId, applicationId);
  }

  @Override
  public <T extends DataSet> DataSetManager<T> getDataSet(String dataSetName) {
    final T dataSet = dataSetInstantiator.getDataSet(dataSetName);

    try {
      final TransactionContext txContext =
        new TransactionContext(txSystemClient, dataSetInstantiator.getTransactionAware());
      txContext.start();
      return new DataSetManager<T>() {
        @Override
        public T get() {
          return dataSet;
        }

        @Override
        public void flush() {
          try {
            txContext.finish();
            txContext.start();
          } catch (TransactionFailureException e) {
            throw Throwables.propagate(e);
          }
        }
      };
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stopAll() {
    try {
      for (Map.Entry<String, ProgramId> entry : Iterables.consumingIterable(runningProcessses.entrySet())) {
        // have to do a check, since mapreduce jobs could stop by themselves earlier, and appFabricServer.stop will
        // throw error when you stop smth that is not running.
        if (isRunning(entry.getValue())) {
          appFabricServer.stop(token, entry.getValue());
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      RuntimeStats.clearStats(applicationId);
    }
  }

  private static final class DataSetClassLoader extends ClassLoader {

    private final ClassLoader classLoader;

    private DataSetClassLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      return classLoader.loadClass(name);
    }
  }

  private boolean isRunning(ProgramId flowId) {
    try {
      ProgramStatus status = appFabricServer.status(token, flowId);
      // comparing to hardcoded string is ugly, but this is how appFabricServer works now to support legacy UI
      return "RUNNING".equals(status.getStatus());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}

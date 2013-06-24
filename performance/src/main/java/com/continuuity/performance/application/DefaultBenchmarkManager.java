package com.continuuity.performance.application;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.DataSet;
import com.continuuity.app.Id;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.EntityType;
import com.continuuity.app.services.FlowDescriptor;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.FlowStatus;
import com.continuuity.archive.JarClassLoader;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SynchronousTransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.test.app.ApplicationManager;
import com.continuuity.test.app.FlowManager;
import com.continuuity.test.app.MapReduceManager;
import com.continuuity.test.app.ProcedureClient;
import com.continuuity.test.app.ProcedureClientFactory;
import com.continuuity.test.app.ProcedureManager;
import com.continuuity.test.app.RuntimeStats;
import com.continuuity.test.app.StreamWriter;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default Benchmark Context.
 */
public class DefaultBenchmarkManager implements ApplicationManager {

  static final Logger LOG = LoggerFactory.getLogger(DefaultBenchmarkManager.class);

  private final ConcurrentMap<String, FlowIdentifier> runningProcessses = Maps.newConcurrentMap();
  private final AuthToken token;
  private final String accountId;
  private final String applicationId;
  private final AppFabricService.Iface appFabricServer;
  private final DataSetInstantiator dataSetInstantiator;
  private final BenchmarkStreamWriterFactory streamWriterFactory;
  private final ProcedureClientFactory procedureClientFactory;

  @Inject
  public DefaultBenchmarkManager(OperationExecutor opex,
                                 BenchmarkStreamWriterFactory streamWriterFactory,
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

    OperationContext ctx = new OperationContext(accountId, applicationId);
    DataFabric dataFabric = new DataFabricImpl(opex, ctx);
    TransactionProxy proxy = new TransactionProxy();
    proxy.setTransactionAgent(new SynchronousTransactionAgent(opex, ctx));

    try {
      this.dataSetInstantiator = new DataSetInstantiator(dataFabric, proxy,
                                                         new DataSetClassLoader(new JarClassLoader(deployedJar)));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    this.dataSetInstantiator.setDataSets(ImmutableList.copyOf(appSpec.getDataSets().values()));
  }

  @Override
  public FlowManager startFlow(final String flowName) {
    return startFlow(flowName, ImmutableMap.<String, String>of());
  }

  @Override
  public FlowManager startFlow(final String flowName, Map<String, String> arguments) {
    try {
      final FlowIdentifier flowId = new FlowIdentifier(accountId, applicationId, flowName, 0);
      Preconditions.checkState(runningProcessses.putIfAbsent(flowName, flowId) == null,
                               "Flow %s is already running", flowName);
      try {
        appFabricServer.start(token, new FlowDescriptor(flowId, arguments));
      } catch (Exception e) {
        runningProcessses.remove(flowName);
        throw Throwables.propagate(e);
      }

      return new FlowManager() {
        @Override
        public void setFlowletInstances(String flowletName, int instances) {
          Preconditions.checkArgument(instances > 0, "Instance counter should be > 0.");
          try {
            appFabricServer.setInstances(token, flowId, flowletName, (short) instances);
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
      final FlowIdentifier jobId = new FlowIdentifier(accountId, applicationId, jobName, 0);
      jobId.setType(EntityType.QUERY);

      // mapreduce job can stop by itself, so refreshing info about its state
      if (!isRunning(jobId)) {
        runningProcessses.remove(jobName);
      }

      Preconditions.checkState(runningProcessses.putIfAbsent(jobName, jobId) == null,
                               "MapReduce job %s is already running", jobName);
      try {
        appFabricServer.start(token, new FlowDescriptor(jobId, arguments));
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
      final FlowIdentifier procedureId = new FlowIdentifier(accountId, applicationId, procedureName, 0);
      procedureId.setType(EntityType.QUERY);
      Preconditions.checkState(runningProcessses.putIfAbsent(procedureName, procedureId) == null,
                               "Procedure %s is already running", procedureName);
      try {
        appFabricServer.start(token, new FlowDescriptor(procedureId, arguments));
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
    return streamWriterFactory.create(CConfiguration.create(), queueName);
  }

  @Override
  public <T extends DataSet> T getDataSet(String dataSetName) {
    return dataSetInstantiator.getDataSet(dataSetName);
  }

  @Override
  public void stopAll() {
    LOG.debug("Stopping all flowlets and procedures...");
    try {
      for (Map.Entry<String, FlowIdentifier> entry : Iterables.consumingIterable(runningProcessses.entrySet())) {
        appFabricServer.stop(token, entry.getValue());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      RuntimeStats.clearStats(applicationId);
    }
    LOG.debug("Stopped all flowlets and procedures.");
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

  private boolean isRunning(FlowIdentifier flowId) {
    try {
      FlowStatus status = appFabricServer.status(token, flowId);
      // comparing to hardcoded string is ugly, but this is how appFabricServer works now to support legacy UI
      return "RUNNING".equals(status.getStatus());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}


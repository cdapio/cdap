/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.test.internal;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.ApiResourceListHolder;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.lang.jar.ProgramClassLoader;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.dataset.DataSetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.gateway.handlers.AppFabricHttpHandler;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ProcedureClient;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ScheduleManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.WorkflowManager;
import com.continuuity.tephra.TransactionContext;
import com.continuuity.tephra.TransactionFailureException;
import com.continuuity.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class DefaultApplicationManager implements ApplicationManager {

  private final ConcurrentMap<String, ProgramId> runningProcessses = Maps.newConcurrentMap();
  private final String accountId;
  private final String applicationId;
  private final TransactionSystemClient txSystemClient;
  private final DataSetInstantiator dataSetInstantiator;
  private final StreamWriterFactory streamWriterFactory;
  private final ProcedureClientFactory procedureClientFactory;
  private final AppFabricClient appFabricClient;
  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public DefaultApplicationManager(LocationFactory locationFactory,
                                   DatasetFramework datasetFramework,
                                   TransactionSystemClient txSystemClient,
                                   StreamWriterFactory streamWriterFactory,
                                   ProcedureClientFactory procedureClientFactory,
                                   CConfiguration configuration,
                                   DiscoveryServiceClient discoveryServiceClient,
                                   AppFabricHttpHandler httpHandler,
                                   TemporaryFolder tempFolder,
                                   @Assisted("accountId") String accountId,
                                   @Assisted("applicationId") String applicationId,
                                   @Assisted Location deployedJar,
                                   @Assisted ApplicationSpecification appSpec) {
    this.accountId = accountId;
    this.applicationId = applicationId;
    this.streamWriterFactory = streamWriterFactory;
    this.procedureClientFactory = procedureClientFactory;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txSystemClient = txSystemClient;
    this.appFabricClient = new AppFabricClient(httpHandler, locationFactory);
    try {
      File tempDir = tempFolder.newFolder();
      BundleJarUtil.unpackProgramJar(deployedJar, tempDir);
      ProgramClassLoader classLoader = ClassLoaders.newProgramClassLoader
        (tempDir, ApiResourceListHolder.getResourceList(), this.getClass().getClassLoader());
      this.dataSetInstantiator = new DataSetInstantiator(datasetFramework, configuration,
                                                         new DataSetClassLoader(classLoader));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    this.dataSetInstantiator.setDataSets(appSpec.getDatasets().values());
  }

  @Override
  public FlowManager startFlow(final String flowName) {
    return startFlow(flowName, ImmutableMap.<String, String>of());
  }

  @Override
  public FlowManager startFlow(final String flowName, Map<String, String> arguments) {
    try {
      final ProgramId flowId = new ProgramId(applicationId, flowName, "flows");
      Preconditions.checkState(runningProcessses.putIfAbsent(flowName, flowId) == null,
                               "Flow %s is already running", flowName);
      try {
        appFabricClient.startProgram(applicationId, flowName, "flows", arguments);
      } catch (Exception e) {
        runningProcessses.remove(flowName);
        throw Throwables.propagate(e);
      }

      return new FlowManager() {
        @Override
        public void setFlowletInstances(String flowletName, int instances) {
          Preconditions.checkArgument(instances > 0, "Instance counter should be > 0.");
          try {
            appFabricClient.setFlowletInstances(applicationId, flowName, flowletName, instances);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public void stop() {
          try {
            if (runningProcessses.remove(flowName, flowId)) {
              appFabricClient.stopProgram(applicationId, flowName, "flows");
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

      final ProgramId jobId = new ProgramId(applicationId, jobName, "mapreduce");

      // mapreduce job can stop by itself, so refreshing info about its state
      if (!isRunning(jobId)) {
        runningProcessses.remove(jobName);
      }

      Preconditions.checkState(runningProcessses.putIfAbsent(jobName, jobId) == null,
                               "MapReduce job %s is already running", jobName);
      try {
        appFabricClient.startProgram(applicationId, jobName, "mapreduce", arguments);
      } catch (Exception e) {
        runningProcessses.remove(jobName);
        throw Throwables.propagate(e);
      }

      return new MapReduceManager() {
        @Override
        public void stop() {
          try {
            if (runningProcessses.remove(jobName, jobId)) {
              appFabricClient.stopProgram(applicationId, jobName, "mapreduce");
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
      final ProgramId procedureId = new ProgramId(applicationId, procedureName, "procedures");
      Preconditions.checkState(runningProcessses.putIfAbsent(procedureName, procedureId) == null,
                               "Procedure %s is already running", procedureName);
      try {
        appFabricClient.startProgram(applicationId, procedureName, "procedures", arguments);
      } catch (Exception e) {
        runningProcessses.remove(procedureName);
        throw Throwables.propagate(e);
      }

      return new ProcedureManager() {
        @Override
        public void stop() {
          try {
            if (runningProcessses.remove(procedureName, procedureId)) {
              appFabricClient.stopProgram(applicationId, procedureName, "procedures");
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
  public WorkflowManager startWorkflow(final String workflowName, Map<String, String> arguments) {
    try {
      final ProgramId workflowId = new ProgramId(applicationId, workflowName, "workflows");
      Preconditions.checkState(runningProcessses.putIfAbsent(workflowName, workflowId) == null,
                               "Workflow %s is already running", workflowName);

      // currently we are using it for schedule, so not starting the workflow

      return new WorkflowManager() {
        @Override
        public List<String> getSchedules() {
          return appFabricClient.getSchedules(applicationId, workflowName);
        }

        @Override
        public List<RunRecord> getHistory() {
          return appFabricClient.getHistory(applicationId, workflowName);
        }

        public ScheduleManager getSchedule(final String schedName) {

          return new ScheduleManager() {
            @Override
            public void suspend() {
              appFabricClient.suspend(applicationId, workflowName, schedName);
            }

            @Override
            public void resume() {
              appFabricClient.resume(applicationId, workflowName, schedName);
            }

            @Override
            public String status() {
              return appFabricClient.scheduleStatus(applicationId, workflowName, schedName);
            }
          };
        }

      };
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ServiceManager startService(String serviceName) {
    return startService(serviceName, ImmutableMap.<String, String>of());
  }

  @Override
  public ServiceManager startService(final String serviceName, Map<String, String> arguments) {
    try {
      final ProgramId serviceId = new ProgramId(applicationId, serviceName, "services");
      Preconditions.checkState(runningProcessses.putIfAbsent(serviceName, serviceId) == null,
                               "Service %s is already running", serviceName);
      try {
        appFabricClient.startProgram(applicationId, serviceName, "services", arguments);
      } catch (Exception e) {
        runningProcessses.remove(serviceName);
        throw Throwables.propagate(e);
      }

      return new ServiceManager() {
        @Override
        public void stop() {
          try {
            if (runningProcessses.remove(serviceName, serviceId)) {
              appFabricClient.stopProgram(applicationId, serviceName, "services");
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
        public boolean isRunning() {
          try {
            return DefaultApplicationManager.this.isRunning(serviceId);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public ServiceDiscovered discover(String applicationId, String serviceId, String serviceName) {
          String discoveryName = String.format("service.%s.%s.%s.%s", accountId, applicationId, serviceId, serviceName);
          return discoveryServiceClient.discover(discoveryName);
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
  public <T> DataSetManager<T> getDataSet(String dataSetName) {
    @SuppressWarnings("unchecked")
    final T dataSet = (T) dataSetInstantiator.getDataSet(dataSetName);

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
          ProgramId id = entry.getValue();
          appFabricClient.stopProgram(id.getApplicationId(), id.getRunnableId(), id.getRunnableType());
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

  private class ProgramId {
    private final String appId;
    private final String runnableId;
    private final String runnableType;

    private ProgramId(String applicationId, String runnableId, String runnableType) {
      this.appId = applicationId;
      this.runnableId = runnableId;
      this.runnableType = runnableType;
    }
    public String getApplicationId() {
      return this.appId;
    }
    public String getRunnableId() {
      return this.runnableId;
    }
    public String getRunnableType() {
      return this.runnableType;
    }
  }

  private boolean isRunning(ProgramId programId) {
    try {

      String status = appFabricClient.getStatus(programId.getApplicationId(),
                                                    programId.getRunnableId(), programId.getRunnableType());
      // comparing to hardcoded string is ugly, but this is how appFabricServer works now to support legacy UI
      return "RUNNING".equals(status);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}

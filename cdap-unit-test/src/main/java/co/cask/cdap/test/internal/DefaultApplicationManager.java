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

package co.cask.cdap.test.internal;

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.data.dataset.DatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.gateway.handlers.AppFabricHttpHandler;
import co.cask.cdap.gateway.handlers.ServiceHttpHandler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ProcedureClient;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.ScheduleManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.discovery.DiscoveryServiceClient;
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
 * A default implementation of {@link ApplicationManager}.
 */
public class DefaultApplicationManager implements ApplicationManager {

  private final ConcurrentMap<String, ProgramId> runningProcesses = Maps.newConcurrentMap();
  private final String accountId;
  private final String applicationId;
  private final TransactionSystemClient txSystemClient;
  private final DatasetInstantiator datasetInstantiator;
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
                                   DiscoveryServiceClient discoveryServiceClient,
                                   AppFabricHttpHandler httpHandler,
                                   ServiceHttpHandler serviceHttpHandler,
                                   TemporaryFolder tempFolder,
                                   @Assisted("accountId") String accountId,
                                   @Assisted("applicationId") String applicationId,
                                   @Assisted Location deployedJar) {
    this.accountId = accountId;
    this.applicationId = applicationId;
    this.streamWriterFactory = streamWriterFactory;
    this.procedureClientFactory = procedureClientFactory;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txSystemClient = txSystemClient;
    this.appFabricClient = new AppFabricClient(httpHandler, serviceHttpHandler, locationFactory);

    try {
      File tempDir = tempFolder.newFolder();
      BundleJarUtil.unpackProgramJar(deployedJar, tempDir);
      ClassLoader classLoader = ProgramClassLoader.create(tempDir, getClass().getClassLoader());
      this.datasetInstantiator = new DatasetInstantiator(Id.Namespace.from(accountId), datasetFramework,
                                                         new DataSetClassLoader(classLoader),
                                                         // todo: collect metrics for datasets outside programs too
                                                         null);
    } catch (IOException e) {
      throw Throwables.propagate(e);
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

  static class ProgramId {
    private final String appId;
    private final String runnableId;
    private final ProgramType runnableType;

    ProgramId(String applicationId, String runnableId, ProgramType runnableType) {
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

    public ProgramType getRunnableType() {
      return this.runnableType;
    }
  }

  @Override
  public FlowManager startFlow(final String flowName) {
    return startFlow(flowName, ImmutableMap.<String, String>of());
  }

  @Override
  public FlowManager startFlow(final String flowName, Map<String, String> arguments) {
    final ProgramId flowId = startProgram(flowName, arguments, ProgramType.FLOW);
    return new DefaultFlowManager(flowName, flowId, applicationId, appFabricClient, this);
  }

  @Override
  public MapReduceManager startMapReduce(final String jobName) {
    return startMapReduce(jobName, ImmutableMap.<String, String>of());
  }

  @Override
  public MapReduceManager startMapReduce(final String jobName, Map<String, String> arguments) {
    return getMapReduceManager(jobName, arguments, ProgramType.MAPREDUCE);
  }

  private MapReduceManager getMapReduceManager(final String jobName, Map<String, String> arguments,
                                               final ProgramType programType) {
    try {
      final ProgramId jobId = startProgram(jobName, arguments, programType);
      return new MapReduceManager() {
        @Override
        public void stop() {
          stopProgram(jobId);
        }

        @Override
        public void waitForFinish(long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException {
          programWaitForFinish(timeout, timeoutUnit, jobId);
        }
      };
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }


  @Override
  public SparkManager startSpark(String jobName) {
    return startSpark(jobName, ImmutableMap.<String, String>of());
  }

  @Override
  public SparkManager startSpark(String jobName, Map<String, String> arguments) {
    return getSparkManager(jobName, arguments, ProgramType.SPARK);
  }

  private SparkManager getSparkManager(final String jobName, Map<String, String> arguments,
                                       final ProgramType programType) {
    try {
      final ProgramId jobId = startProgram(jobName, arguments, programType);
      return new SparkManager() {
        @Override
        public void stop() {
          stopProgram(jobId);
        }

        @Override
        public void waitForFinish(long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException {
          programWaitForFinish(timeout, timeoutUnit, jobId);
        }
      };
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private ProgramId startProgram(String jobName, Map<String, String> arguments, ProgramType programType) {
    final ProgramId jobId = new ProgramId(applicationId, jobName, programType);
    // program can stop by itself, so refreshing info about its state
    if (!isRunning(jobId)) {
      runningProcesses.remove(jobName);
    }

    Preconditions.checkState(runningProcesses.putIfAbsent(jobName, jobId) == null,
                             programType + " program %s is already running", jobName);
    try {
      appFabricClient.startProgram(applicationId, jobName, programType, arguments);
    } catch (Exception e) {
      runningProcesses.remove(jobName);
      throw Throwables.propagate(e);
    }
    return jobId;
  }

  private void programWaitForFinish(long timeout, TimeUnit timeoutUnit,
                                    ProgramId jobId) throws InterruptedException, TimeoutException {
    // Min sleep time is 10ms, max sleep time is 1 seconds
    long sleepMillis = Math.max(10, Math.min(timeoutUnit.toMillis(timeout) / 10, TimeUnit.SECONDS.toMillis(1)));
    Stopwatch stopwatch = new Stopwatch().start();
    while (isRunning(jobId) && stopwatch.elapsedTime(timeoutUnit) < timeout) {
      TimeUnit.MILLISECONDS.sleep(sleepMillis);
    }

    if (isRunning(jobId)) {
      throw new TimeoutException("Time limit reached.");
    }
  }

  @Override
  public ProcedureManager startProcedure(final String procedureName) {
    return startProcedure(procedureName, ImmutableMap.<String, String>of());
  }

  @Override
  public ProcedureManager startProcedure(final String procedureName, Map<String, String> arguments) {
    final ProgramId procedureId = startProgram(procedureName, arguments, ProgramType.PROCEDURE);
    return new ProcedureManager() {
      @Override
      public void stop() {
        stopProgram(procedureId);
      }

      @Override
      public ProcedureClient getClient() {
        return procedureClientFactory.create(accountId, applicationId, procedureName);
      }
    };
  }


  @Override
  public WorkflowManager startWorkflow(final String workflowName, Map<String, String> arguments) {
    final ProgramId workflowId = new ProgramId(applicationId, workflowName, ProgramType.WORKFLOW);
    // currently we are using it for schedule, so not starting the workflow

    return new WorkflowManager() {
      @Override
      public List<ScheduleSpecification> getSchedules() {
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
            appFabricClient.suspend(applicationId, schedName);
          }

          @Override
          public void resume() {
            appFabricClient.resume(applicationId, schedName);
          }

          @Override
          public String status(int expectedCode) {
            return appFabricClient.scheduleStatus(applicationId, schedName, expectedCode);
          }
        };
      }

    };
  }

  @Override
  public ServiceManager startService(String serviceName) {
    return startService(serviceName, ImmutableMap.<String, String>of());
  }

  @Override
  public ServiceManager startService(final String serviceName, Map<String, String> arguments) {
    final ProgramId serviceId = startProgram(serviceName, arguments, ProgramType.SERVICE);
    return new DefaultServiceManager(accountId, serviceId, appFabricClient, discoveryServiceClient, this);
  }

  @Override
  public WorkerManager startWorker(String workerName) {
    return startWorker(workerName, ImmutableMap.<String, String>of());
  }

  @Override
  public WorkerManager startWorker(String workerName, Map<String, String> arguments) {
    final ProgramId workerId = startProgram(workerName, arguments, ProgramType.WORKER);
    return new DefaultWorkerManager(accountId, workerId, appFabricClient, discoveryServiceClient, this);
  }

  @Override
  public StreamWriter getStreamWriter(String streamName) {
    Id.Stream streamId = Id.Stream.from(accountId, streamName);
    return streamWriterFactory.create(streamId);
  }

  @Override
  public <T> DataSetManager<T> getDataSet(String dataSetName) {
    @SuppressWarnings("unchecked")
    final T dataSet = (T) datasetInstantiator.getDataset(dataSetName);

    try {
      final TransactionContext txContext =
        new TransactionContext(txSystemClient, datasetInstantiator.getTransactionAware());
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
      for (Map.Entry<String, ProgramId> entry : Iterables.consumingIterable(runningProcesses.entrySet())) {
        // have to do a check, since mapreduce jobs could stop by themselves earlier, and appFabricServer.stop will
        // throw error when you stop something that is not running.
        if (isRunning(entry.getValue())) {
          ProgramId id = entry.getValue();
          appFabricClient.stopProgram(id.getApplicationId(), id.getRunnableId(), id.getRunnableType());
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  void stopProgram(ProgramId programId) {
    String programName = programId.getRunnableId();
    try {
      if (runningProcesses.remove(programName, programId)) {
        appFabricClient.stopProgram(applicationId, programName, programId.getRunnableType());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  boolean isRunning(ProgramId programId) {
    try {

      String status = appFabricClient.getStatus(programId.getApplicationId(),
                                                programId.getRunnableId(), programId.getRunnableType());
      // comparing to hardcoded string is ugly, but this is how appFabricServer works now to support legacy UI
      return "STARTING".equals(status) || "RUNNING".equals(status);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}

/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test.remote;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.ScheduleClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.ScheduleManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class RemoteApplicationManager implements ApplicationManager {

  protected final Id.Application application;

  private final ClientConfig clientConfig;
  private final MetricsClient metricsClient;

  public RemoteApplicationManager(Id.Application application, ClientConfig clientConfig) {
    this.application = application;
    this.clientConfig = clientConfig;
    this.metricsClient = new MetricsClient(clientConfig);
  }

  private ClientConfig getClientConfig() {
    ConnectionConfig connectionConfig = ConnectionConfig.builder(clientConfig.getConnectionConfig())
      .setNamespace(application.getNamespace())
      .build();
    return new ClientConfig.Builder(clientConfig).setConnectionConfig(connectionConfig).build();
  }

  private ApplicationClient getApplicationClient() {
    return new ApplicationClient(getClientConfig());
  }

  private ProgramClient getProgramClient() {
    return new ProgramClient(getClientConfig());
  }

  private ScheduleClient getScheduleClient() {
    return new ScheduleClient(getClientConfig());
  }

  @Override
  public FlowManager startFlow(final String flowName) {
    return startFlow(flowName, ImmutableMap.<String, String>of());
  }

  @Override
  public FlowManager startFlow(final String flowName, Map<String, String> arguments) {
    final ProgramId flowId = startProgram(flowName, arguments, ProgramType.FLOW);
    return new FlowManager() {
      @Override
      public void setFlowletInstances(String flowletName, int instances) {
        Preconditions.checkArgument(instances > 0, "Instance counter should be > 0.");
        try {
          getProgramClient().setFlowletInstances(application.getId(), flowName, flowletName, instances);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public RuntimeMetrics getFlowletMetrics(String flowletId) {
        return metricsClient.getFlowletMetrics(
          Id.Program.from(Id.Application.from(clientConfig.getNamespace(), application.getId()),
                          ProgramType.FLOW, flowId.getRunnableId()), flowletId);
      }

      @Override
      public void stop() {
        stopProgram(flowId);
      }

      @Override
      public boolean isRunning() {
        try {
          String status = getProgramClient().getStatus(application.getId(), ProgramType.FLOW, flowName);
          return "RUNNING".equals(status);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
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
        public RuntimeMetrics getMetrics() {
          return metricsClient.getMapReduceMetrics(Id.Program.from(application, ProgramType.MAPREDUCE,
                                                                   jobId.getRunnableId()));
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

  private ProgramId startProgram(String programName, Map<String, String> arguments, ProgramType programType) {
    ProgramClient programClient = getProgramClient();
    try {
      String status = programClient.getStatus(application.getId(), programType, programName);
      Preconditions.checkState("STOPPED".equals(status), programType + " program %s is already running", programName);
      programClient.start(application.getId(), programType, programName, arguments);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return new ProgramId(programName, programType);
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
  @Deprecated
  public ProcedureManager startProcedure(final String procedureName) {
    try {
      getProgramClient().start(application.getId(), ProgramType.PROCEDURE, procedureName);
      return new RemoteProcedureManager(Id.Procedure.from(application, procedureName), clientConfig);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  @Deprecated
  public ProcedureManager startProcedure(final String procedureName, Map<String, String> arguments) {
    try {
      getProgramClient().start(application.getId(), ProgramType.PROCEDURE, procedureName, arguments);
      return new RemoteProcedureManager(Id.Procedure.from(application, procedureName), clientConfig);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public WorkflowManager startWorkflow(final String workflowName, Map<String, String> arguments) {
    // currently we are using it for schedule, so not starting the workflow
    return new WorkflowManager() {
      @Override
      public List<ScheduleSpecification> getSchedules() {
        try {
          return getScheduleClient().list(application.getId(), workflowName);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public List<RunRecord> getHistory() {
        try {
          return getProgramClient().getProgramRuns(application.getId(), ProgramType.WORKFLOW,
                                                   workflowName, "ALL", 0, Long.MAX_VALUE, Integer.MAX_VALUE);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      public ScheduleManager getSchedule(final String schedName) {
        return new ScheduleManager() {
          @Override
          public void suspend() {
            try {
              getScheduleClient().suspend(application.getId(), schedName);
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public void resume() {
            try {
              getScheduleClient().resume(application.getId(), schedName);
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public String status(int expectedCode) {
            try {
              return getScheduleClient().getStatus(application.getId(), schedName);
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
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
    startProgram(serviceName, arguments, ProgramType.SERVICE);
    return new RemoteServiceManager(Id.Service.from(application, serviceName), clientConfig);
  }

  @Override
  public WorkerManager startWorker(final String workerName, Map<String, String> arguments) {
    final ProgramId workerId = new ProgramId(workerName, ProgramType.WORKER);
    return new WorkerManager() {
      @Override
      public void setRunnableInstances(int instances) {
        Preconditions.checkArgument(instances > 0, "Instance count should be > 0.");
        try {
          getProgramClient().setWorkerInstances(application.getId(), workerName, instances);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void stop() {
        stopProgram(workerId);
      }

      @Override
      public boolean isRunning() {
        try {
          String status = getProgramClient().getStatus(application.getId(), ProgramType.WORKER, workerName);
          return "RUNNING".equals(status);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  @Override
  public WorkerManager startWorker(String workerName) {
    return startWorker(workerName, ImmutableMap.<String, String>of());
  }

  @Override
  public StreamWriter getStreamWriter(String streamName) {
    return new RemoteStreamWriter(clientConfig, streamName);
  }

  @Override
  public <T> DataSetManager<T> getDataSet(String dataSetName) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void stopAll() {
    try {
      for (ProgramRecord programRecord : getApplicationClient().listPrograms(application.getId())) {
        // have to do a check, since mapreduce jobs could stop by themselves earlier, and appFabricServer.stop will
        // throw error when you stop something that is not running.
        ProgramId id = new ProgramId(programRecord.getId(), programRecord.getType());
        if (isRunning(id)) {
          getProgramClient().stop(application.getId(), id.getRunnableType(), id.getRunnableId());
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  void stopProgram(ProgramId programId) {
    String programName = programId.getRunnableId();
    try {
      getProgramClient().stop(application.getId(), programId.getRunnableType(), programName);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  boolean isRunning(ProgramId programId) {
    try {
      String status = getProgramClient().getStatus(application.getId(), programId.getRunnableType(),
                                                   programId.getRunnableId());
      // comparing to hardcoded string is ugly, but this is how appFabricServer works now to support legacy UI
      return "STARTING".equals(status) || "RUNNING".equals(status);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  static class ProgramId {
    private final String runnableId;
    private final ProgramType runnableType;

    ProgramId(String runnableId, ProgramType runnableType) {
      this.runnableId = runnableId;
      this.runnableType = runnableType;
    }
    public String getRunnableId() {
      return this.runnableId;
    }
    public ProgramType getRunnableType() {
      return this.runnableType;
    }
  }
}

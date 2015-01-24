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

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ScheduleManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
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

  private final ClientConfig clientConfig;
  private final ApplicationClient applicationClient;
  private final ProgramClient programClient;
  private final String applicationId;

  public RemoteApplicationManager(String applicationId, ClientConfig clientConfig) {
    this.applicationId = applicationId;
    this.clientConfig = clientConfig;
    this.applicationClient = new ApplicationClient(clientConfig);
    this.programClient = new ProgramClient(clientConfig);
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
          programClient.setFlowletInstances(applicationId, flowName, flowletName, instances);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void stop() {
        stopProgram(flowId);
      }

      @Override
      public boolean isRunning() {
        try {
          String status = programClient.getStatus(applicationId, ProgramType.FLOW, flowName);
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
    final ProgramId programId = new ProgramId(applicationId, programName, programType);
    try {
      Preconditions.checkState(programClient.getStatus(applicationId, programType, programName).equals("STOPPED"),
                               programType + " program %s is already running", programName);
      programClient.start(applicationId, programType, programName, arguments);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return programId;
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
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public ProcedureManager startProcedure(final String procedureName, Map<String, String> arguments) {
    throw new UnsupportedOperationException();
  }

  @Override
  public WorkflowManager startWorkflow(final String workflowName, Map<String, String> arguments) {
    final ProgramId workflowId = new ProgramId(applicationId, workflowName, ProgramType.WORKFLOW);
    // currently we are using it for schedule, so not starting the workflow

    return new WorkflowManager() {
      @Override
      public List<ScheduleSpecification> getSchedules() {
        throw new UnsupportedOperationException("TODO");
      }

      @Override
      public List<RunRecord> getHistory() {
        try {
          return programClient.getProgramRuns(applicationId, ProgramType.WORKFLOW, workflowName, "ALL",
                                       0, Long.MAX_VALUE, Integer.MAX_VALUE);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      public ScheduleManager getSchedule(final String schedName) {

        return new ScheduleManager() {
          @Override
          public void suspend() {
            throw new UnsupportedOperationException("TODO");
          }

          @Override
          public void resume() {
            throw new UnsupportedOperationException("TODO");
          }

          @Override
          public String status() {
            throw new UnsupportedOperationException("TODO");
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
    return new RemoteServiceManager(serviceId, clientConfig);
  }

  @Override
  public StreamWriter getStreamWriter(String streamName) {
    return new RemoteStreamWriter(clientConfig, streamName);
  }

  @Override
  public <T> DataSetManager<T> getDataSet(String dataSetName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void stopAll() {
    try {
      for (List<ProgramRecord> programRecords : applicationClient.listPrograms(applicationId).values()) {
        for (ProgramRecord programRecord : programRecords) {
          // have to do a check, since mapreduce jobs could stop by themselves earlier, and appFabricServer.stop will
          // throw error when you stop something that is not running.
          ProgramId id = new ProgramId(programRecord.getApp(), programRecord.getId(), programRecord.getType());
          if (isRunning(id)) {
            programClient.stop(id.getApplicationId(), id.getRunnableType(), id.getRunnableId());
          }
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      RuntimeStats.clearStats(applicationId);
    }
  }

  void stopProgram(ProgramId programId) {
    String programName = programId.getRunnableId();
    try {
      programClient.stop(applicationId, programId.getRunnableType(), programName);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  boolean isRunning(ProgramId programId) {
    try {

      String status = programClient.getStatus(programId.getApplicationId(),
                                              programId.getRunnableType(), programId.getRunnableId());
      // comparing to hardcoded string is ugly, but this is how appFabricServer works now to support legacy UI
      return "STARTING".equals(status) || "RUNNING".equals(status);
    } catch (Exception e) {
      throw Throwables.propagate(e);
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
}

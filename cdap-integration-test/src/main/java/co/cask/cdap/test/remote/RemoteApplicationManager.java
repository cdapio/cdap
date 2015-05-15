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

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.DefaultMapReduceManager;
import co.cask.cdap.test.DefaultSparkManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
public class RemoteApplicationManager implements ApplicationManager {
  protected final Id.Application application;

  private final ClientConfig clientConfig;
  private final ProgramClient programClient;
  private final ApplicationClient applicationClient;

  public RemoteApplicationManager(Id.Application application, ClientConfig clientConfig) {
    this.application = application;

    ClientConfig namespacedClientConfig = new ClientConfig.Builder(clientConfig).build();
    namespacedClientConfig.setNamespace(application.getNamespace());
    this.clientConfig = namespacedClientConfig;
    this.programClient = new ProgramClient(clientConfig);
    this.applicationClient = new ApplicationClient(clientConfig);
  }

  @Override
  public FlowManager startFlow(final String flowName) {
    return startFlow(flowName, ImmutableMap.<String, String>of());
  }

  @Override
  public FlowManager startFlow(final String flowName, Map<String, String> arguments) {
    final Id.Program flowId = startProgram(flowName, arguments, ProgramType.FLOW);
    return new RemoteFlowManager(flowId, clientConfig, this);
  }

  @Override
  public MapReduceManager startMapReduce(final String programName) {
    return startMapReduce(programName, ImmutableMap.<String, String>of());
  }

  @Override
  public MapReduceManager startMapReduce(final String programName, Map<String, String> arguments) {
    return new DefaultMapReduceManager(startProgram(programName, arguments, ProgramType.MAPREDUCE), this);
  }

  @Override
  public SparkManager startSpark(String programName) {
    return startSpark(programName, ImmutableMap.<String, String>of());
  }

  @Override
  public SparkManager startSpark(String programName, Map<String, String> arguments) {
    final Id.Program programId = startProgram(programName, arguments, ProgramType.SPARK);
    return new DefaultSparkManager(programId, this);
  }

  private Id.Program startProgram(String programName, Map<String, String> arguments, ProgramType programType) {
    try {
      String status = programClient.getStatus(application.getId(), programType, programName);
      Preconditions.checkState("STOPPED".equals(status), programType + " program %s is already running", programName);
      programClient.start(application.getId(), programType, programName, arguments);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return Id.Program.from(application, programType, programName);
  }

  @Override
  public WorkflowManager startWorkflow(final String workflowName, Map<String, String> arguments) {
    // currently we are using it for schedule, so not starting the workflow
    Id.Program workflowId = Id.Program.from(application, ProgramType.WORKFLOW, workflowName);
    return new RemoteWorkflowManager(workflowId, clientConfig, this);
  }

  @Override
  public ServiceManager startService(String serviceName) {
    return startService(serviceName, ImmutableMap.<String, String>of());
  }

  @Override
  public ServiceManager startService(final String serviceName, Map<String, String> arguments) {
    Id.Program programId = startProgram(serviceName, arguments, ProgramType.SERVICE);
    return new RemoteServiceManager(programId, clientConfig, this);
  }

  @Override
  public WorkerManager startWorker(final String workerName, Map<String, String> arguments) {
    final Id.Program workerId = Id.Program.from(application, ProgramType.WORKER, workerName);
    return new RemoteWorkerManager(workerId, clientConfig, this);
  }

  @Override
  public WorkerManager startWorker(String workerName) {
    return startWorker(workerName, ImmutableMap.<String, String>of());
  }

  @Override
  @Deprecated
  public StreamWriter getStreamWriter(String streamName) {
    return new RemoteStreamWriter(new RemoteStreamManager(clientConfig,
                                                          Id.Stream.from(application.getNamespaceId(), streamName)));
  }

  @Override
  public <T> DataSetManager<T> getDataSet(String dataSetName) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void stopAll() {
    try {
      for (ProgramRecord programRecord : applicationClient.listPrograms(application.getId())) {
        // have to do a check, since appFabricServer.stop will throw error when you stop something that is not running.
        Id.Program id = Id.Program.from(application, programRecord.getType(), programRecord.getName());
        if (isRunning(id)) {
          programClient.stop(application.getId(), id.getType(), id.getId());
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stopProgram(Id.Program programId) {
    try {
      programClient.stop(application.getId(), programId.getType(), programId.getId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isRunning(Id.Program programId) {
    try {
      String status = programClient.getStatus(application.getId(), programId.getType(),
                                              programId.getId());
      // comparing to hardcoded string is ugly, but this is how appFabricServer works now to support legacy UI
      return "STARTING".equals(status) || "RUNNING".equals(status);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}

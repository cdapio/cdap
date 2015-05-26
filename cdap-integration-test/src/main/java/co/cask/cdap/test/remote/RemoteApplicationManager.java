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
import co.cask.cdap.test.AbstractApplicationManager;
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

import java.util.Map;

/**
 *
 */
public class RemoteApplicationManager extends AbstractApplicationManager {
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
  public FlowManager getFlowManager(String flowName) {
    Id.Program flowId = Id.Program.from(application, ProgramType.FLOW, flowName);
    return new RemoteFlowManager(flowId, clientConfig, this);
  }

  @Override
  public MapReduceManager getMapReduceManager(String programName) {
    Id.Program programId = Id.Program.from(application, ProgramType.MAPREDUCE, programName);
    return new DefaultMapReduceManager(programId, this);
  }

  @Override
  public SparkManager getSparkManager(String jobName) {
    Id.Program programId = Id.Program.from(application, ProgramType.SPARK, jobName);
    return new DefaultSparkManager(programId, this);
  }

  @Override
  public WorkflowManager getWorkflowManager(String workflowName) {
    Id.Program programId = Id.Program.from(application, ProgramType.WORKFLOW, workflowName);
    return new RemoteWorkflowManager(programId, clientConfig, this);
  }

  @Override
  public ServiceManager getServiceManager(String serviceName) {
    Id.Program programId = Id.Program.from(application, ProgramType.SERVICE, serviceName);
    return new RemoteServiceManager(programId, clientConfig, this);
  }

  @Override
  public WorkerManager getWorkerManager(String workerName) {
    Id.Program programId = Id.Program.from(application, ProgramType.WORKER, workerName);
    return new RemoteWorkerManager(programId, clientConfig, this);
  }

  @Override
  protected Id.Program startProgram(String programName, Map<String, String> arguments, ProgramType programType) {
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

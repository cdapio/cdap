/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.ApplicationDetail;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.PluginInstanceDetail;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.test.AbstractApplicationManager;
import co.cask.cdap.test.DefaultMapReduceManager;
import co.cask.cdap.test.DefaultSparkManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.util.List;
import java.util.Map;

/**
 * {@link AbstractApplicationManager} for use in integration tests.
 */
public class RemoteApplicationManager extends AbstractApplicationManager {
  private final ClientConfig clientConfig;
  private final ProgramClient programClient;
  private final ApplicationClient applicationClient;
  private final RESTClient restClient;

  @Deprecated
  public RemoteApplicationManager(Id.Application application, ClientConfig clientConfig, RESTClient restClient) {
    this(application.toEntityId(), clientConfig, restClient);
  }

  public RemoteApplicationManager(ApplicationId application, ClientConfig clientConfig, RESTClient restClient) {
    super(application);
    this.clientConfig = clientConfig;
    this.programClient = new ProgramClient(clientConfig, restClient);
    this.applicationClient = new ApplicationClient(clientConfig, restClient);
    this.restClient = restClient;
  }

  @Override
  public FlowManager getFlowManager(String flowName) {
    Id.Flow flowId = Id.Flow.from(application.toId(), flowName);
    return new RemoteFlowManager(flowId, clientConfig, restClient, this);
  }

  @Override
  public MapReduceManager getMapReduceManager(String programName) {
    Id.Program programId = Id.Program.from(application.toId(), ProgramType.MAPREDUCE, programName);
    return new DefaultMapReduceManager(programId, this);
  }

  @Override
  public SparkManager getSparkManager(String jobName) {
    Id.Program programId = Id.Program.from(application.toId(), ProgramType.SPARK, jobName);
    return new DefaultSparkManager(programId, this);
  }

  @Override
  public WorkflowManager getWorkflowManager(String workflowName) {
    Id.Workflow programId = Id.Workflow.from(application.toId(), workflowName);
    return new RemoteWorkflowManager(programId, clientConfig, restClient, this);
  }

  @Override
  public ServiceManager getServiceManager(String serviceName) {
    ServiceId serviceId = new ServiceId(application, serviceName);
    return new RemoteServiceManager(serviceId, clientConfig, restClient, this);
  }

  @Override
  public WorkerManager getWorkerManager(String workerName) {
    Id.Worker programId = Id.Worker.from(application.toId(), workerName);
    return new RemoteWorkerManager(programId, clientConfig, restClient, this);
  }

  @Override
  public List<PluginInstanceDetail> getPlugins() {
    try {
      return applicationClient.getPlugins(application);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stopAll() {
    try {
      for (ProgramRecord programRecord : applicationClient.listPrograms(application)) {
        // have to do a check, since appFabricServer.stop will throw error when you stop something that is not running.
        ProgramId id = application.program(programRecord.getType(), programRecord.getName());
        if (isRunning(id)) {
          programClient.stop(id);
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stopProgram(Id.Program programId) {
    try {
      programClient.stop(programId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stopProgram(ProgramId programId) {
    try {
      programClient.stop(programId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void startProgram(Id.Program programId, Map<String, String> arguments) {
    try {
      String status = programClient.getStatus(programId);
      Preconditions.checkState("STOPPED".equals(status), "Program %s is already running", programId);
      programClient.start(programId, false, arguments);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void startProgram(ProgramId programId, Map<String, String> arguments) {
    try {
      String status = programClient.getStatus(programId);
      Preconditions.checkState("STOPPED".equals(status), "Program %s is already running", programId);
      programClient.start(programId, false, arguments);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isRunning(Id.Program programId) {
    try {
      String status = programClient.getStatus(programId);
      // comparing to hardcoded string is ugly, but this is how appFabricServer works now to support legacy UI
      return "STARTING".equals(status) || "RUNNING".equals(status);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isRunning(ProgramId programId) {
    try {
      String status = programClient.getStatus(programId);
      return "STARTING".equals(status) || "RUNNING".equals(status);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<RunRecord> getHistory(Id.Program programId, ProgramRunStatus status) {
    try {
      return programClient.getProgramRuns(programId, status.name(), 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void update(AppRequest appRequest) throws Exception {
    applicationClient.update(application.toId(), appRequest);
  }

  @Override
  public void delete() throws Exception {
    applicationClient.delete(application);
  }

  @Override
  public ApplicationDetail getInfo() throws Exception {
    return applicationClient.get(application);
  }

  @Override
  public void setRuntimeArgs(ProgramId programId, Map<String, String> args) throws Exception {
    programClient.setRuntimeArgs(programId.toId(), args);
  }
}

/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.test.remote;

import com.google.common.base.Throwables;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.PluginInstanceDetail;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.test.AbstractApplicationManager;
import io.cdap.cdap.test.DefaultMapReduceManager;
import io.cdap.cdap.test.MapReduceManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.WorkerManager;
import io.cdap.cdap.test.WorkflowManager;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * {@link AbstractApplicationManager} for use in integration tests.
 */
public class RemoteApplicationManager extends AbstractApplicationManager {
  private final ClientConfig clientConfig;
  private final ProgramClient programClient;
  private final ApplicationClient applicationClient;
  private final RESTClient restClient;

  public RemoteApplicationManager(ApplicationId application, ClientConfig clientConfig, RESTClient restClient) {
    super(application);
    this.clientConfig = clientConfig;
    this.programClient = new ProgramClient(clientConfig, restClient);
    this.applicationClient = new ApplicationClient(clientConfig, restClient);
    this.restClient = restClient;
  }

  @Override
  public MapReduceManager getMapReduceManager(String programName) {
    ProgramId programId = application.mr(programName);
    return new DefaultMapReduceManager(Id.Program.fromEntityId(programId), this);
  }

  @Override
  public SparkManager getSparkManager(String jobName) {
    return new RemoteSparkManager(application.spark(jobName), this, clientConfig, restClient);
  }

  @Override
  public WorkflowManager getWorkflowManager(String workflowName) {
    WorkflowId programId = application.workflow(workflowName);
    return new RemoteWorkflowManager(programId, clientConfig, restClient, this);
  }

  @Override
  public ServiceManager getServiceManager(String serviceName) {
    ServiceId serviceId = new ServiceId(application, serviceName);
    return new RemoteServiceManager(serviceId, clientConfig, restClient, this);
  }

  @Override
  public WorkerManager getWorkerManager(String workerName) {
    return new RemoteWorkerManager(application.worker(workerName), clientConfig, restClient, this);
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
        ProgramId programId = application.program(programRecord.getType(), programRecord.getName());
        if (!isStopped(programId)) {
          programClient.stop(programId);
        }
        waitForStopped(programId);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stopProgram(ProgramId programId) {
    try {
      programClient.stop(programId);
      waitForStopped(programId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void startProgram(ProgramId programId, Map<String, String> arguments) {
    try {
      programClient.start(programId, false, arguments);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isRunning(ProgramId programId) {
    return isInState(programId, ProgramStatus.RUNNING);
  }

  @Override
  public boolean isStopped(ProgramId programId) {
    return isInState(programId, ProgramStatus.STOPPED);
  }

  private boolean isInState(ProgramId programId, ProgramStatus status) {
    try {
      String actual = programClient.getStatus(programId);
      return status.name().equals(actual);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<RunRecord> getHistory(ProgramId programId, ProgramRunStatus status) {
    try {
      return programClient.getProgramRuns(programId, status.name(), 0, Long.MAX_VALUE, Integer.MAX_VALUE);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void addSchedule(ScheduleDetail scheduleDetail) {
    try {
      applicationClient.addSchedule(application, scheduleDetail);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void enableSchedule(ScheduleId scheduleId) throws Exception {
    try {
      applicationClient.enableSchedule(scheduleId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void update(AppRequest appRequest) throws Exception {
    applicationClient.update(application, appRequest);
  }

  @Override
  public void delete() throws Exception {
    applicationClient.delete(application);
  }

  @Override
  public void upgrade() throws Exception {
    applicationClient.upgradeApplication(application);
  }

  @Override
  public void upgrade(Set<String> artifactScopes, boolean allowSnapshot) throws Exception {
    applicationClient.upgradeApplication(application, artifactScopes, allowSnapshot);
  }

  @Override
  public ApplicationDetail getInfo() throws Exception {
    return applicationClient.get(application);
  }

  @Override
  public void setRuntimeArgs(ProgramId programId, Map<String, String> args) throws Exception {
    programClient.setRuntimeArgs(programId, args);
  }

  @Override
  public Map<String, String> getRuntimeArgs(ProgramId programId) throws Exception {
    return programClient.getRuntimeArgs(programId);
  }
}

/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.test.internal;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.AppFabricClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.PluginInstanceDetail;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.test.AbstractApplicationManager;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DefaultMapReduceManager;
import io.cdap.cdap.test.DefaultSparkManager;
import io.cdap.cdap.test.MapReduceManager;
import io.cdap.cdap.test.MetricsManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.WorkerManager;
import io.cdap.cdap.test.WorkflowManager;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A default implementation of {@link ApplicationManager}.
 */
public class DefaultApplicationManager extends AbstractApplicationManager {

  private final AppFabricClient appFabricClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final MetricsManager metricsManager;

  @Inject
  public DefaultApplicationManager(DiscoveryServiceClient discoveryServiceClient,
                                   AppFabricClient appFabricClient,
                                   MetricsManager metricsManager,
                                   @Assisted("applicationId")ApplicationId applicationId) {
    super(applicationId);
    this.discoveryServiceClient = discoveryServiceClient;
    this.appFabricClient = appFabricClient;
    this.metricsManager = metricsManager;
  }

  @Override
  public MapReduceManager getMapReduceManager(String programName) {
    Id.Program programId = Id.Program.from(Id.Application.fromEntityId(application),
                                           ProgramType.MAPREDUCE, programName);
    return new DefaultMapReduceManager(programId, this);
  }

  @Override
  public SparkManager getSparkManager(String jobName) {
    return new DefaultSparkManager(application.spark(jobName), this, discoveryServiceClient);
  }

  @Override
  public WorkflowManager getWorkflowManager(String workflowName) {
    Id.Program programId = Id.Program.from(Id.Application.fromEntityId(application),
                                           ProgramType.WORKFLOW, workflowName);
    return new DefaultWorkflowManager(programId, appFabricClient, this);
  }

  @Override
  public ServiceManager getServiceManager(String serviceName) {
    ProgramId programId = application.service(serviceName);
    return new DefaultServiceManager(programId, appFabricClient, discoveryServiceClient, this, metricsManager);
  }

  @Override
  public WorkerManager getWorkerManager(String workerName) {
    Id.Program programId = Id.Program.from(Id.Application.fromEntityId(application), ProgramType.WORKER, workerName);
    return new DefaultWorkerManager(programId, appFabricClient, this);
  }

  @Override
  public List<PluginInstanceDetail> getPlugins() {
    try {
      return appFabricClient.getPlugins(application);
    } catch (Exception e) {
     throw Throwables.propagate(e);
    }
  }

  @Override
  public void stopAll() {
    try {
      ApplicationDetail appDetail = appFabricClient.getVersionedInfo(application);
      for (ProgramRecord programRecord : appDetail.getPrograms()) {
        try {
          appFabricClient.stopProgram(application.getNamespace(), application.getApplication(),
                                      appDetail.getAppVersion(), programRecord.getName(), programRecord.getType(),
                                      null);
        } catch (BadRequestException e) {
          // Ignore this as this will be throw if the program is not running, which is fine as there could
          // be programs in the application that are currently not running.
        }
        waitForStopped(application.program(programRecord.getType(), programRecord.getName()));
      }
    } catch (NamespaceNotFoundException e) {
      // This can be safely ignore if the unit-test already deleted the namespace
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stopProgram(ProgramId programId, String gracefulShutdownSecs) {
    String programName = programId.getProgram();
    try {
      appFabricClient.stopProgram(application.getNamespace(), application.getApplication(), application.getVersion(),
                                  programName, programId.getType(), null);
      waitForStopped(programId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void startProgram(ProgramId programId, Map<String, String> arguments) {
    try {
      appFabricClient.startProgram(application.getNamespace(), application.getApplication(),
                                   application.getVersion(), programId.getProgram(), programId.getType(), arguments);
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
      String actual = appFabricClient.getStatus(application.getNamespace(), programId.getApplication(),
                                                programId.getVersion(), programId.getProgram(), programId.getType());
      return status.name().equals(actual);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<RunRecord> getHistory(ProgramId programId, ProgramRunStatus status) {
    try {
      return appFabricClient.getHistory(programId, status);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void addSchedule(ScheduleDetail scheduleDetail) throws Exception {
    appFabricClient.addSchedule(application, scheduleDetail);
  }

  @Override
  public void enableSchedule(ScheduleId scheduleId) throws Exception {
    appFabricClient.enableSchedule(scheduleId);
  }

  @Override
  public void update(AppRequest appRequest) throws Exception {
    appFabricClient.updateApplication(application, appRequest);
  }

  @Override
  public void delete() throws Exception {
    appFabricClient.deleteApplication(application);
  }

  @Override
  public void upgrade() throws Exception {
    appFabricClient.upgradeApplication(application);
  }

  @Override
  public void upgrade(Set<String> artifactScopes, boolean allowSnapshot) throws Exception {
    appFabricClient.upgradeApplication(application, artifactScopes, allowSnapshot);
  }

  @Override
  public ApplicationDetail getInfo() throws Exception {
    return appFabricClient.getInfo(application);
  }

  @Override
  public void setRuntimeArgs(ProgramId programId, Map<String, String> args) throws Exception {
    appFabricClient.setRuntimeArgs(programId, args);
  }

  @Override
  public Map<String, String> getRuntimeArgs(ProgramId programId) throws Exception {
    return appFabricClient.getRuntimeArgs(programId);
  }
}

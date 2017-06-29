/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.AppFabricClient;
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
import co.cask.cdap.test.AbstractApplicationManager;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DefaultMapReduceManager;
import co.cask.cdap.test.DefaultSparkManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.MetricsManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
  public FlowManager getFlowManager(String flowName) {
    Id.Program programId = Id.Program.from(application.toId(), ProgramType.FLOW, flowName);
    return new DefaultFlowManager(programId, appFabricClient, this, metricsManager);
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
    Id.Program programId = Id.Program.from(application.toId(), ProgramType.WORKFLOW, workflowName);
    return new DefaultWorkflowManager(programId, appFabricClient, this);
  }

  @Override
  public ServiceManager getServiceManager(String serviceName) {
    ProgramId programId = application.service(serviceName);
    return new DefaultServiceManager(programId, appFabricClient, discoveryServiceClient, this, metricsManager);
  }

  @Override
  public WorkerManager getWorkerManager(String workerName) {
    Id.Program programId = Id.Program.from(application.toId(), ProgramType.WORKER, workerName);
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
                                      appDetail.getAppVersion(), programRecord.getName(), programRecord.getType());
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
  public void stopProgram(ProgramId programId) {
    String programName = programId.getProgram();
    try {
      appFabricClient.stopProgram(application.getNamespace(), application.getApplication(), application.getVersion(),
                                  programName, programId.getType());
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
    try {
      String status = appFabricClient.getStatus(application.getNamespace(), programId.getApplication(),
                                                programId.getVersion(), programId.getProgram(), programId.getType());
      return "STARTING".equals(status) || "RUNNING".equals(status);
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
  public void update(AppRequest appRequest) throws Exception {
    appFabricClient.updateApplication(application, appRequest);
  }

  @Override
  public void delete() throws Exception {
    appFabricClient.deleteApplication(application);
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

  @Override
  public void waitForStopped(final ProgramId programId) throws Exception {
    // TODO CDAP-12182 This is a workaround to ensure that there are no pending run records before moving on to the next
    // test. This should be removed once stopping a program on CDAP waits for the run record to be persisted.
    Tasks.waitFor(0, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return appFabricClient.getHistory(programId, ProgramRunStatus.RUNNING).size();
      }
    }, 10, TimeUnit.SECONDS);
  }
}

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

import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A default implementation of {@link ApplicationManager}.
 */
public class DefaultApplicationManager extends AbstractApplicationManager {
  private final Set<Id.Program> runningProcesses = Sets.newSetFromMap(Maps.<Id.Program, Boolean>newConcurrentMap());
  private final AppFabricClient appFabricClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final MetricsManager metricsManager;

  @Inject
  public DefaultApplicationManager(DiscoveryServiceClient discoveryServiceClient,
                                   AppFabricClient appFabricClient,
                                   MetricsManager metricsManager,
                                   @Assisted("applicationId") Id.Application application) {
    super(application);
    this.discoveryServiceClient = discoveryServiceClient;
    this.appFabricClient = appFabricClient;
    this.metricsManager = metricsManager;
  }

  @Override
  public FlowManager getFlowManager(String flowName) {
    Id.Program programId = Id.Program.from(application, ProgramType.FLOW, flowName);
    return new DefaultFlowManager(programId, appFabricClient, this, metricsManager);
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
    return new DefaultWorkflowManager(programId, appFabricClient, this);
  }

  @Override
  public ServiceManager getServiceManager(String serviceName) {
    Id.Program programId = Id.Program.from(application, ProgramType.SERVICE, serviceName);
    return new DefaultServiceManager(programId, appFabricClient, discoveryServiceClient, this, metricsManager);
  }

  @Override
  public WorkerManager getWorkerManager(String workerName) {
    Id.Program programId = Id.Program.from(application, ProgramType.WORKER, workerName);
    return new DefaultWorkerManager(programId, appFabricClient, this);
  }

  @Override
  public void stopAll() {
    try {
      for (Id.Program programId : Iterables.consumingIterable(runningProcesses)) {
        // have to do a check, since mapreduce jobs could stop by themselves earlier, and appFabricServer.stop will
        // throw error when you stop something that is not running.
        if (isRunning(programId)) {
          appFabricClient.stopProgram(application.getNamespaceId(), programId.getApplicationId(),
                                      programId.getId(), programId.getType());
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stopProgram(Id.Program programId) {
    String programName = programId.getId();
    try {
      if (runningProcesses.remove(programId)) {
        appFabricClient.stopProgram(application.getNamespaceId(), application.getId(),
                                    programName, programId.getType());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void startProgram(Id.Program programId, Map<String, String> arguments) {
  // program can stop by itself, so refreshing info about its state
    if (!isRunning(programId)) {
      runningProcesses.remove(programId);
    }

    Preconditions.checkState(runningProcesses.add(programId), "Program %s is already running", programId);
    try {
      appFabricClient.startProgram(application.getNamespaceId(), application.getId(),
                                   programId.getId(), programId.getType(), arguments);
    } catch (Exception e) {
      runningProcesses.remove(programId);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isRunning(Id.Program programId) {
    try {
      String status = appFabricClient.getStatus(application.getNamespaceId(), programId.getApplicationId(),
                                                programId.getId(), programId.getType());
      // comparing to hardcoded string is ugly, but this is how appFabricServer works now to support legacy UI
      return "STARTING".equals(status) || "RUNNING".equals(status);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<RunRecord> getHistory(Id.Program programId, ProgramRunStatus status) {
    try {
      return appFabricClient.getHistory(programId, status);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}

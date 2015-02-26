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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.exception.ApplicationNotFoundException;
import co.cask.cdap.common.exception.NamespaceNotFoundException;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * Service that manages lifecycle of Applications.
 */
public class AppLifecycleService {

  private static final Logger LOG = LoggerFactory.getLogger(AppLifecycleService.class);

  private final ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory;
  private final Scheduler scheduler;
  private final Store store;
  private final ProgramRuntimeService programService;

  @Inject
  public AppLifecycleService(Scheduler scheduler, StoreFactory storeFactory,
                             ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory,
                             ProgramRuntimeService programService) {
    this.scheduler = scheduler;
    this.store = storeFactory.create();
    this.managerFactory = managerFactory;
    this.programService = programService;
  }

  public ApplicationSpecification getApp(Id.Application appId) throws ApplicationNotFoundException {
    return store.getApplication(appId);
  }

  public Collection<ApplicationSpecification> listApps(Id.Namespace namespaceId) throws NamespaceNotFoundException {
    return store.getAllApplications(namespaceId);
  }

  public void deploy(Id.Namespace namespace, @Nullable String appId, DeploymentInfo deploymentInfo) throws Exception {
    Manager<DeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(new ProgramTerminator() {
      @Override
      public void stop(Id.Namespace id, Id.Program programId, ProgramType type) throws ExecutionException {
        deleteHandler(programId, type);
      }
    });
    manager.deploy(namespace, appId, deploymentInfo).get();
  }

  public AbstractAppFabricHttpHandler.AppFabricServiceStatus deleteApps(Id.Namespace namespace) throws Exception {
    Collection<ApplicationSpecification> allSpecs = store.getAllApplications(namespace);

    //Check if any App associated with this namespace is running
    final Id.Namespace accId = Id.Namespace.from(namespace.getId());
    boolean appRunning = checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program programId) {
        return programId.getApplication().getNamespace().equals(accId);
      }
    }, ProgramType.values());

    if (appRunning) {
      return AbstractAppFabricHttpHandler.AppFabricServiceStatus.PROGRAM_STILL_RUNNING;
    }

    //All Apps are STOPPED, delete them
    for (ApplicationSpecification appSpec : allSpecs) {
      Id.Application id = Id.Application.from(namespace.getId(), appSpec.getName());
      deleteApp(id);
    }
    return AbstractAppFabricHttpHandler.AppFabricServiceStatus.OK;
  }

  public AbstractAppFabricHttpHandler.AppFabricServiceStatus deleteApp(final Id.Application appId) throws Exception {
    //Check if all are stopped.
    boolean appRunning = checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program programId) {
        return programId.getApplication().equals(appId);
      }
    }, ProgramType.values());

    if (appRunning) {
      return AbstractAppFabricHttpHandler.AppFabricServiceStatus.PROGRAM_STILL_RUNNING;
    }

    ApplicationSpecification spec;
    try {
      spec = store.getApplication(appId);
    } catch (ApplicationNotFoundException e) {
      return AbstractAppFabricHttpHandler.AppFabricServiceStatus.PROGRAM_NOT_FOUND;
    }

    //Delete the schedules
    for (WorkflowSpecification workflowSpec : spec.getWorkflows().values()) {
      Id.Program workflowProgramId = Id.Program.from(appId, workflowSpec.getName());
      scheduler.deleteSchedules(workflowProgramId, SchedulableProgramType.WORKFLOW);
    }

    deleteMetrics(appId.getNamespaceId(), appId.getId());

    //Delete all preferences of the application and of all its programs
    deletePreferences(appId);

    // Delete all streams and queues state of each flow
    // TODO: This should be unified with the DeletedProgramHandlerStage
    for (FlowSpecification flowSpecification : spec.getFlows().values()) {
      Id.Program flowProgramId = Id.Program.from(appId, flowSpecification.getName());

      // Collects stream name to all group ids consuming that stream
      Multimap<String, Long> streamGroups = HashMultimap.create();
      for (FlowletConnection connection : flowSpecification.getConnections()) {
        if (connection.getSourceType() == FlowletConnection.Type.STREAM) {
          long groupId = FlowUtils.generateConsumerGroupId(flowProgramId, connection.getTargetName());
          streamGroups.put(connection.getSourceName(), groupId);
        }
      }
      // Remove all process states and group states for each stream
      String namespace = String.format("%s.%s", flowProgramId.getApplicationId(), flowProgramId.getId());
      for (Map.Entry<String, Collection<Long>> entry : streamGroups.asMap().entrySet()) {
        streamConsumerFactory.dropAll(Id.Stream.from(appId.getNamespaceId(), entry.getKey()),
                                      namespace, entry.getValue());
      }

      queueAdmin.dropAllForFlow(appId.getNamespaceId(), appId.getId(), flowSpecification.getName());
    }
    deleteProgramLocations(appId);

    Location appArchive = store.getApplicationArchiveLocation(appId);
    Preconditions.checkNotNull(appArchive, "Could not find the location of application", appId.getId());
    appArchive.delete();
    store.removeApplication(appId);
    return AbstractAppFabricHttpHandler.AppFabricServiceStatus.OK;
  }

  private void deleteHandler(Id.Program programId, ProgramType type) throws ExecutionException {
    try {
      switch (type) {
        case FLOW:
          stopProgramIfRunning(programId, type);
          break;
        case PROCEDURE:
          stopProgramIfRunning(programId, type);
          break;
        case WORKFLOW:
          scheduler.deleteSchedules(programId, SchedulableProgramType.WORKFLOW);
          break;
        case MAPREDUCE:
          //no-op
          break;
        case SERVICE:
          stopProgramIfRunning(programId, type);
          break;
        case WORKER:
          stopProgramIfRunning(programId, type);
          break;
      }
    } catch (InterruptedException e) {
      throw new ExecutionException(e);
    } catch (SchedulerException e) {
      throw new ExecutionException(e);
    }
  }

  private void stopProgramIfRunning(Id.Program programId, ProgramType type)
    throws InterruptedException, ExecutionException {
    ProgramRuntimeService.RuntimeInfo programRunInfo = findRuntimeInfo(programId.getNamespaceId(),
                                                                       programId.getApplicationId(),
                                                                       programId.getId(),
                                                                       type, programService);
    if (programRunInfo != null) {
      doStop(programRunInfo);
    }
  }

  private void doStop(ProgramRuntimeService.RuntimeInfo runtimeInfo)
    throws ExecutionException, InterruptedException {
    Preconditions.checkNotNull(runtimeInfo, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND));
    ProgramController controller = runtimeInfo.getController();
    controller.stop().get();
  }

  private void deleteSchedules(String namespaceId, ApplicationSpecification specification)
    throws IOException, SchedulerException {

    // Delete the existing schedules.
    for (Map.Entry<String, ScheduleSpecification> entry : specification.getSchedules().entrySet()) {
      ScheduleProgramInfo programInfo = entry.getValue().getProgram();
      Id.Program programId = Id.Program.from(namespaceId, specification.getName(), programInfo.getProgramName());
      scheduler.deleteSchedules(programId, programInfo.getProgramType());
    }
  }

  private void setupSchedules(String namespaceId, ApplicationSpecification specification)
    throws IOException, SchedulerException {

    deleteSchedules(namespaceId, specification);
    // Add new schedules.
    for (Map.Entry<String, ScheduleSpecification> entry : specification.getSchedules().entrySet()) {
      ScheduleProgramInfo programInfo = entry.getValue().getProgram();
      Id.Program programId = Id.Program.from(namespaceId, specification.getName(),
                                             programInfo.getProgramName());
      List<Schedule> scheduleList = Lists.newArrayList();
      scheduleList.add(entry.getValue().getSchedule());
      scheduler.schedule(programId, programInfo.getProgramType(), scheduleList);
    }
  }

  protected ProgramRuntimeService.RuntimeInfo findRuntimeInfo(String namespaceId, String appId,
                                                              String flowId, ProgramType typeId,
                                                              ProgramRuntimeService runtimeService) {
    ProgramType type = ProgramType.valueOf(typeId.name());
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
                               namespaceId, flowId);

    Id.Program programId = Id.Program.from(namespaceId, appId, flowId);

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }
}

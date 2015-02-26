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

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.ApplicationNotFoundException;
import co.cask.cdap.common.exception.NamespaceNotFoundException;
import co.cask.cdap.common.http.AbstractBodyConsumer;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
  private final LocationFactory locationFactory;
  private final ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory;
  private final CConfiguration configuration;
  private final Scheduler scheduler;
  private final Store store;
  private final ProgramRuntimeService programService;

  @Inject
  public AppLifecycleService(CConfiguration configuration, Scheduler scheduler, StoreFactory storeFactory,
                             LocationFactory locationFactory,
                             ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory,
                             ProgramRuntimeService programService) {
    this.configuration = configuration;
    this.scheduler = scheduler;
    this.store = storeFactory.create();
    this.locationFactory = locationFactory;
    this.managerFactory = managerFactory;
    this.programService = programService;
  }

  public ApplicationSpecification getApp(Id.Application appId) throws ApplicationNotFoundException {
    return store.getApplication(appId);
  }

  public Iterable<ApplicationSpecification> getAllApps(Id.Namespace namespaceId) throws NamespaceNotFoundException {
    return store.getAllApplications(namespaceId);
  }

  public void deploy(final Id.Namespace namespace, @Nullable final String appName,
                     DeploymentInfo deploymentInfo) throws Exception {
    if (store.getNamespace(namespace) == null) {
      throw new NamespaceNotFoundException(namespace);
    }

      Manager<DeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(new ProgramTerminator() {
        @Override
        public void stop(Id.Namespace id, Id.Program programId, ProgramType type) throws ExecutionException {
          deleteHandler(programId, type);
        }
      });

      ApplicationWithPrograms applicationWithPrograms =
        manager.deploy(namespace, appName, deploymentInfo).get();
      ApplicationSpecification specification = applicationWithPrograms.getSpecification();
      setupSchedules(namespace.getId(), specification);
  }

  private void deleteHandler(Id.Program programId, ProgramType type)
    throws ExecutionException {
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
    }
  }

  private void deleteSchedules(String namespaceId, ApplicationSpecification specification) throws IOException {
    // Delete the existing schedules.
    for (Map.Entry<String, ScheduleSpecification> entry : specification.getSchedules().entrySet()) {
      ScheduleProgramInfo programInfo = entry.getValue().getProgram();
      Id.Program programId = Id.Program.from(namespaceId, specification.getName(), programInfo.getProgramName());
      scheduler.deleteSchedules(programId, programInfo.getProgramType());
    }
  }

  private void setupSchedules(String namespaceId, ApplicationSpecification specification) throws IOException {

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

  protected ProgramRuntimeService.RuntimeInfo findRuntimeInfo(String namespaceId, String appId,
                                                              String flowId, ProgramType typeId,
                                                              ProgramRuntimeService runtimeService) {
    ProgramType type = ProgramType.valueOf(typeId.name());
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND), namespaceId, flowId);

    Id.Program programId = Id.Program.from(namespaceId, appId, flowId);

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

  private ApplicationRecord makeAppRecord(ApplicationSpecification appSpec) {
    return new ApplicationRecord("App", appSpec.getName(), appSpec.getName(), appSpec.getDescription());
  }
}

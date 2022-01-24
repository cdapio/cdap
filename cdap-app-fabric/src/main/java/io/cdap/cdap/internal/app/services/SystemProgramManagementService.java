/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import com.google.inject.Inject;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * System program management service for ensuring programs are running/stopped as expected
 */
public class SystemProgramManagementService extends AbstractRetryableScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(SystemProgramManagementService.class);

  private final long scheduleIntervalInMillis;
  private final ProgramRuntimeService programRuntimeService;
  private final ProgramLifecycleService programLifecycleService;
  private final AtomicReference<Map<ProgramId, Arguments>> programsEnabled;

  @Inject
  SystemProgramManagementService(CConfiguration cConf, ProgramRuntimeService programRuntimeService,
                                 ProgramLifecycleService programLifecycleService) {
    super(RetryStrategies
            .fixDelay(cConf.getLong(Constants.AppFabric.SYSTEM_PROGRAM_SCAN_INTERVAL_SECONDS), TimeUnit.SECONDS));
    this.scheduleIntervalInMillis = TimeUnit.SECONDS
      .toMillis(cConf.getLong(Constants.AppFabric.SYSTEM_PROGRAM_SCAN_INTERVAL_SECONDS));
    this.programRuntimeService = programRuntimeService;
    this.programLifecycleService = programLifecycleService;
    this.programsEnabled = new AtomicReference<>();
  }

  /**
   * Sets the map of programs that are currently enabled along with their runtime args.
   * The programs that are not present in map will be stopped during the next run of the service.
   *
   * @param programsEnabled the set of programs to enable
   */
  public void setProgramsEnabled(Map<ProgramId, Arguments> programsEnabled) {
    this.programsEnabled.set(new HashMap<>(programsEnabled));
  }

  @Override
  protected long runTask() {
    if (programsEnabled.get() == null) {
      LOG.debug("Programs to run not yet set, will be retried.");
      return scheduleIntervalInMillis;
    }
    reconcilePrograms();
    return scheduleIntervalInMillis;
  }

  private void reconcilePrograms() {
    Map<ProgramId, Arguments> enabledProgramsMap = new HashMap<>(this.programsEnabled.get());
    Set<ProgramRunId> programRunsToStop = new HashSet<>();
    //Get all current runs
    List<ProgramRuntimeService.RuntimeInfo> runtimeInfos = programRuntimeService.listAll(ProgramType.values());
    //sort by descending order of runtime
    runtimeInfos.sort((runtimeInfo1, runtimeInfo2) ->
                        Long.compare(RunIds.getTime(runtimeInfo2.getController().getProgramRunId().getRun(),
                                                    TimeUnit.MILLISECONDS),
                                     RunIds.getTime(runtimeInfo1.getController().getProgramRunId().getRun(),
                                                    TimeUnit.MILLISECONDS))
    );
    //Find programs to run and stop
    for (ProgramRuntimeService.RuntimeInfo runtimeInfo : runtimeInfos) {
      ProgramId programId = runtimeInfo.getProgramId();
      if (!programId.getNamespaceId().equals(NamespaceId.SYSTEM)) {
        //We care only about system programs
        continue;
      }
      //Remove from map since it is already running.
      //If map doesnt have entry, program is disabled or it is an additional run.
      if (enabledProgramsMap.remove(programId) == null) {
        programRunsToStop.add(runtimeInfo.getController().getProgramRunId());
      }
    }
    //start programs
    startPrograms(enabledProgramsMap);
    //stop programs
    programRunsToStop.forEach(this::stopProgram);
  }

  private void startPrograms(Map<ProgramId, Arguments> enabledProgramsMap) {
    for (ProgramId programId : enabledProgramsMap.keySet()) {
      Map<String, String> overrides = enabledProgramsMap.get(programId).asMap();
      LOG.debug("Starting program {} with args {}", programId, overrides);
      try {
        programLifecycleService.start(programId, overrides, false, false);
      } catch (ConflictException ex) {
        // Ignore if the program is already running.
        LOG.debug("Program {} is already running.", programId);
      } catch (Exception ex) {
        LOG.warn("Could not start program {} , will be retried in next run.", programId, ex);
      }
    }
  }

  private void stopProgram(ProgramRunId programRunId) {
    LOG.debug("Stopping program run {} ", programRunId);
    try {
      programLifecycleService.stop(programRunId.getParent(), programRunId.getRun());
    } catch (Exception ex) {
      LOG.warn("Could not stop program run {} , will be retried .", programRunId, ex);
    }
  }
}

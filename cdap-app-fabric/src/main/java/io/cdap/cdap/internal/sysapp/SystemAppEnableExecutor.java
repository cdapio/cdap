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

package io.cdap.cdap.internal.sysapp;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.sysapp.SystemAppStep.Arguments;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Deploys an application with given arguments. Also runs all programs corresponding to the
 * application.
 */
public class SystemAppEnableExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(SystemAppEnableExecutor.class);
  private static final Logger LIMITED_LOGGER = Loggers
    .sampling(LOG, LogSamplers.limitRate(TimeUnit.SECONDS.toMillis(100)));

  private static final Gson GSON = new Gson();
  private final ApplicationLifecycleService appLifecycleService;
  private final ProgramLifecycleService programLifecycleService;

  @Inject
  SystemAppEnableExecutor(ApplicationLifecycleService appLifecycleService,
                          ProgramLifecycleService programLifecycleService) {
    this.appLifecycleService = appLifecycleService;
    this.programLifecycleService = programLifecycleService;
  }

  /**
   * Deploys an application with given arguments and starts all its programs.
   */
  public void deployAppAndStartPrograms(Arguments arguments) {
    // TODO(CDAP-16243): Make deploy app and start programs idempotent to not have transactional issues.
    ApplicationWithPrograms appDetail = retryableDeploySystemApp(arguments);
    if (appDetail == null) {
      return;
    }

    // TODO(CDAP-16243): Fix logic to restart programs for app with difference version/config.
    for (ProgramDescriptor program : appDetail.getPrograms()) {
      try {
        startProgram(program.getProgramId());
      } catch (Exception ex) {
        LOG.error("Failed to start program {} for app {}.", program.getProgramId(), arguments.getId(), ex);
      }
    }
  }

  @Nullable
  private ApplicationWithPrograms retryableDeploySystemApp(Arguments arguments) {
    try {
      return Retries.callWithRetries(() -> {
        try {
          return deploySystemApp(arguments);
        } catch (RetryableException ex) {
          LIMITED_LOGGER.debug("Failed to deploy app {}. Will retry", arguments.getId(), ex);
          throw ex;
        }
      },
      RetryStrategies.fixDelay(6, TimeUnit.SECONDS));
    } catch (Exception ex) {
      LOG.error("Failed to deploy app {} with exception", arguments.getId(), ex);
      return null;
    }
  }

  private ApplicationWithPrograms deploySystemApp(Arguments arguments) throws Exception {
    ApplicationId appId = arguments.getId();
    ArtifactSummary artifactSummary = arguments.getArtifact();

    KerberosPrincipalId ownerPrincipalId =
      arguments.getOwnerPrincipal() == null ? null : new KerberosPrincipalId(arguments.getOwnerPrincipal());

    // if we don't null check, it gets serialized to "null"
    String configString = arguments.getConfig() == null ? null : GSON.toJson(arguments.getConfig());

    try {
      return appLifecycleService.deployApp(appId.getParent(), appId.getApplication(), appId.getVersion(),
                                           artifactSummary, configString, x -> { },
                                           ownerPrincipalId, arguments.canUpdateSchedules(), false,
                                           Collections.emptyMap());

    } catch (UnauthorizedException | InvalidArtifactException e) {
      throw e;
    } catch (DatasetManagementException e) {
      if (e.getCause() instanceof UnauthorizedException) {
        throw (UnauthorizedException) e.getCause();
      } else {
        throw new RetryableException(e);
      }
    } catch (Exception e) {
      throw new RetryableException(e);
    }
  }

  private void startProgram(ProgramId programId) throws Exception {
    try {
      ProgramStatus currentStatus = programLifecycleService.getProgramStatus(programId);
      // If a system app is already running, it needs to be restarted as app artifact/arguments might have changed.
      // Restart should trigger running programs with latest version.
      // TODO(CDAP-16243): Find smarter way to trigger restart of programs rather than always restarting. May be
      //                   checking if artifact version has changed would be a good start.
      try {
        if (currentStatus == ProgramStatus.RUNNING) {
          programLifecycleService.stop(programId);
        }
      } catch (ConflictException e) {
        // Will reach here if the program is already stopped, which means it tried to stop after the status check above.
        // ignore this, as it means the program is stopped as we wanted.
      }
      programLifecycleService.run(programId, Collections.emptyMap(), false);
    } catch (ConflictException e) {
      // thrown if the program is already running, which means it was started after the status check above.
      // ignore this, as it means the program is running as expected
    } catch (NotFoundException e) {
      // use a nicer error message
      throw new IllegalArgumentException(String.format("Cannot start %s because it does not exist.", programId), e);
    }
  }
}


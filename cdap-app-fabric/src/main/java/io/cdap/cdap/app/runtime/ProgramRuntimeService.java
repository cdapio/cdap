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

package io.cdap.cdap.app.runtime;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.proto.ProgramLiveInfo;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.twill.api.RunId;
import org.apache.twill.api.logging.LogEntry;

/**
 * Service for interacting with the runtime system.
 */
public interface ProgramRuntimeService extends Service {

  /**
   * Represents information of a running program.
   */
  interface RuntimeInfo {

    ProgramController getController();

    ProgramType getType();

    ProgramId getProgramId();

    @Nullable
    RunId getTwillRunId();
  }

  /**
   * Runs the given program and return a {@link RuntimeInfo} about the running program. The program
   * is run if it is not already running, otherwise the {@link RuntimeInfo} of the already running
   * program is returned.
   *
   * @param programDescriptor describing the program to run
   * @param options {@link ProgramOptions} that are needed by the program.
   * @param runId {@link RunId} for the program run
   * @return A {@link ProgramController} for the running program.
   */
  RuntimeInfo run(ProgramDescriptor programDescriptor, ProgramOptions options, RunId runId);

  /**
   * Find the {@link RuntimeInfo} for a running program with the given {@link RunId}.
   *
   * @param programId The id of the program.
   * @param runId The program {@link RunId}.
   * @return A {@link RuntimeInfo} for the running program or {@code null} if no such program is
   *     found.
   */
  @Nullable
  RuntimeInfo lookup(ProgramId programId, RunId runId);

  /**
   * Get {@link RuntimeInfo} for all running programs of the given type.
   *
   * @param type Type of running programs to list.
   * @return An immutable map from {@link RunId} to {@link ProgramController}.
   */
  Map<RunId, RuntimeInfo> list(ProgramType type);

  /**
   * Get {@link RuntimeInfo} for a specified program.
   *
   * @param program The program for which the {@link RuntimeInfo} needs to be determined
   * @return An immutable map from {@link RunId} to {@link ProgramController}
   */
  Map<RunId, RuntimeInfo> list(ProgramId program);

  /**
   * Get runtime information about a running program. The content of this information is different
   * for each runtime environment. For example, in a distributed environment, this would contain the
   * YARN application id and the container information for each runnable. For in-memory, it may be
   * empty.
   */
  ProgramLiveInfo getLiveInfo(ProgramId programId);

  /**
   * Get information about running programs. Protected only to support v2 APIs
   *
   * @param types Types of program to check returns List of info about running programs.
   */
  List<RuntimeInfo> listAll(ProgramType... types);

  /**
   * Reset log levels for the given program. Only supported program types for this action are {@link
   * ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which log levels are to be
   *     reset.
   * @param loggerNames the {@link String} set of the logger names to be updated, empty means
   *     reset for all loggers.
   * @param runId the run id of the program.
   * @throws InterruptedException if there is an error while asynchronously resetting log
   *     levels.
   * @throws ExecutionException if there is an error while asynchronously resetting log levels.
   * @throws UnauthorizedException if the user does not have privileges to reset log levels for
   *     the specified program. To reset log levels for a program, a user needs {@link
   *     StandardPermission#UPDATE} on the program.
   */
   void resetProgramLogLevels(ProgramId programId, Set<String> loggerNames, @Nullable String runId) throws Exception;

  /**
   * Update log levels for the given program. Only supported program types for this action are
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which log levels are to be
   *     updated
   * @param logLevels the {@link Map} of the log levels to be updated.
   * @param runId the run id of the program.
   * @throws InterruptedException if there is an error while asynchronously updating log
   *     levels.
   * @throws ExecutionException if there is an error while asynchronously updating log levels.
   * @throws BadRequestException if the log level is not valid or the program type is not
   *     supported.
   * @throws UnauthorizedException if the user does not have privileges to update log levels for
   *     the specified program. To update log levels for a program, a user needs {@link
   *     StandardPermission#UPDATE} on the program.
   */
  void updateProgramLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels, @Nullable String runId)
      throws Exception;

  /**
   * Set instances for the given program. Only supported program types for this action are {@link
   * ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which instances are to be
   *     updated
   * @param instances the number of instances to be updated.
   * @param instances the previous number of instances.
   *
   * @throws InterruptedException if there is an error while asynchronously updating instances
   * @throws ExecutionException if there is an error while asynchronously updating instances
   * @throws BadRequestException if the number of instances specified is less than 0
   * @throws UnauthorizedException if the user does not have privileges to set instances for the
   *     specified program. To set instances for a program, a user needs {@link
   *     StandardPermission#UPDATE} on the program.
   */
  void setInstances(ProgramId programId, int instances, int oldInstances) throws Exception;
}

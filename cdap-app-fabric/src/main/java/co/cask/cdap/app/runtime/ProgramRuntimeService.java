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

package co.cask.cdap.app.runtime;

import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.RunId;

import java.util.Map;

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

    Id.Program getProgramId();
  }

  /**
   * Starts the given program and return a {@link RuntimeInfo} about the running program.
   *
   * @param program A {@link Program} to run.
   * @param options {@link ProgramOptions} that are needed by the program.
   * @return A {@link ProgramController} for the running program.
   */
  RuntimeInfo run(Program program, ProgramOptions options);

  /**
   * Find the {@link RuntimeInfo} for a running program with the given {@link RunId}.
   *
   * @param runId The program {@link RunId}.
   * @return A {@link RuntimeInfo} for the running program or {@code null} if no such program is found.
   */
  RuntimeInfo lookup(RunId runId);

  /**
   * Get {@link RuntimeInfo} for all running programs of the given type.
   *
   * @param type Type of running programs to list.
   * @return An immutable map from {@link RunId} to {@link ProgramController}.
   */
  Map<RunId, RuntimeInfo> list(ProgramType type);

  /**
   * Get runtime information about a running program. The content of this information is different
   * for each runtime environment. For example, in a distributed environment, this would contain the
   * YARN application id and the container information for each runnable. For in-memory, it may be empty.
   */
  ProgramLiveInfo getLiveInfo(Id.Program programId, ProgramType type);

  /**
   * Check if any program that satisfy the given {@link Predicate} is running.
   * Protected only to support v2 APIs
   *
   * @param predicate Get call on each running {@link Id.Program}.
   * @param types Types of program to check
   * returns True if a program is running as defined by the predicate.
   */
  boolean checkAnyRunning(Predicate<Id.Program> predicate, ProgramType... types);
}

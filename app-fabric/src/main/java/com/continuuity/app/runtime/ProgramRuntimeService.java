package com.continuuity.app.runtime;

import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.internal.app.runtime.service.LiveInfo;
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

    Type getType();

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
  Map<RunId, RuntimeInfo> list(Type type);

  /**
   * Get runtime information about a running program. The content of this information is different
   * for each runtime environment. For example, in a distributed environment, this would contain the
   * YARN application id and the container information for each runnable. For in-memory, it may be empty.
   */
  LiveInfo getLiveInfo(Id.Program programId, Type type);
}

/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.bootstrap.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Starts a program if it exists and is not running.
 */
public class ProgramStarter extends BaseStepExecutor<ProgramStarter.Arguments> {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramStarter.class);
  private final ProgramLifecycleService programLifecycleService;

  @Inject
  ProgramStarter(ProgramLifecycleService programLifecycleService) {
    this.programLifecycleService = programLifecycleService;
  }

  @Override
  public void execute(Arguments arguments) throws Exception {
    ProgramId programId = arguments.getId();

    Preconditions.checkArgument(programLifecycleService.getProgramSpecification(programId) != null,
                                "Cannot start %s because it does not exist.", programId);

    try {
      // do nothing if the program is already running
      ProgramStatus currentStatus = programLifecycleService.getProgramStatus(programId);
      if (currentStatus != ProgramStatus.STOPPED) {
        LOG.info("Program {} is in the {} state, skipping start program bootstrap step.", programId, currentStatus);
        return;
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

  /**
   * Arguments required to start a program
   */
  static class Arguments implements Validatable {
    private String namespace;
    private String application;
    private String type;
    private String name;

    @VisibleForTesting
    Arguments(String namespace, String application, String type, String name) {
      this.namespace = namespace;
      this.application = application;
      this.type = type;
      this.name = name;
    }

    private ProgramId getId() {
      ProgramType programType;
      try {
        programType = ProgramType.valueOf(type.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format("Invalid program type '%s': %s", type, e.getMessage()), e);
      }

      NamespaceId namespaceId = new NamespaceId(namespace);
      return namespaceId.app(application).program(programType, name);
    }

    @Override
    public void validate() {
      if (namespace == null || namespace.isEmpty()) {
        throw new IllegalArgumentException("Namespace must be specified");
      }
      if (application == null || application.isEmpty()) {
        throw new IllegalArgumentException("Application must be specified");
      }
      if (type == null || type.isEmpty()) {
        throw new IllegalArgumentException("Program type must be specified");
      }
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Program name must be specified");
      }
      getId();
    }
  }
}

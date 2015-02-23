/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.runtime.ProgramRunner;

/**
 * Factory for creating {@link ProgramRunner}.
 */
public interface ProgramRunnerFactory {

  /**
   * Types of program that could be created.
   */
  public enum Type {
    FLOW,
    FLOWLET,
    PROCEDURE,
    MAPREDUCE,
    SPARK,
    WORKFLOW,
    WEBAPP,
    WORKER,
    WORKER_COMPONENT,
    SERVICE,
    SERVICE_COMPONENT
  }

  ProgramRunner create(Type programType);
}

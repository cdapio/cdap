/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.app.deploy;

import io.cdap.cdap.app.runtime.ProgramController;

/**
 * This interface is used for providing the abstraction for executing program-run logic either
 * In-memory or remotely.
 */
public interface ProgramRunDispatcher {

  /**
   * Creates a new instance of {@link ProgramController} for the given program run info.
   *
   * @param dispatcherContext Context Information required to perform program-run operation.
   * @return An instance of {@link ProgramController}
   * @throws Exception if program-run operation fails
   */
  ProgramController dispatchProgram(ProgramRunDispatcherContext dispatcherContext) throws Exception;

}

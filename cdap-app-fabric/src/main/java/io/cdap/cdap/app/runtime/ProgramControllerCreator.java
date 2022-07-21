/*
 * Copyright © 2018 Cask Data, Inc.
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

import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.TwillController;

/**
 * This interface provides the capability to create {@link ProgramController} from {@link TwillController}.
 */
public interface ProgramControllerCreator {

  /**
   * Creates a {@link ProgramController} for the given program that was launched as a Twill application.
   *
   * @param programRunId the program run id of the program being launched
   * @param twillController the {@link TwillController} to interact with the twill application
   * @return a new instance of {@link ProgramController}.
   */
  ProgramController createProgramController(ProgramRunId programRunId, TwillController twillController);
}

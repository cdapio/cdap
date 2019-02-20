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

package co.cask.cdap.app.runtime;

import co.cask.cdap.master.spi.program.ProgramController;
import co.cask.cdap.proto.id.ProgramId;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;

/**
 * This interface provides the capability to create {@link ProgramController} from {@link TwillController}.
 */
public interface ProgramControllerCreator {

  /**
   * Creates a {@link ProgramController} for the given program that was launched as a Twill application.
   *
   * @param twillController the {@link TwillController} to interact with the twill application
   * @param programId the program id of the program being launched
   * @param runId the run id of the particular execution
   * @return a new instance of {@link ProgramController}.
   */
  ProgramController createProgramController(TwillController twillController, ProgramId programId, RunId runId);
}

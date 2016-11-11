/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.distributed;

import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramController;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;

/**
 * Provides method to create {@link ProgramController} in distributed mode.
 */
public interface DistributedProgramControllerFactory {

  /**
   * Creates a {@link ProgramController} for the given program that was launched as a Twill application.
   *
   * @param twillController the {@link TwillController} to interact with the twill application
   * @param programDescriptor information for the Program being launched
   * @param runId the run id of the particular execution
   * @return a new instance of {@link ProgramController}.
   */
  ProgramController createProgramController(TwillController twillController,
                                            ProgramDescriptor programDescriptor, RunId runId);
}

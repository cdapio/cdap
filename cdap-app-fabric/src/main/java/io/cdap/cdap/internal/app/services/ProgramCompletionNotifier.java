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

package io.cdap.cdap.internal.app.services;

import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProgramRunId;

/**
 * Interface for receiving notification on program completion event.
 */
public interface ProgramCompletionNotifier {

  /**
   * Invoked when the given program completed.
   *
   * @param programRunId the program run that is completed
   * @param completionStatus the status where the program completed with. It must be one of the end state
   *                         as reflected by {@link ProgramRunStatus#isEndState()}.
   */
  void onProgramCompleted(ProgramRunId programRunId, ProgramRunStatus completionStatus);
}

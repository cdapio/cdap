/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.Workflow;

/**
 * An interface for Programs which can run in a {@link Workflow}. Please see {@link SchedulableProgramType}.
 */
interface ProgramWorkflowRunner {

  /**
   * Programs which want to support running in Workflow should give the implementation to get a {@link Runnable} of
   * for the program name in the workflow.
   *
   * @param name name of program in workflow
   * @return {@link Runnable} which will be called to execute the program
   */
  Runnable create(String name);
}

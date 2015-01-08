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
package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;

import java.util.concurrent.Callable;

/**
 * An interface for Programs which can run in a {@link Workflow}. Please see {@link SchedulableProgramType}.
 */
public interface ProgramWorkflowRunner {

  /**
   * Programs which want to support running in Workflow should give the implementation to get a {@link Callable} of
   * {@link RuntimeContext} from the program name in the workflow.
   *
   * @param name name of program in workflow
   * @return {@link Callable} of {@link RuntimeContext} which will be called to execute the program
   */
  Callable<RuntimeContext> create(String name);

  /**
   * Programs which want to run in Workflow should give an implementation to run the given {@link Program} with
   * {@link ProgramOptions} and should return the {@link RuntimeContext} of the run.
   *
   * @param program {@link Program} to run
   * @param options {@link ProgramOptions} with which this program should run
   * @return {@link RuntimeContext} of the program run
   * @throws Exception if the execution of the program fails.
   */
  RuntimeContext runAndWait(Program program, ProgramOptions options) throws Exception;
}

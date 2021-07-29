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

package io.cdap.cdap.app.runtime;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.ProgramType;

import javax.annotation.Nullable;

/**
 * Factory for creating {@link ProgramRunner} and support classes.
 */
public interface ProgramRunnerFactory {

  /**
   * Creates a {@link ProgramRunner} for the given {@link ProgramType}.
   *
   * @param programOptions
   * @param cConf configuration for the run
   * @return a {@link ProgramRunner} that can execute the given program type.
   * @throws IllegalArgumentException if no {@link ProgramRunner} is found for the given program type
   */
  ProgramRunner create(ProgramOptions programOptions, CConfiguration cConf);

  /**
   * Creates a {@link ProgramControllerCreator} for the given {@link ProgramType}.
   *
   * @param programType type of program
   * @return a {@link ProgramControllerCreator} that can execute the given program type.
   * @throws IllegalArgumentException if no {@link ProgramRunner} is found for the given program type
   */
  ProgramControllerCreator createProgramControllerCreator(ProgramType programType);

  /**
   * Creates a {@link ProgramClassLoaderProvider} for the given {@link ProgramType}.
   *
   * @param programType type of program
   * @return a {@link ProgramClassLoaderProvider} that can execute the given program type.
   * @throws IllegalArgumentException if no {@link ProgramRunner} is found for the given program type
   */
  @Nullable
  ProgramClassLoaderProvider createProgramClassLoaderProvider(ProgramType programType);
}

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

package io.cdap.cdap.app.runtime;

import io.cdap.cdap.proto.ProgramType;

import java.util.Map;

/**
 * Factory for creating program parent classloaders.
 */
public interface ProgramRunnerClassLoaderFactory {
  /**
   * Directly creates a ClassLoader for the given program type.
   * This is useful if you need the program class loader but do not need to run a program.
   *
   * @param programType The type of program
   * @return a {@link ClassLoader} for the given program runner
   */
  ClassLoader createProgramClassLoader(ProgramType programType);

  /**
   * Creates a map of classloaders for all programs types.
   *
   * @return An map of program types to class loaders
   */
  Map<ProgramType, ClassLoader> createProgramClassLoaders();
}

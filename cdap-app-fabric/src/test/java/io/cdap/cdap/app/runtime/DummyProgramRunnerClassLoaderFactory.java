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

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.ProgramType;

/**
 * A dummy implementation of {@link ProgramRunnerClassLoaderFactory} that doesn't support any program type.
 * It is purely for testing purpose.
 */
public class DummyProgramRunnerClassLoaderFactory implements ProgramRunnerClassLoaderFactory {
  @Override
  public ClassLoader createProgramClassLoader(CConfiguration cConf, ProgramType programType) {
    throw new IllegalArgumentException("Program type not supported: " + programType);
  }
}

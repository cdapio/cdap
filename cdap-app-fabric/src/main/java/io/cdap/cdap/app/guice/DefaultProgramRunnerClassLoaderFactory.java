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

package io.cdap.cdap.app.guice;

import io.cdap.cdap.app.runtime.ProgramRunnerClassLoaderFactory;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import io.cdap.cdap.proto.ProgramType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;

/**
 * {@link io.cdap.cdap.app.runtime.ProgramRunnerClassLoaderFactory} which retrieve a program runner classloader from
 * the {@link io.cdap.cdap.app.runtime.ProgramRuntimeProvider}.
 */
public class DefaultProgramRunnerClassLoaderFactory implements ProgramRunnerClassLoaderFactory {

  private final CConfiguration cConf;
  private final ProgramRuntimeProviderLoader runtimeProviderLoader;

  @Inject
  public DefaultProgramRunnerClassLoaderFactory(CConfiguration cConf,
                                                ProgramRuntimeProviderLoader programRuntimeProviderLoader) {
    this.cConf = cConf;
    this.runtimeProviderLoader = programRuntimeProviderLoader;
  }

  @Override
  public ClassLoader createProgramClassLoader(ProgramType programType) {
    return runtimeProviderLoader.get(programType).createProgramClassLoader(cConf, programType);
  }

  @Override
  public Map<ProgramType, ClassLoader> createProgramClassLoaders() {
    Map<ProgramType, ClassLoader> classLoaderMap = new HashMap<>();
    for (Map.Entry<ProgramType, ProgramRuntimeProvider> entry : runtimeProviderLoader.getAll().entrySet()) {
      classLoaderMap.put(entry.getKey(), entry.getValue().createProgramClassLoader(cConf, entry.getKey()));
    }
    return Collections.unmodifiableMap(classLoaderMap);
  }
}

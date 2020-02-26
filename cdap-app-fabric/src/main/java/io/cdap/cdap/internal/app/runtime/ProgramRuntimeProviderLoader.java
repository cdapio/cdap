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

package io.cdap.cdap.internal.app.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.proto.ProgramType;

import java.util.ServiceLoader;
import java.util.Set;

/**
 * A singleton class for discovering {@link ProgramRuntimeProvider} through the runtime extension mechanism that uses
 * the Java {@link ServiceLoader} architecture.
 */
@Singleton
public class ProgramRuntimeProviderLoader extends AbstractExtensionLoader<ProgramType, ProgramRuntimeProvider> {
  private final CConfiguration cConf;

  @VisibleForTesting
  @Inject
  public ProgramRuntimeProviderLoader(CConfiguration cConf) {
    super(cConf.get(Constants.AppFabric.RUNTIME_EXT_DIR, ""));
    this.cConf = cConf;
  }

  @Override
  public Set<ProgramType> getSupportedTypesForProvider(ProgramRuntimeProvider programRuntimeProvider) {
    // See if the provide supports the required program type
    ProgramRuntimeProvider.SupportedProgramType supportedTypes =
      programRuntimeProvider.getClass().getAnnotation(ProgramRuntimeProvider.SupportedProgramType.class);
    ImmutableSet.Builder<ProgramType> types = ImmutableSet.builder();

    for (ProgramType programType : supportedTypes.value()) {
      if (programRuntimeProvider.isSupported(programType, cConf)) {
        System.out.println("### spark compat is supported");
        types.add(programType);
      }
    }
    return types.build();
  }
}

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

package io.cdap.cdap.app.preview;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.internal.app.preview.PreviewRunnerServiceStopper;

/**
 *
 */
public class PreviewRunnerKillerModule extends RuntimeModule {
  @Override
  public Module getInMemoryModules() {
    return getStandaloneModules();
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(PreviewRunnerServiceStopper.class).to(DefaultPreviewRunnerManager.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(PreviewRunnerServiceStopper.class).toInstance(runnerId -> {
          // TODO [SAGAR] no op for now
        });
      }
    };
  }
}

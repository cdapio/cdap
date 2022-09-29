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

import com.google.inject.PrivateModule;
import io.cdap.cdap.app.runtime.ProgramRunnerClassLoaderFactory;

/**
 * Bindings for {@link io.cdap.cdap.app.runtime.ProgramRunnerClassLoaderFactory}.
 */
public class ProgramRunnerClassLoaderModule extends PrivateModule {
  @Override
  protected void configure() {
    bind(ProgramRunnerClassLoaderFactory.class).to(DefaultProgramRunnerClassLoaderFactory.class);
    expose(ProgramRunnerClassLoaderFactory.class);
  }
}

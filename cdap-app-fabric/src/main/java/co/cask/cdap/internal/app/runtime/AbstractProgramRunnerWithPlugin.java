/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;

import java.io.File;
import javax.annotation.Nullable;

/**
 * Provides method to create {@link PluginInstantiator} for Program Runners
 */
public abstract class AbstractProgramRunnerWithPlugin implements ProgramRunner {

  private final CConfiguration cConf;

  public AbstractProgramRunnerWithPlugin(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Nullable
  protected PluginInstantiator createPluginInstantiator(ProgramOptions options, ClassLoader classLoader) {
    if (options.getArguments().getOption(ProgramOptionConstants.PLUGIN_DIR) == null) {
      return null;
    }

    return new PluginInstantiator(
      cConf, classLoader, new File(options.getArguments().getOption(ProgramOptionConstants.PLUGIN_DIR)));
  }
}

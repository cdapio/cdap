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
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Service;
import org.apache.twill.internal.ServiceListenerAdapter;

import java.io.Closeable;
import java.io.File;
import javax.annotation.Nullable;

/**
 * Provides method to create {@link PluginInstantiator} for Program Runners
 */
public abstract class AbstractProgramRunnerWithPlugin implements ProgramRunner {

  protected final CConfiguration cConf;

  public AbstractProgramRunnerWithPlugin(CConfiguration cConf) {
    this.cConf = cConf;
  }

  /**
   * Creates a {@link PluginInstantiator} based on the {@link ProgramOptionConstants#PLUGIN_DIR} in
   * the system arguments in the given {@link ProgramOptions}.
   *
   * @param options the program runner options
   * @param classLoader the parent ClassLoader for the {@link PluginInstantiator} to use
   * @return A new {@link PluginInstantiator} or {@code null} if no plugin is available.
   */
  @Nullable
  protected PluginInstantiator createPluginInstantiator(ProgramOptions options, ClassLoader classLoader) {
    if (!options.getArguments().hasOption(ProgramOptionConstants.PLUGIN_DIR)) {
      return null;
    }
    return new PluginInstantiator(
      cConf, classLoader, new File(options.getArguments().getOption(ProgramOptionConstants.PLUGIN_DIR)));
  }

  /**
   * Creates a service listener to cleanup closeables.
   */
  protected Service.Listener createRuntimeServiceListener(final Iterable<Closeable> closeables) {
    return new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        closeAllQuietly(closeables);
      }

      @Override
      public void failed(Service.State from, @Nullable final Throwable failure) {
        closeAllQuietly(closeables);
      }
    };
  }

  protected void closeAllQuietly(Iterable<Closeable> closeables) {
    for (Closeable c : closeables) {
      Closeables.closeQuietly(c);
    }
  }
}

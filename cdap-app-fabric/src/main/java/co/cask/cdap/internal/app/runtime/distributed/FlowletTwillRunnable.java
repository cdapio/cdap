/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.app.guice.DataFabricFacadeModule;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueClientFactory;
import co.cask.cdap.internal.app.queue.QueueReaderFactory;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.runtime.flow.FlowletProgramRunner;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.Command;
import org.apache.twill.api.TwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * The {@link TwillRunnable} for running a flowlet instance.
 */
final class FlowletTwillRunnable extends AbstractProgramTwillRunnable<FlowletProgramRunner> {

  private static final Logger LOG = LoggerFactory.getLogger(FlowletTwillRunnable.class);

  FlowletTwillRunnable(String name) {
    super(name);
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    try {
      super.handleCommand(command);
    } catch (ExecutionException e) {
      Throwable rootCause = Throwables.getRootCause(e);
      if (!"Suspension not allowed".equals(rootCause.getMessage())
        && !"Resumption not allowed".equals(rootCause.getMessage())) {
        throw e;
      }
      LOG.debug("Command failure ignored", e);
    }
  }

  @Override
  protected Arguments resolveScope(Arguments arguments) {
    return new BasicArguments(RuntimeArguments.extractScope(FlowUtils.FLOWLET_SCOPE, name, arguments.asMap()));
  }

  @Override
  protected Module createModule(CConfiguration cConf, Configuration hConf,
                                ProgramOptions programOptions, ProgramRunId programRunId) {
    Module module = super.createModule(cConf, hConf, programOptions, programRunId);
    return new AbstractModule() {
      @Override
      protected void configure() {
        install(module);

        // Add bindings for Flow operations.
        install(new DataFabricFacadeModule());
        bind(QueueReaderFactory.class).in(Scopes.SINGLETON);
        bind(QueueClientFactory.class).to(HBaseQueueClientFactory.class).in(Singleton.class);
      }
    };
  }

  @Override
  protected Map<String, String> getExtraSystemArguments() {
    Map<String, String> args = new HashMap<>(super.getExtraSystemArguments());
    args.put(ProgramOptionConstants.FLOWLET_NAME, name);
    return args;
  }
}

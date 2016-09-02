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
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.runtime.flow.FlowletProgramRunner;
import com.google.common.base.Throwables;
import org.apache.twill.api.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 *
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
}

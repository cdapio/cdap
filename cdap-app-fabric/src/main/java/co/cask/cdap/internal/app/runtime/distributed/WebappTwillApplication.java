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

import co.cask.cdap.api.Resources;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.webapp.WebappProgramRunner;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Throwables;
import org.apache.twill.api.EventHandler;

import java.util.Map;


/**
 * Twill application wrapper for webapp.
 */
public final class WebappTwillApplication extends AbstractProgramTwillApplication {

  private final Program program;
  private final Resources resources;

  protected WebappTwillApplication(Program program, Arguments runtimeArgs,
                                   Map<String, LocalizeResource> localizeResources,
                                   EventHandler eventHandler) {
    super(program, localizeResources, eventHandler);
    this.program = program;
    this.resources = SystemArguments.getResources(runtimeArgs, null);
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.WEBAPP;
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    try {
      String serviceName = WebappProgramRunner.getServiceName(ProgramType.WEBAPP, program);
      runnables.put(serviceName, new RunnableResource(
        new WebappTwillRunnable(serviceName),
        createResourceSpec(resources, 1)
      ));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}

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

import co.cask.cdap.app.program.Program;
import co.cask.cdap.internal.app.runtime.webapp.WebappProgramRunner;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Throwables;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;

import java.util.Map;


/**
 * Twill application wrapper for webapp.
 */
public final class WebappTwillApplication extends AbstractProgramTwillApplication {

  private final Program program;

  protected WebappTwillApplication(Program program,
                                   Map<String, LocalizeResource> localizeResources,
                                   EventHandler eventHandler) {
    super(program, localizeResources, eventHandler);
    this.program = program;
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.WEBAPP;
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();

    try {
      String serviceName = WebappProgramRunner.getServiceName(ProgramType.WEBAPP, program);
      runnables.put(serviceName, new RunnableResource(
        new WebappTwillRunnable(serviceName),
        resourceSpec
      ));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}

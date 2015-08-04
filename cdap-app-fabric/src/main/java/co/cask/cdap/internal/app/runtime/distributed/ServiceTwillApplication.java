/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;

import java.util.Map;

/**
 * The {@link TwillApplication} for running {@link Service} in distributed mode.
 */
public class ServiceTwillApplication extends AbstractProgramTwillApplication {

  private final ServiceSpecification spec;

  public ServiceTwillApplication(Program program, ServiceSpecification spec,
                                 Map<String, LocalizeResource> localizeResources,
                                 EventHandler eventHandler) {
    super(program, localizeResources, eventHandler);
    this.spec = spec;
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.SERVICE;
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    // Add a runnable for the service handler
    runnables.put(spec.getName(), new RunnableResource(
      new ServiceTwillRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
      createResourceSpec(spec.getResources(), spec.getInstances())
    ));
  }
}

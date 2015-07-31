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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;

import java.util.Map;

/**
 * The {@link TwillApplication} for running {@link Workflow} in distributed mode.
 */
public class WorkflowTwillApplication extends AbstractProgramTwillApplication {

  private final WorkflowSpecification spec;
  private final Resources resources;

  public WorkflowTwillApplication(Program program, WorkflowSpecification spec,
                                  Map<String, LocalizeResource> localizeResources,
                                  EventHandler eventHandler,
                                  Resources resources) {
    super(program, localizeResources, eventHandler);
    this.spec = spec;
    this.resources = resources;
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.WORKFLOW;
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    runnables.put(spec.getName(), new RunnableResource(
      new WorkflowTwillRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
      createResourceSpec(resources, 1)
    ));
  }
}

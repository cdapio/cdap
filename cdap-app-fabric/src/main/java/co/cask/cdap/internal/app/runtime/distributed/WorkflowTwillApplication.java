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

import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;

import java.io.File;
import java.util.Map;

/**
 * The {@link TwillApplication} for running {@link Workflow} in distributed mode.
 */
public class WorkflowTwillApplication extends AbstractProgramTwillApplication {

  private static final int WORKFLOW_MEMORY_MB = 512;

  private final WorkflowSpecification spec;

  public WorkflowTwillApplication(Program program, WorkflowSpecification spec,
                                  Map<String, File> localizeFiles,
                                  EventHandler eventHandler) {
    super(program, localizeFiles, eventHandler);
    this.spec = spec;
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.WORKFLOW;
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(WORKFLOW_MEMORY_MB, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();

    runnables.put(spec.getName(), new RunnableResource(
      new WorkflowTwillRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
      resourceSpec
    ));
  }
}

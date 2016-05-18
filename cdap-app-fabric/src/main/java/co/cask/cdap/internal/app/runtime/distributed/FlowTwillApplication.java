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

import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.FlowletSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;

import java.util.Map;

/**
 * The {@link TwillApplication} for running {@link Flow} in distributed mode.
 * Each {@link Flowlet} runs in a separate YARN container.
 */
public final class FlowTwillApplication extends AbstractProgramTwillApplication {

  private final FlowSpecification spec;

  public FlowTwillApplication(Program program, FlowSpecification spec,
                              Map<String, LocalizeResource> localizeResources, EventHandler eventHandler) {
    super(program, localizeResources, eventHandler);
    this.spec = spec;
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.FLOW;
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    for (Map.Entry<String, FlowletDefinition> entry  : spec.getFlowlets().entrySet()) {
      FlowletDefinition flowletDefinition = entry.getValue();
      FlowletSpecification flowletSpec = flowletDefinition.getFlowletSpec();

      String flowletName = entry.getKey();
      runnables.put(entry.getKey(), new RunnableResource(
        new FlowletTwillRunnable(flowletName),
        createResourceSpec(flowletSpec.getResources(), flowletDefinition.getInstances())
      ));
    }
  }
}

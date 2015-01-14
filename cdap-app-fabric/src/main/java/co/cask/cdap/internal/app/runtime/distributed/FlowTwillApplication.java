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

import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.flow.flowlet.FlowletSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.Location;

import java.io.File;
import java.util.Map;

/**
 *
 */
public final class FlowTwillApplication implements TwillApplication {

  private final FlowSpecification spec;
  private final Program program;
  private final File hConfig;
  private final File cConfig;
  private final EventHandler eventHandler;

  public FlowTwillApplication(Program program, FlowSpecification spec,
                              File hConfig, File cConfig, EventHandler eventHandler) {
    this.spec = spec;
    this.program = program;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
    this.eventHandler = eventHandler;
  }

  @Override
  public TwillSpecification configure() {
    TwillSpecification.Builder.MoreRunnable moreRunnable = TwillSpecification.Builder.with()
      .setName(String.format("%s.%s.%s.%s",
                             ProgramType.FLOW.name().toLowerCase(),
                             program.getNamespaceId(), program.getApplicationId(), spec.getName()))
      .withRunnable();

    Location programLocation = program.getJarLocation();
    String programName = programLocation.getName();
    TwillSpecification.Builder.RunnableSetter runnableSetter = null;
    for (Map.Entry<String, FlowletDefinition> entry  : spec.getFlowlets().entrySet()) {
      FlowletDefinition flowletDefinition = entry.getValue();
      FlowletSpecification flowletSpec = flowletDefinition.getFlowletSpec();
      ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
        .setVirtualCores(flowletSpec.getResources().getVirtualCores())
        .setMemory(flowletSpec.getResources().getMemoryMB(), ResourceSpecification.SizeUnit.MEGA)
        .setInstances(flowletDefinition.getInstances())
        .build();

      String flowletName = entry.getKey();
      runnableSetter = moreRunnable
        .add(flowletName,
             new FlowletTwillRunnable(flowletName, "hConf.xml", "cConf.xml"), resourceSpec)
        .withLocalFiles().add(programName, programLocation.toURI())
                         .add("hConf.xml", hConfig.toURI())
                         .add("cConf.xml", cConfig.toURI()).apply();
    }

    Preconditions.checkState(runnableSetter != null, "No flowlet for the flow.");
    return runnableSetter.anyOrder().withEventHandler(eventHandler).build();
  }
}

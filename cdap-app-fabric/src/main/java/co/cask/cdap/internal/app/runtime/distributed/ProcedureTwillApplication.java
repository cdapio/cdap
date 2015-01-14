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

import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.Location;

import java.io.File;

/**
 *
 */
public final class ProcedureTwillApplication implements TwillApplication {

  private final ProcedureSpecification spec;
  private final Program program;
  private final File hConfig;
  private final File cConfig;
  private final EventHandler eventHandler;

  public ProcedureTwillApplication(Program program, ProcedureSpecification spec,
                                   File hConfig, File cConfig, EventHandler eventHandler) {
    this.spec = spec;
    this.program = program;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
    this.eventHandler = eventHandler;
  }

  @Override
  public TwillSpecification configure() {
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(spec.getResources().getVirtualCores())
      .setMemory(spec.getResources().getMemoryMB(), ResourceSpecification.SizeUnit.MEGA)
      .setInstances(spec.getInstances())
      .build();

    Location programLocation = program.getJarLocation();

    return TwillSpecification.Builder.with()
      .setName(String.format("%s.%s.%s.%s",
                             ProgramType.PROCEDURE.name().toLowerCase(),
                             program.getNamespaceId(), program.getApplicationId(), spec.getName()))
      .withRunnable()
        .add(spec.getName(),
             new ProcedureTwillRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
             resourceSpec)
        .withLocalFiles()
          .add(programLocation.getName(), programLocation.toURI())
          .add("hConf.xml", hConfig.toURI())
          .add("cConf.xml", cConfig.toURI()).apply()
      .anyOrder().withEventHandler(eventHandler).build();
  }
}

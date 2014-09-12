/*
 * Copyright © 2014 Cask Data, Inc.
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
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.Location;

import java.io.File;


/**
 * Twill application wrapper for webapp.
 */
public final class WebappTwillApplication implements TwillApplication {

  private final Program program;
  private final File hConfig;
  private final File cConfig;
  private final EventHandler eventHandler;

  public WebappTwillApplication(Program program, File hConfig, File cConfig, EventHandler eventHandler) {
    this.program = program;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
    this.eventHandler = eventHandler;
  }

  @Override
  public TwillSpecification configure() {
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();

    Location programLocation = program.getJarLocation();

    try {
      String serviceName = WebappProgramRunner.getServiceName(ProgramType.WEBAPP, program);

      return TwillSpecification.Builder.with()
        .setName(serviceName)
        .withRunnable()
          .add(serviceName, new WebappTwillRunnable(serviceName, "hConf.xml", "cConf.xml"),
               resourceSpec)
          .withLocalFiles()
            .add(programLocation.getName(), programLocation.toURI())
            .add("hConf.xml", hConfig.toURI())
            .add("cConf.xml", cConfig.toURI()).apply()
        .anyOrder().withEventHandler(eventHandler).build();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}

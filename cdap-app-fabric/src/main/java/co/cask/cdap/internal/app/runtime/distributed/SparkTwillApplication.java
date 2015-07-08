/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;

import java.io.File;
import java.util.Map;

/**
 * A {@link TwillApplication} for running {@link Spark} program in distributed mode.
 */
public class SparkTwillApplication extends AbstractProgramTwillApplication {

  private final String name;

  public SparkTwillApplication(Program program, SparkSpecification spec,
                               Map<String, File> localizeFiles, EventHandler eventHandler) {
    super(program, localizeFiles, eventHandler);
    this.name = spec.getName();
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.SPARK;
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    // TODO (CDAP-2936): Make it configurable
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();

    runnables.put(name, new RunnableResource(
      new SparkTwillRunnable(name, "hConf.xml", "cConf.xml"),
      resourceSpec
    ));
  }
}

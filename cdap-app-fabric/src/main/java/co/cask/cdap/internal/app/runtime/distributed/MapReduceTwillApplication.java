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

import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;

import java.io.File;
import java.util.Map;

/**
 * The {@link TwillApplication} for running {@link MapReduce} in distributed mode.
 */
public final class MapReduceTwillApplication extends AbstractProgramTwillApplication {

  private final String name;

  public MapReduceTwillApplication(Program program, MapReduceSpecification spec,
                                   Map<String, File> localizeFiles, EventHandler eventHandler) {
    super(program, localizeFiles, eventHandler);
    this.name = spec.getName();
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.MAPREDUCE;
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    // These resources are for the container that runs the mapred client that will launch the actual mapred job.
    // It does not need much memory.  Memory for mappers and reduces are specified in the MapReduceSpecification,
    // which is configurable by the author of the job.
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();

    runnables.put(name, new RunnableResource(
      new MapReduceTwillRunnable(name, "hConf.xml", "cConf.xml"),
      resourceSpec
    ));
  }
}

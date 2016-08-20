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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;

import java.util.Map;

/**
 * The {@link TwillApplication} for running {@link MapReduce} in distributed mode.
 */
public final class MapReduceTwillApplication extends AbstractProgramTwillApplication {

  private final MapReduceSpecification spec;
  private final Resources resources;

  public MapReduceTwillApplication(Program program, Arguments runtimeArgs, MapReduceSpecification spec,
                                   Map<String, LocalizeResource> localizeResources,
                                   EventHandler eventHandler) {
    super(program, localizeResources, eventHandler);
    this.spec = spec;

    // These resources are for the container that runs the mapred client that will launch the actual mapred job.
    Map<String, String> clientArgs = RuntimeArguments.extractScope("task", "client", runtimeArgs.asMap());
    this.resources = SystemArguments.getResources(clientArgs, spec.getDriverResources());
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.MAPREDUCE;
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    runnables.put(spec.getName(),
                  new RunnableResource(new MapReduceTwillRunnable(spec.getName()), createResourceSpec(resources, 1)));
  }
}

/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.distributed;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.distributed.AbstractProgramTwillApplication;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;

import java.util.Map;

/**
 * A {@link TwillApplication} for running {@link Spark} program in distributed mode.
 */
final class SparkTwillApplication extends AbstractProgramTwillApplication {

  private final SparkSpecification spec;
  private final Resources resources;

  SparkTwillApplication(Program program, Arguments runtimeArgs, SparkSpecification spec,
                        Map<String, LocalizeResource> localizeResources,
                        EventHandler eventHandler) {
    super(program, localizeResources, eventHandler);
    this.spec = spec;

    Map<String, String> clientArgs = RuntimeArguments.extractScope("task", "client", runtimeArgs.asMap());
    this.resources = SystemArguments.getResources(clientArgs, spec.getClientResources());
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.SPARK;
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    runnables.put(spec.getName(),
                  new RunnableResource(new SparkTwillRunnable(spec.getName()), createResourceSpec(resources, 1)));
  }
}

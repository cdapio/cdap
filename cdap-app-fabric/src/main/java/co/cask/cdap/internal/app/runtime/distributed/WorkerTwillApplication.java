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

import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;

import java.io.File;
import java.util.Map;

/**
 * The {@link TwillApplication} for running {@link Worker} in distributed mode.
 */
public class WorkerTwillApplication extends AbstractProgramTwillApplication {

  private final WorkerSpecification spec;

  public WorkerTwillApplication(Program program, WorkerSpecification spec,
                                Map<String, File> localizeFiles,
                                EventHandler eventHandler) {
    super(program, localizeFiles, eventHandler);
    this.spec = spec;
  }

  @Override
  protected ProgramType getType() {
    return ProgramType.WORKER;
  }

  @Override
  protected void addRunnables(Map<String, RunnableResource> runnables) {
    runnables.put(spec.getName(), new RunnableResource(
      new WorkerTwillRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
      createResourceSpec(spec.getResources(), spec.getInstances())
    ));
  }
}

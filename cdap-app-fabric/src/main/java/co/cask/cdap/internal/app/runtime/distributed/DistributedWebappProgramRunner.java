/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.webapp.WebappProgramRunner;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * Distributed program runner for webapp.
 */
public final class DistributedWebappProgramRunner extends DistributedProgramRunner {

  @Inject
  DistributedWebappProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                 TokenSecureStoreRenewer tokenSecureStoreRenewer,
                                 Impersonator impersonator) {
    super(twillRunner, hConf, cConf, tokenSecureStoreRenewer, impersonator);
  }

  @Override
  public ProgramController createProgramController(TwillController twillController,
                                                   ProgramDescriptor programDescriptor, RunId runId) {
    return new WebappTwillProgramController(programDescriptor.getProgramId(), twillController, runId).startListen();
  }

  @Override
  protected Map<String, ProgramTwillApplication.RunnableResource> getRunnables(Program program,
                                                                               ProgramOptions programOptions) {
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.WEBAPP, "Only WEBAPP process type is supported.");

    Map<String, ProgramTwillApplication.RunnableResource> runnables = new HashMap<>();

    String serviceName = WebappProgramRunner.getServiceName(program.getId());
    Resources resources = SystemArguments.getResources(programOptions.getUserArguments().asMap(), null);

    runnables.put(serviceName, new ProgramTwillApplication.RunnableResource(new WebappTwillRunnable(serviceName),
                                                                            createResourceSpec(resources, 1)));
    return runnables;
  }
}

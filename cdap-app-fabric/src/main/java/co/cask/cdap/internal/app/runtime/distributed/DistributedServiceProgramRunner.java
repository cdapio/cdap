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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.AbortOnTimeoutEventHandler;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.security.TokenSecureStoreUpdater;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Distributed ProgramRunner for Service.
 */
public class DistributedServiceProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedServiceProgramRunner.class);

  @Inject
  DistributedServiceProgramRunner(TwillRunner twillRunner, LocationFactory locationFactory,
                                  YarnConfiguration hConf, CConfiguration cConf,
                                  TokenSecureStoreUpdater tokenSecureStoreUpdater) {
    super(twillRunner, locationFactory, hConf, cConf, tokenSecureStoreUpdater);
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options,
                                     Map<String, LocalizeResource> localizeResources,
                                     ApplicationLauncher launcher) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.SERVICE, "Only SERVICE process type is supported.");

    final ServiceSpecification serviceSpec = appSpec.getServices().get(program.getName());
    Preconditions.checkNotNull(serviceSpec, "Missing ServiceSpecification for %s", program.getName());

    // Launch service runnables program runners
    LOG.info("Launching distributed service: {}:{}", program.getName(), serviceSpec.getName());

    TwillController controller = launcher.launch(new ServiceTwillApplication(program, serviceSpec,
                                                                             localizeResources, eventHandler));

    DistributedServiceRunnableInstanceUpdater instanceUpdater = new DistributedServiceRunnableInstanceUpdater(
      program, controller);
    RunId runId = RunIds.fromString(options.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    return new ServiceTwillProgramController(program.getId(), controller, instanceUpdater, runId).startListen();
  }

  @Override
  protected EventHandler createEventHandler(CConfiguration cConf) {
    return new AbortOnTimeoutEventHandler(
      cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE), true);
  }
}

package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbortOnTimeoutEventHandler;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Distributed ProgramRunner for Service.
 */
public class DistributedServiceProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedServiceProgramRunner.class);

  @Inject
  DistributedServiceProgramRunner(TwillRunner twillRunner, Configuration hConfig, CConfiguration cConfig) {
    super(twillRunner, hConfig, cConfig);
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options, File hConfFile, File cConfFile,
                                     ApplicationLauncher launcher) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.SERVICE, "Only SERVICE process type is supported.");

    final ServiceSpecification serviceSpec = appSpec.getServices().get(program.getName());
    Preconditions.checkNotNull(serviceSpec, "Missing ServiceSpecification for %s", program.getName());

    // Launch service runnables program runners
    LOG.info("Launching distributed service: {}:{}", program.getName(), serviceSpec.getName());

    TwillController controller = launcher.launch(new ServiceTwillApplication(program, serviceSpec, hConfFile,
                                                                             cConfFile, eventHandler));

    DistributedServiceRunnableInstanceUpdater instanceUpdater = new DistributedServiceRunnableInstanceUpdater(
      program, controller);

    return new ServiceTwillProgramController(program.getName(), controller, instanceUpdater).startListen();
  }

  @Override
  protected EventHandler createEventHandler(CConfiguration cConf) {
    return new AbortOnTimeoutEventHandler(
      cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE), true);
  }
}

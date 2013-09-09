/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.weave.api.WeaveRunner;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public final class DistributedWorkflowProgramRunner extends AbstractDistributedProgramRunner {

  @Inject
  public DistributedWorkflowProgramRunner(WeaveRunner weaveRunner, Configuration hConf, CConfiguration cConf) {
    super(weaveRunner, hConf, cConf);
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}

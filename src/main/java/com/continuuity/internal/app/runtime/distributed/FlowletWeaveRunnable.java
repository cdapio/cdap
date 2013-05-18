/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.flow.FlowletProgramRunner;

/**
 *
 */
final class FlowletWeaveRunnable extends AbstractProgramWeaveRunnable<FlowletProgramRunner> {

  FlowletWeaveRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<FlowletProgramRunner> getProgramClass() {
    return FlowletProgramRunner.class;
  }
}

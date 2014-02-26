/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.flow.FlowletProgramRunner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 *
 */
final class FlowletTwillRunnable extends AbstractProgramTwillRunnable<FlowletProgramRunner> {

  private final boolean disableTransaction;

  FlowletTwillRunnable(String name, String hConfName, String cConfName, boolean disableTransaction) {
    super(name, hConfName, cConfName);
    this.disableTransaction = disableTransaction;
  }

  @Override
  protected Map<String, String> getConfigs() {
    return ImmutableMap.of(ProgramOptionConstants.DISABLE_TRANSACTION, Boolean.toString(disableTransaction));
  }

  @Override
  protected Class<FlowletProgramRunner> getProgramClass() {
    return FlowletProgramRunner.class;
  }
}

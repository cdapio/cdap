/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.workflow.WorkflowProgramRunner;

/**
 *
 */
final class WorkflowWeaveRunnable extends AbstractProgramWeaveRunnable<WorkflowProgramRunner> {

  WorkflowWeaveRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<WorkflowProgramRunner> getProgramClass() {
    return WorkflowProgramRunner.class;
  }
}

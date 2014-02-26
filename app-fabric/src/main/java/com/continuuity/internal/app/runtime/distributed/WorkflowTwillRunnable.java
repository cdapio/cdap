/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.workflow.WorkflowProgramRunner;
import org.apache.hadoop.mapred.YarnClientProtocolProvider;

/**
 *
 */
final class WorkflowTwillRunnable extends AbstractProgramTwillRunnable<WorkflowProgramRunner> {

  // NOTE: DO NOT REMOVE.  Though it is unused, the dependency is needed when submitting the mapred job.
  private YarnClientProtocolProvider provider;

  WorkflowTwillRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<WorkflowProgramRunner> getProgramClass() {
    return WorkflowProgramRunner.class;
  }
}

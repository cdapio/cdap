package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.program.Program;
import org.apache.twill.api.TwillController;

/**
 * For updating the number of service runnable instances.
 */
public class DistributedServiceRunnableInstanceUpdater {
  private final Program program;
  private final TwillController twillController;

  DistributedServiceRunnableInstanceUpdater(Program program, TwillController twillController) {
    this.program = program;
    this.twillController = twillController;
  }

  void update(String runnableId, int newInstanceCount, int oldInstanceCount) throws Exception {
    //TODO: Check if suspension of instances or wait till number of instances equal oldinstance count are required
    twillController.changeInstances(runnableId, newInstanceCount).get();
  }
}

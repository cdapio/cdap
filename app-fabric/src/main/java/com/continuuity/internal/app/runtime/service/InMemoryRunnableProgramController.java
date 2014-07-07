package com.continuuity.internal.app.runtime.service;

import com.continuuity.internal.app.runtime.AbstractProgramController;
import org.apache.twill.internal.BasicTwillContext;

/**
 * Program Controller for Service runnable
 */
public class InMemoryRunnableProgramController extends AbstractProgramController {
  private InMemoryRunnableDriver driver;

  public InMemoryRunnableProgramController(String serviceName, String runnableName, BasicTwillContext twillContext,
                                           InMemoryRunnableDriver driver) {
    super(serviceName + ":" + runnableName, twillContext.getRunId());
    this.driver = driver;
  }

  @Override
  protected void doSuspend() throws Exception {
    //no-op
  }

  @Override
  protected void doResume() throws Exception {
    //no-op
  }

  @Override
  protected void doStop() throws Exception {
    driver.stopAndWait();
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    //no-op
  }
}

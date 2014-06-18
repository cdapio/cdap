package com.continuuity.internal.app.runtime.service;

import com.continuuity.internal.app.runtime.AbstractProgramController;
import org.apache.twill.api.TwillContext;
import org.apache.twill.internal.BasicTwillContext;

/**
 * Program Controller for Service runnable
 */
public class InMemoryRunnableProgramController extends AbstractProgramController {

  private TwillContext context;
  private InMemoryRunnableDriver driver;

  public InMemoryRunnableProgramController(String name, String runnableName,
                                           BasicTwillContext twillContext, InMemoryRunnableDriver driver) {
    super(name + ":" + runnableName, twillContext.getRunId());
    this.context = context;
    this.driver = driver;
  }

  @Override
  protected void doSuspend() throws Exception {

  }

  @Override
  protected void doResume() throws Exception {

  }

  @Override
  protected void doStop() throws Exception {
    driver.stopAndWait();

  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {

  }
}

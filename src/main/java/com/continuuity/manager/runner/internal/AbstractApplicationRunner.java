package com.continuuity.manager.runner.internal;

import com.continuuity.manager.runner.ApplicationRunner;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

/**
 *
 */
public class AbstractApplicationRunner extends AbstractExecutionThreadService implements ApplicationRunner {

  @Override
  protected void startUp() throws Exception {
    super.startUp();    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  protected void shutDown() throws Exception {
    super.shutDown();    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      // Keep monitoring
    }
  }
}

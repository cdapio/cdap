/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.common.Threads;
import com.google.common.util.concurrent.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
final class WorkflowProgramController extends AbstractProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowProgramController.class);

  private final WorkflowDriver driver;

  WorkflowProgramController(String programName, WorkflowDriver driver, RunId runId) {
    super(programName, runId);
    this.driver = driver;
    startListen(driver);
  }

  @Override
  protected void doSuspend() throws Exception {
    LOG.info("Suspend not supported.");
  }

  @Override
  protected void doResume() throws Exception {
    LOG.info("Resume not supported.");
  }

  @Override
  protected void doStop() throws Exception {
    driver.stopAndWait();
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    LOG.info("Command ignored {}, {}", name, value);
  }
  
  private void startListen(Service service) {
    // Forward state changes from the given service to this controller.
    service.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        started();
      }

      @Override
      public void terminated(Service.State from) {
        stop();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        error(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }
}

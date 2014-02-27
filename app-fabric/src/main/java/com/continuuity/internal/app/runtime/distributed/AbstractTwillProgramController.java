/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.runtime.ProgramController;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.TwillController;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ProgramController} that control program through twill.
 */
abstract class AbstractTwillProgramController extends AbstractProgramController implements ProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTwillProgramController.class);

  protected final String programName;
  protected final TwillController twillController;

  protected AbstractTwillProgramController(String programName, TwillController twillController) {
    super(programName, twillController.getRunId());
    this.programName = programName;
    this.twillController = twillController;
  }

  /**
   * Starts listening to TwillController state changes. For internal use only.
   * The listener cannot be binded in constructor to avoid reference leak.
   *
   * @return this instance.
   */
  public ProgramController startListen() {
    twillController.addListener(createTwillListener(), Threads.SAME_THREAD_EXECUTOR);
    return this;
  }

  @Override
  protected final void doSuspend() throws Exception {
    twillController.sendCommand(ProgramCommands.SUSPEND).get();
  }

  @Override
  protected final void doResume() throws Exception {
    twillController.sendCommand(ProgramCommands.RESUME).get();
  }

  @Override
  protected final void doStop() throws Exception {
    twillController.stopAndWait();
  }

  private TwillController.Listener createTwillListener() {
    return new ServiceListenerAdapter() {

      @Override
      public void running() {
        LOG.info("Twill program running: {} {}", programName, twillController.getRunId());
        started();
      }

      @Override
      public void terminated(Service.State from) {
        LOG.info("Twill program terminated: {} {}", programName, twillController.getRunId());
        stop();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.info("Twill program failed: {} {}", programName, twillController.getRunId());
        error(failure);
      }
    };
  }
}

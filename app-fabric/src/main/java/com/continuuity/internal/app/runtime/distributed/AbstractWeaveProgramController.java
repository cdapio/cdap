/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.runtime.ProgramController;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.common.Threads;
import com.google.common.util.concurrent.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ProgramController} that control program through weave.
 */
abstract class AbstractWeaveProgramController extends AbstractProgramController implements ProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractWeaveProgramController.class);

  protected final String programName;
  protected final WeaveController weaveController;

  protected AbstractWeaveProgramController(String programName, WeaveController weaveController) {
    super(programName, weaveController.getRunId());
    this.programName = programName;
    this.weaveController = weaveController;
  }

  /**
   * Starts listening to WeaveController state changes. For internal use only.
   * The listener cannot be binded in constructor to avoid reference leak.
   *
   * @return this instance.
   */
  public ProgramController startListen() {
    weaveController.addListener(createWeaveListener(), Threads.SAME_THREAD_EXECUTOR);
    return this;
  }

  @Override
  protected final void doSuspend() throws Exception {
    weaveController.sendCommand(ProgramCommands.SUSPEND).get();
  }

  @Override
  protected final void doResume() throws Exception {
    weaveController.sendCommand(ProgramCommands.RESUME).get();
  }

  @Override
  protected final void doStop() throws Exception {
    weaveController.stopAndWait();
  }

  private WeaveController.Listener createWeaveListener() {
    return new ServiceListenerAdapter() {

      @Override
      public void running() {
        LOG.info("Weave program running: {} {}", programName, weaveController.getRunId());
        started();
      }

      @Override
      public void terminated(Service.State from) {
        LOG.info("Weave program terminated: {} {}", programName, weaveController.getRunId());
        stop();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.info("Weave program failed: {} {}", programName, weaveController.getRunId());
        error(failure);
      }
    };
  }
}

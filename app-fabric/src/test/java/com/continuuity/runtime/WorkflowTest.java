/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.runtime;

import com.continuuity.WorkflowApp;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.common.Threads;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class WorkflowTest {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowTest.class);

  @Test
  public void testWorkflow() throws Exception {
    final ApplicationWithPrograms app = TestHelper.deployApplicationWithManager(WorkflowApp.class);
    ProgramRunnerFactory runnerFactory = TestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    ProgramRunner programRunner = runnerFactory.create(ProgramRunnerFactory.Type.WORKFLOW);

    Program program = app.getPrograms().iterator().next();

    final CountDownLatch latch = new CountDownLatch(1);
    programRunner.run(program, new SimpleProgramOptions(program)).addListener(new AbstractListener() {
      @Override
      public void stopped() {
        LOG.info("Stopped");
        latch.countDown();
      }

      @Override
      public void error(Throwable cause) {
        LOG.info("Error", cause);
        latch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    latch.await();
  }
}

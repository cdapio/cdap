/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.runtime;

import com.continuuity.WorkflowApp;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.common.Threads;
import com.google.common.collect.ImmutableMap;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class WorkflowTest {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testWorkflow() throws Exception {
    final ApplicationWithPrograms app = TestHelper.deployApplicationWithManager(WorkflowApp.class);
    ProgramRunnerFactory runnerFactory = TestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    ProgramRunner programRunner = runnerFactory.create(ProgramRunnerFactory.Type.WORKFLOW);

    Program program = app.getPrograms().iterator().next();

    final CountDownLatch latch = new CountDownLatch(1);
    String inputPath = createInput();
    String outputPath = new File(tmpFolder.newFolder(), "output").getAbsolutePath();
    BasicArguments userArgs = new BasicArguments(ImmutableMap.of("inputPath", inputPath, "outputPath", outputPath));
    ProgramOptions options = new SimpleProgramOptions(program.getName(), new BasicArguments(), userArgs);

    programRunner.run(program, options).addListener(new AbstractListener() {
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

  private String createInput() throws IOException {
    File inputDir = tmpFolder.newFolder();

    File inputFile = new File(inputDir.getPath() + "/words.txt");
    inputFile.deleteOnExit();
    BufferedWriter writer = new BufferedWriter(new FileWriter(inputFile));
    try {
      writer.write("this text has");
      writer.newLine();
      writer.write("two words text inside");
    } finally {
      writer.close();
    }

    return inputDir.getAbsolutePath();
  }
}

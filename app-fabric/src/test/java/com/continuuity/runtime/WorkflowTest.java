/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.runtime;

import com.continuuity.OneActionWorkflowApp;
import com.continuuity.WorkflowApp;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.test.XSlowTests;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.common.Threads;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 *
 */
@Category(XSlowTests.class)
public class WorkflowTest {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Supplier<File> TEMP_FOLDER_SUPPLIER = new Supplier<File>() {

    @Override
    public File get() {
      try {
        return tmpFolder.newFolder();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  };


  @Test(timeout = 120 * 1000L)
  public void testWorkflow() throws Exception {
    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(WorkflowApp.class,
                                                                                         TEMP_FOLDER_SUPPLIER);
    ProgramRunnerFactory runnerFactory = AppFabricTestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    ProgramRunner programRunner = runnerFactory.create(ProgramRunnerFactory.Type.WORKFLOW);

    Program program = Iterators.filter(app.getPrograms().iterator(), new Predicate<Program>() {
      @Override
      public boolean apply(Program input) {
        return input.getType() == Type.WORKFLOW;
      }
    }).next();

    String inputPath = createInput();
    String outputPath = new File(tmpFolder.newFolder(), "output").getAbsolutePath();
    BasicArguments userArgs = new BasicArguments(ImmutableMap.of("inputPath", inputPath, "outputPath", outputPath));
    ProgramOptions options = new SimpleProgramOptions(program.getName(), new BasicArguments(), userArgs);

    final SettableFuture<String> completion = SettableFuture.create();
    programRunner.run(program, options).addListener(new AbstractListener() {
      @Override
      public void stopped() {
        LOG.info("Stopped");
        completion.set("Completed");
      }

      @Override
      public void error(Throwable cause) {
        LOG.info("Error", cause);
        completion.setException(cause);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    completion.get();
  }

  @Test(timeout = 120 * 1000L)
  public void testOneActionWorkflow() throws Exception {
    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(OneActionWorkflowApp.class,
                                                                                         TEMP_FOLDER_SUPPLIER);
    ProgramRunnerFactory runnerFactory = AppFabricTestHelper.getInjector().getInstance(ProgramRunnerFactory.class);
    ProgramRunner programRunner = runnerFactory.create(ProgramRunnerFactory.Type.WORKFLOW);

    Program program = Iterators.filter(app.getPrograms().iterator(), new Predicate<Program>() {
      @Override
      public boolean apply(Program input) {
        return input.getType() == Type.WORKFLOW;
      }
    }).next();

    ProgramOptions options = new SimpleProgramOptions(program.getName(), new BasicArguments(), new BasicArguments());

    final SettableFuture<String> completion = SettableFuture.create();
    programRunner.run(program, options).addListener(new AbstractListener() {
      @Override
      public void stopped() {
        LOG.info("Stopped");
        completion.set("Completed");
      }

      @Override
      public void error(Throwable cause) {
        LOG.info("Error", cause);
        completion.setException(cause);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    String run = completion.get();
    Assert.assertEquals("Completed", run);
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

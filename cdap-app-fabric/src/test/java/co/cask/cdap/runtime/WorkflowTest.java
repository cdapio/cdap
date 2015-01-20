/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.runtime;

import co.cask.cdap.MissingMapReduceWorkflowApp;
import co.cask.cdap.MissingSparkWorkflowApp;
import co.cask.cdap.OneActionWorkflowApp;
import co.cask.cdap.WorkflowApp;
import co.cask.cdap.WorkflowSchedulesWithSameNameApp;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.internal.AppFabricTestHelper;
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
        return input.getType() == ProgramType.WORKFLOW;
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
  public void testBadInputInWorkflow() throws Exception {
    // try deploying app containing Workflow configured with non-existent MapReduce program
    try {
      final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(
        MissingMapReduceWorkflowApp.class,
        TEMP_FOLDER_SUPPLIER);
      Assert.fail("Should have thrown Exception because MapReduce program is missing in the Application.");
    } catch (Exception ex) {
      Assert.assertEquals(ex.getCause().getMessage(),
                          "MapReduce program 'SomeMapReduceProgram' is not configured with the Application.");
    }

    // try deploying app containing Workflow configured with non-existent Spark program
    try {
      final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(
        MissingSparkWorkflowApp.class,
        TEMP_FOLDER_SUPPLIER);
      Assert.fail("Should have thrown Exception because Spark program is missing in the Application.");
    } catch (Exception ex) {
      Assert.assertEquals(ex.getCause().getMessage(),
                          "Spark program 'SomeSparkProgram' is not configured with the Application.");
    }

    // try deploying app containing Workflow configured with multiple schedules with the same name
    try {
      final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(
        WorkflowSchedulesWithSameNameApp.class,
        TEMP_FOLDER_SUPPLIER);
      Assert.fail("Should have thrown Exception because Workflow is configured with schedules having same name.");
    } catch (Exception ex) {
      Assert.assertEquals(ex.getCause().getCause().getMessage(),
                          "Schedule with the name 'DailySchedule' already exists.");
    }
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
        return input.getType() == ProgramType.WORKFLOW;
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

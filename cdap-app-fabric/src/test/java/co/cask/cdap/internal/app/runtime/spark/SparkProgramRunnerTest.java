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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.DatasetInstantiator;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import co.cask.cdap.test.internal.DefaultId;
import co.cask.cdap.test.internal.TempFolder;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TxConstants;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import org.apache.twill.common.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(XSlowTests.class)
public class SparkProgramRunnerTest {

  private static final TempFolder TEMP_FOLDER = new TempFolder();

  private static Injector injector;
  private static TransactionExecutorFactory txExecutorFactory;

  private static TransactionManager txService;
  private static DatasetFramework dsFramework;
  private static DatasetInstantiator datasetInstantiator;

  final String testString1 = "persisted data";
  final String testString2 = "distributed systems";

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

  @BeforeClass
  public static void beforeClass() {
    // we are only gonna do long-running transactions here. Set the tx timeout to a ridiculously low value.
    // that will test that the long-running transactions actually bypass that timeout.
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("data").getAbsolutePath());
    conf.setInt(TxConstants.Manager.CFG_TX_TIMEOUT, 1);
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 2);
    injector = AppFabricTestHelper.getInjector(conf);
    txService = injector.getInstance(TransactionManager.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    dsFramework = new NamespacedDatasetFramework(injector.getInstance(DatasetFramework.class),
                                                 new DefaultDatasetNamespace(conf));

    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    datasetInstantiator =
      new DatasetInstantiator(DefaultId.NAMESPACE, datasetFramework, injector.getInstance(CConfiguration.class),
                              SparkProgramRunnerTest.class.getClassLoader(), null);

    txService.startAndWait();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    txService.stopAndWait();
  }

  @After
  public void after() throws Exception {
    // cleanup user data (only user datasets)
    for (DatasetSpecification spec : dsFramework.getInstances(DefaultId.NAMESPACE)) {
      dsFramework.deleteInstance(Id.DatasetInstance.from(DefaultId.NAMESPACE, spec.getName()));
    }
  }

  @Test
  public void testSparkWithObjectStore() throws Exception {
    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(SparkAppUsingObjectStore.class, TEMP_FOLDER_SUPPLIER);

    prepareInputData();
    runProgram(app, SparkAppUsingObjectStore.CharCountSpecification.class);
    checkOutputData();
  }

  @Test
  public void testScalaSparkWithObjectStore() throws Exception {
    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(ScalaSparkAppUsingObjectStore.class, TEMP_FOLDER_SUPPLIER);

    prepareInputData();
    runProgram(app, ScalaSparkAppUsingObjectStore.CharCountSpecification.class);
    checkOutputData();
  }

  private void prepareInputData() throws TransactionFailureException, InterruptedException {
    final ObjectStore<String> input = datasetInstantiator.getDataset("keys");

    //Populate some input
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          input.write(Bytes.toBytes(testString1), testString1);
          input.write(Bytes.toBytes(testString2), testString2);
        }
      });
  }

  private void checkOutputData() throws TransactionFailureException, InterruptedException {
    final KeyValueTable output = datasetInstantiator.getDataset("count");
    //read output and verify result
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          byte[] val = output.read(Bytes.toBytes(testString1));
          Assert.assertTrue(val != null);
          Assert.assertEquals(Bytes.toInt(val), testString1.length());

          val = output.read(Bytes.toBytes(testString2));
          Assert.assertTrue(val != null);
          Assert.assertEquals(Bytes.toInt(val), testString2.length());

        }
      });
  }

  private void runProgram(ApplicationWithPrograms app, Class<?> programClass) throws Exception {
    waitForCompletion(submit(app, programClass));
  }

  private void waitForCompletion(ProgramController controller) throws InterruptedException {
    final CountDownLatch completion = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState) {
        if (currentState == ProgramController.State.COMPLETED || currentState == ProgramController.State.ERROR) {
          completion.countDown();
        }
      }

      @Override
      public void completed() {
        completion.countDown();
      }

      @Override
      public void error(Throwable cause) {
        completion.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    completion.await(10, TimeUnit.MINUTES);
  }

  private ProgramController submit(ApplicationWithPrograms app, Class<?> programClass) throws ClassNotFoundException {
    ProgramRunnerFactory runnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    Program program = getProgram(app, programClass);
    ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));

    HashMap<String, String> userArgs = Maps.newHashMap();
    return runner.run(program, new SimpleProgramOptions(program.getName(), new BasicArguments(),
                                                        new BasicArguments(userArgs)));
  }

  private Program getProgram(ApplicationWithPrograms app, Class<?> programClass) throws ClassNotFoundException {
    for (Program p : app.getPrograms()) {
      if (programClass.getCanonicalName().equals(p.getMainClass().getCanonicalName())) {
        return p;
      }
    }
    return null;
  }
}

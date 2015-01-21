/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data.dataset.DatasetInstantiator;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
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

import java.io.IOException;
import java.text.DateFormat;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.internal.app.runtime.batch.AppWithTimePartitionedFileSet.PARTITIONED;

@Category(XSlowTests.class)
/**
 * This tests that we can read and write time-partitioned file sets with map/reduce, using the partition
 * time to specify input and output partitions. It does not test that the dataset is queryable with Hive,
 * or that its partitions are registered correctly in the Hive meta store. That is because here in the
 * app-fabric tests, explore is disabled.
 */
public class MapReduceWithPartitionedTest {
  private static Injector injector;
  private static TransactionExecutorFactory txExecutorFactory;

  private static TransactionManager txService;
  private static DatasetFramework dsFramework;
  private static DatasetInstantiator datasetInstantiator;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Supplier<java.io.File> TEMP_FOLDER_SUPPLIER = new Supplier<java.io.File>() {
    @Override
    public java.io.File get() {
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
    conf.setInt(TxConstants.Manager.CFG_TX_TIMEOUT, 1);
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 2);
    injector = AppFabricTestHelper.getInjector(conf);
    txService = injector.getInstance(TransactionManager.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    dsFramework = new NamespacedDatasetFramework(injector.getInstance(DatasetFramework.class),
                                                 new DefaultDatasetNamespace(conf, Namespace.USER));

    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    datasetInstantiator =
      new DatasetInstantiator(datasetFramework, injector.getInstance(CConfiguration.class),
                              MapReduceWithPartitionedTest.class.getClassLoader(), null);

    txService.startAndWait();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    txService.stopAndWait();
  }

  @After
  public void after() throws Exception {
    // cleanup user data (only user datasets)
    for (DatasetSpecification spec : dsFramework.getInstances()) {
      dsFramework.deleteInstance(spec.getName());
    }
  }

  @Test
  public void testTimePartitionedWithMR() throws Exception {

    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(AppWithTimePartitionedFileSet.class, TEMP_FOLDER_SUPPLIER);

    // write a value to the input table
    final Table table = datasetInstantiator.getDataset(AppWithTimePartitionedFileSet.INPUT);
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          table.put(Bytes.toBytes("x"), AppWithTimePartitionedFileSet.ONLY_COLUMN, Bytes.toBytes("1"));
        }
      });

    final long time = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT)
      .parse("1/15/15 11:15 am").getTime();
    final long time5 = time + TimeUnit.MINUTES.toMillis(5);

    // run the partition writer m/r with this output partition time
    Map<String, String> runtimeArguments = Maps.newHashMap();
    Map<String, String> outputArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, time);
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, PARTITIONED, outputArgs));
    runProgram(app, AppWithTimePartitionedFileSet.PartitionWriter.class, new BasicArguments(runtimeArguments));

    // this should have created a partition in the tpfs
    final TimePartitionedFileSet tpfs = datasetInstantiator.getDataset(PARTITIONED);
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          String path = tpfs.getPartition(time);
          Assert.assertNotNull(path);
          Assert.assertTrue(path.contains("2015-01-15/11-15"));
          System.err.println("Path for partition: " + path);
        }
      });

    // delete the data in the input table and write a new row
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          table.delete(Bytes.toBytes("x"));
          table.put(Bytes.toBytes("y"), AppWithTimePartitionedFileSet.ONLY_COLUMN, Bytes.toBytes("2"));
        }
      });

    // now run the m/r again with a new partition time, say 5 minutes later
    TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, time5);
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, PARTITIONED, outputArgs));
    runProgram(app, AppWithTimePartitionedFileSet.PartitionWriter.class, new BasicArguments(runtimeArguments));

    // this should have created a partition in the tpfs
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          String path = tpfs.getPartition(time5);
          Assert.assertNotNull(path);
          Assert.assertTrue(path.contains("2015-01-15/11-20"));
          System.err.println("Path for partition 5: " + path);
        }
      });

    // now run a map/reduce that reads all the partitions
    runtimeArguments = Maps.newHashMap();
    Map<String, String> inputArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setInputStartTime(inputArgs, time - TimeUnit.MINUTES.toMillis(5));
    TimePartitionedFileSetArguments.setInputEndTime(inputArgs, time5 + TimeUnit.MINUTES.toMillis(5));
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, PARTITIONED, inputArgs));
    runtimeArguments.put(AppWithTimePartitionedFileSet.ROW_TO_WRITE, "a");
    runProgram(app, AppWithTimePartitionedFileSet.PartitionReader.class, new BasicArguments(runtimeArguments));

    // this should have read both partitions - and written both x and y to row a
    final Table output = datasetInstantiator.getDataset(AppWithTimePartitionedFileSet.OUTPUT);
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Row row = output.get(Bytes.toBytes("a"));
          Assert.assertEquals("1", row.getString("x"));
          Assert.assertEquals("2", row.getString("y"));
        }
      });

    // now run a map/reduce that reads a range of the partitions, namely the first one
    TimePartitionedFileSetArguments.setInputStartTime(inputArgs, time - TimeUnit.MINUTES.toMillis(5));
    TimePartitionedFileSetArguments.setInputEndTime(inputArgs, time + TimeUnit.MINUTES.toMillis(2));
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, PARTITIONED, inputArgs));
    runtimeArguments.put(AppWithTimePartitionedFileSet.ROW_TO_WRITE, "b");
    runProgram(app, AppWithTimePartitionedFileSet.PartitionReader.class, new BasicArguments(runtimeArguments));

    // this should have read the first partition only - and written only x to row b
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Row row = output.get(Bytes.toBytes("b"));
          Assert.assertEquals("1", row.getString("x"));
          Assert.assertNull(row.get("y"));
        }
      });
  }

  private void runProgram(ApplicationWithPrograms app, Class<?> programClass, Arguments args)
    throws Exception {
    waitForCompletion(submit(app, programClass, args));
  }

  private void waitForCompletion(ProgramController controller) throws InterruptedException {
    final CountDownLatch completion = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState) {
        if (currentState == ProgramController.State.STOPPED || currentState == ProgramController.State.ERROR) {
          completion.countDown();
        }
      }

      @Override
      public void stopped() {
        completion.countDown();
      }

      @Override
      public void error(Throwable cause) {
        completion.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // MR tests can run for long time.
    completion.await(5, TimeUnit.MINUTES);
  }

  private ProgramController submit(ApplicationWithPrograms app, Class<?> programClass, Arguments userArgs)
    throws ClassNotFoundException {
    ProgramRunnerFactory runnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    final Program program = getProgram(app, programClass);
    ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));

    return runner.run(program, new SimpleProgramOptions(program.getName(),
                                                        new BasicArguments(),
                                                        userArgs));
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

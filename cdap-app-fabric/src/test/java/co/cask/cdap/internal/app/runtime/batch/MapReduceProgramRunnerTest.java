/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data.dataset.DataSetInstantiator;
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
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TxConstants;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(XSlowTests.class)
public class MapReduceProgramRunnerTest {
  private static Injector injector;
  private static TransactionExecutorFactory txExecutorFactory;

  private static TransactionManager txService;
  private static DatasetFramework dsFramework;
  private static DataSetInstantiator dataSetInstantiator;

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
    conf.setInt(TxConstants.Manager.CFG_TX_TIMEOUT, 1);
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 2);
    injector = AppFabricTestHelper.getInjector(conf);
    txService = injector.getInstance(TransactionManager.class);
    txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);
    dsFramework = new NamespacedDatasetFramework(injector.getInstance(DatasetFramework.class),
                                                 new DefaultDatasetNamespace(conf, Namespace.USER));

    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    dataSetInstantiator =
      new DataSetInstantiator(datasetFramework, injector.getInstance(CConfiguration.class),
                              MapReduceProgramRunnerTest.class.getClassLoader(), null, null);

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
  public void testMapreduceWithObjectStore() throws Exception {
    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(AppWithMapReduceUsingObjectStore.class, TEMP_FOLDER_SUPPLIER);

    final ObjectStore<String> input = dataSetInstantiator.getDataSet("keys");

    final String testString = "persisted data";

    //Populate some input
    txExecutorFactory.createExecutor(dataSetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          input.write(Bytes.toBytes(testString), testString);
          input.write(Bytes.toBytes("distributed systems"), "distributed systems");
        }
      });

    runProgram(app, AppWithMapReduceUsingObjectStore.ComputeCounts.class, false);

    final KeyValueTable output = dataSetInstantiator.getDataSet("count");
    //read output and verify result
    txExecutorFactory.createExecutor(dataSetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          byte[] val = output.read(Bytes.toBytes(testString));
          Assert.assertTrue(val != null);
          Assert.assertEquals(Bytes.toString(val), Integer.toString(testString.length()));

          val = output.read(Bytes.toBytes("distributed systems"));
          Assert.assertTrue(val != null);
          Assert.assertEquals(Bytes.toString(val), "19");

        }
      });
  }

  @Test
  public void testWordCount() throws Exception {

    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(AppWithMapReduce.class,
                                                                                         TEMP_FOLDER_SUPPLIER);
    final String inputPath = createInput();
    final File outputDir = new File(tmpFolder.newFolder(), "output");

    final KeyValueTable jobConfigTable = dataSetInstantiator.getDataSet("jobConfig");

    // write config into dataset
    txExecutorFactory.createExecutor(dataSetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          jobConfigTable.write(Bytes.toBytes("inputPath"), Bytes.toBytes(inputPath));
          jobConfigTable.write(Bytes.toBytes("outputPath"), Bytes.toBytes(outputDir.getPath()));
        }
      });

    runProgram(app, AppWithMapReduce.ClassicWordCount.class, false);

    File[] outputFiles = outputDir.listFiles();
    Assert.assertNotNull("no output files found", outputFiles);
    Assert.assertTrue("no output files found", outputFiles.length > 0);
    File outputFile = outputFiles[0];
    int lines = Files.readLines(outputFile, Charsets.UTF_8).size();
    // dummy check that output file is not empty
    Assert.assertTrue(lines > 0);
  }

  @Test
  public void testJobSuccess() throws Exception {
    testSuccess(false);
  }

  @Test
  public void testJobSuccessWithFrequentFlushing() throws Exception {
    // simplest test for periodic flushing
    // NOTE: we will change auto-flush to take into account size of buffered data, so no need to do/test a lot with
    //       current approach
    testSuccess(true);
  }

  private void testSuccess(boolean frequentFlushing) throws Exception {
    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(AppWithMapReduce.class,
                                                                                         TEMP_FOLDER_SUPPLIER);

    // we need to do a "get" on all datasets we use so that they are in dataSetInstantiator.getTransactionAware()
    final TimeseriesTable table = (TimeseriesTable) dataSetInstantiator.getDataSet("timeSeries");
    final KeyValueTable beforeSubmitTable = dataSetInstantiator.getDataSet("beforeSubmit");
    final KeyValueTable onFinishTable = dataSetInstantiator.getDataSet("onFinish");
    final Table counters = dataSetInstantiator.getDataSet("counters");
    final Table countersFromContext = dataSetInstantiator.getDataSet("countersFromContext");

    // 1) fill test data
    fillTestInputData(txExecutorFactory, dataSetInstantiator, table, false);

    // 2) run job
    final long start = System.currentTimeMillis();
    runProgram(app, AppWithMapReduce.AggregateTimeseriesByTag.class, frequentFlushing);
    final long stop = System.currentTimeMillis();

    // 3) verify results
    txExecutorFactory.createExecutor(dataSetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Map<String, Long> expected = Maps.newHashMap();
          // note: not all records add to the sum since filter by tag="tag1" and ts={1..3} is used
          expected.put("tag1", 18L);
          expected.put("tag2", 3L);
          expected.put("tag3", 18L);

          Iterator<TimeseriesTable.Entry> agg = table.read(AggregateMetricsByTag.BY_TAGS, start, stop);
          int count = 0;
          while (agg.hasNext()) {
            TimeseriesTable.Entry entry = agg.next();
            String tag = Bytes.toString(entry.getTags()[0]);
            Assert.assertEquals((long) expected.get(tag), Bytes.toLong(entry.getValue()));
            count++;
          }
          Assert.assertEquals(expected.size(), count);

          Assert.assertArrayEquals(Bytes.toBytes("beforeSubmit:done"),
                                   beforeSubmitTable.read(Bytes.toBytes("beforeSubmit")));
          Assert.assertArrayEquals(Bytes.toBytes("onFinish:done"),
                                   onFinishTable.read(Bytes.toBytes("onFinish")));

          Assert.assertTrue(counters.get(new Get("mapper")).getLong("records", 0) > 0);
          Assert.assertTrue(counters.get(new Get("reducer")).getLong("records", 0) > 0);
          Assert.assertTrue(countersFromContext.get(new Get("mapper")).getLong("records", 0) > 0);
          Assert.assertTrue(countersFromContext.get(new Get("reducer")).getLong("records", 0) > 0);
        }
      });
  }

  @Test
  public void testJobFailure() throws Exception {
    testFailure(false);
  }

  @Test
  public void testJobFailureWithFrequentFlushing() throws Exception {
    testFailure(true);
  }

  // TODO: this tests failure in Map tasks. We also need to test: failure in Reduce task, kill of a job by user.
  private void testFailure(boolean frequentFlushing) throws Exception {
    // We want to verify that when mapreduce job fails:
    // * things written in beforeSubmit() remains and visible to others
    // * things written in tasks not visible to others TODO AAA: do invalidate
    // * things written in onfinish() remains and visible to others

    // NOTE: the code of this test is similar to testTimeSeriesRecordsCount() test. We put some "bad data" intentionally
    //       here to be recognized by map tasks as a message to emulate failure

    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(AppWithMapReduce.class,
                                                                                         TEMP_FOLDER_SUPPLIER);

    // we need to do a "get" on all datasets we use so that they are in dataSetInstantiator.getTransactionAware()
    final TimeseriesTable table = (TimeseriesTable) dataSetInstantiator.getDataSet("timeSeries");
    final KeyValueTable beforeSubmitTable = dataSetInstantiator.getDataSet("beforeSubmit");
    final KeyValueTable onFinishTable = dataSetInstantiator.getDataSet("onFinish");
    final Table counters = dataSetInstantiator.getDataSet("counters");
    final Table countersFromContext = dataSetInstantiator.getDataSet("countersFromContext");

    // 1) fill test data
    fillTestInputData(txExecutorFactory, dataSetInstantiator, table, true);

    // 2) run job
    final long start = System.currentTimeMillis();
    runProgram(app, AppWithMapReduce.AggregateTimeseriesByTag.class, frequentFlushing);
    final long stop = System.currentTimeMillis();

    // 3) verify results
    txExecutorFactory.createExecutor(dataSetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          // data should be rolled back todo: test that partially written is rolled back too
          Assert.assertFalse(table.read(AggregateMetricsByTag.BY_TAGS, start, stop).hasNext());

          // but written beforeSubmit and onFinish is available to others
          Assert.assertArrayEquals(Bytes.toBytes("beforeSubmit:done"),
                                   beforeSubmitTable.read(Bytes.toBytes("beforeSubmit")));
          Assert.assertArrayEquals(Bytes.toBytes("onFinish:done"),
                                   onFinishTable.read(Bytes.toBytes("onFinish")));
          Assert.assertEquals(0, counters.get(new Get("mapper")).getLong("records", 0));
          Assert.assertEquals(0, counters.get(new Get("reducer")).getLong("records", 0));
          Assert.assertEquals(0, countersFromContext.get(new Get("mapper")).getLong("records", 0));
          Assert.assertEquals(0, countersFromContext.get(new Get("reducer")).getLong("records", 0));
        }
    });
  }

  private void fillTestInputData(TransactionExecutorFactory txExecutorFactory,
                                 DataSetInstantiator dataSetInstantiator,
                                 final TimeseriesTable table,
                                 final boolean withBadData) throws TransactionFailureException, InterruptedException {
    TransactionExecutor executor = txExecutorFactory.createExecutor(dataSetInstantiator.getTransactionAware());
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() {
        fillTestInputData(table, withBadData);
      }
    });
  }

  private void fillTestInputData(TimeseriesTable table, boolean withBadData) {
    byte[] metric1 = Bytes.toBytes("metric");
    byte[] metric2 = Bytes.toBytes("metric2");
    byte[] tag1 = Bytes.toBytes("tag1");
    byte[] tag2 = Bytes.toBytes("tag2");
    byte[] tag3 = Bytes.toBytes("tag3");
    // m1e1 = metric: 1, entity: 1
    table.write(new TimeseriesTable.Entry(metric1, Bytes.toBytes(3L), 1, tag3, tag2, tag1));
    table.write(new TimeseriesTable.Entry(metric1, Bytes.toBytes(10L), 2, tag2, tag3));
    // 55L will make job fail
    table.write(new TimeseriesTable.Entry(metric1, Bytes.toBytes(withBadData ? 55L : 15L), 3, tag1, tag3));
    table.write(new TimeseriesTable.Entry(metric1, Bytes.toBytes(23L), 4, tag2));


    table.write(new TimeseriesTable.Entry(metric2, Bytes.toBytes(4L), 3, tag1, tag3));
  }

  private void runProgram(ApplicationWithPrograms app, Class<?> programClass, boolean frequentFlushing)
    throws Exception {
    waitForCompletion(submit(app, programClass, frequentFlushing));
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

  private ProgramController submit(ApplicationWithPrograms app, Class<?> programClass, boolean frequentFlushing)
    throws ClassNotFoundException {
    ProgramRunnerFactory runnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    final Program program = getProgram(app, programClass);
    ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));

    HashMap<String, String> userArgs = Maps.newHashMap();
    userArgs.put("metric", "metric");
    userArgs.put("startTs", "1");
    userArgs.put("stopTs", "3");
    userArgs.put("tag", "tag1");
    if (frequentFlushing) {
      userArgs.put("frequentFlushing", "true");
    }
    return runner.run(program, new SimpleProgramOptions(program.getName(),
                                                        new BasicArguments(),
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

    return inputDir.getPath();
  }

}

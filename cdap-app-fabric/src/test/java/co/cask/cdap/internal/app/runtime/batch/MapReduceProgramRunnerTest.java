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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data.dataset.DatasetInstantiator;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import co.cask.cdap.test.internal.DefaultId;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TxConstants;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.inject.Injector;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
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
                                                 new DefaultDatasetNamespace(conf));

    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    datasetInstantiator =
      new DatasetInstantiator(DefaultId.NAMESPACE, datasetFramework, injector.getInstance(CConfiguration.class),
                              MapReduceProgramRunnerTest.class.getClassLoader(), null);

    txService.startAndWait();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    txService.stopAndWait();
  }

  @After
  public void after() throws Exception {
    // cleanup user data (only user datasets)
    for (DatasetSpecificationSummary spec : dsFramework.getInstances(DefaultId.NAMESPACE)) {
      dsFramework.deleteInstance(Id.DatasetInstance.from(DefaultId.NAMESPACE, spec.getName()));
    }
  }

  @Test
  public void testMapreduceWithFileSet() throws Exception {
    // test reading and writing distinct datasets, reading more than one path
    // hack to use different datasets at each invocation of this test
    System.setProperty("INPUT_DATASET_NAME", "numbers");
    System.setProperty("OUTPUT_DATASET_NAME", "sums");

    Map<String, String> runtimeArguments = Maps.newHashMap();
    Map<String, String> inputArgs = Maps.newHashMap();
    FileSetArguments.setInputPaths(inputArgs, "abc, xyz");
    Map<String, String> outputArgs = Maps.newHashMap();
    FileSetArguments.setOutputPath(outputArgs, "a001");
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, "numbers", inputArgs));
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, "sums", outputArgs));
    testMapreduceWithFile("numbers", "abc, xyz", "sums", "a001",
                          AppWithMapReduceUsingFileSet.class,
                          AppWithMapReduceUsingFileSet.ComputeSum.class, new BasicArguments(runtimeArguments));

    // test reading and writing same dataset
    // hack to use different datasets at each invocation of this test
    System.setProperty("INPUT_DATASET_NAME", "boogie");
    System.setProperty("OUTPUT_DATASET_NAME", "boogie");
    runtimeArguments = Maps.newHashMap();
    inputArgs = Maps.newHashMap();
    FileSetArguments.setInputPaths(inputArgs, "zzz");
    outputArgs = Maps.newHashMap();
    FileSetArguments.setOutputPath(outputArgs, "f123");
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, "boogie", inputArgs));
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, "boogie", outputArgs));
    testMapreduceWithFile("boogie", "zzz", "boogie", "f123",
                          AppWithMapReduceUsingFileSet.class,
                          AppWithMapReduceUsingFileSet.ComputeSum.class, new BasicArguments(runtimeArguments));
  }

  @Test
  public void testMapreduceWithDynamicFileSet() throws Exception {
    Id.DatasetInstance rtInput1 = Id.DatasetInstance.from(DefaultId.NAMESPACE, "rtInput1");
    Id.DatasetInstance rtInput2 = Id.DatasetInstance.from(DefaultId.NAMESPACE, "rtInput2");
    Id.DatasetInstance rtOutput1 = Id.DatasetInstance.from(DefaultId.NAMESPACE, "rtOutput1");
    // create the datasets here because they are not created by the app
    dsFramework.addInstance("fileSet", rtInput1, FileSetProperties.builder()
      .setBasePath("/rtInput1")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
      .build());
    dsFramework.addInstance("fileSet", rtOutput1, FileSetProperties.builder()
      .setBasePath("/rtOutput1")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
      .build());
    // build runtime args for app
    Map<String, String> runtimeArguments = Maps.newHashMap();
    runtimeArguments.put(AppWithMapReduceUsingRuntimeFileSet.INPUT_NAME, "rtInput1");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeFileSet.INPUT_PATHS, "abc, xyz");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeFileSet.OUTPUT_NAME, "rtOutput1");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeFileSet.OUTPUT_PATH, "a001");
    // test reading and writing distinct datasets, reading more than one path
    testMapreduceWithFile("rtInput1", "abc, xyz", "rtOutput1", "a001",
                          AppWithMapReduceUsingRuntimeFileSet.class,
                          AppWithMapReduceUsingRuntimeFileSet.ComputeSum.class,
                          new BasicArguments(runtimeArguments));

    // test reading and writing same dataset
    dsFramework.addInstance("fileSet", rtInput2, FileSetProperties.builder()
      .setBasePath("/rtInput2")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
      .build());
    runtimeArguments = Maps.newHashMap();
    runtimeArguments.put(AppWithMapReduceUsingRuntimeFileSet.INPUT_NAME, "rtInput2");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeFileSet.INPUT_PATHS, "zzz");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeFileSet.OUTPUT_NAME, "rtInput2");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeFileSet.OUTPUT_PATH, "f123");
    testMapreduceWithFile("rtInput2", "zzz", "rtInput2", "f123",
                          AppWithMapReduceUsingRuntimeFileSet.class,
                          AppWithMapReduceUsingRuntimeFileSet.ComputeSum.class,
                          new BasicArguments(runtimeArguments));
  }

  private void testMapreduceWithFile(String inputDatasetName, String inputPaths,
                                     String outputDatasetName, String outputPath,
                                     Class appClass, Class mrClass,
                                     Arguments runtimeArgs) throws Exception {

    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(appClass, TEMP_FOLDER_SUPPLIER);

    Map<String, String> inputArgs = Maps.newHashMap();
    Map<String, String> outputArgs = Maps.newHashMap();
    FileSetArguments.setInputPaths(inputArgs, inputPaths);
    FileSetArguments.setOutputPath(outputArgs, outputPath);

    // write a handful of numbers to a file; compute their sum, too.
    final long[] values = { 15L, 17L, 7L, 3L };
    final FileSet input = datasetInstantiator.getDataset(inputDatasetName, inputArgs);
    long sum = 0L, count = 1;
    for (Location inputLocation : input.getInputLocations()) {
      final PrintWriter writer = new PrintWriter(inputLocation.getOutputStream());
      for (long value : values) {
        value *= count;
        writer.println(value);
        sum += value;
      }
      writer.close();
      count++;
    }

    runProgram(app, mrClass, runtimeArgs);

    // output location in file system is a directory that contains a part file, a _SUCCESS file, and checksums
    // (.<filename>.crc) for these files. Find the actual part file. Its name begins with "part". In this case,
    // there should be only one part file (with this small data, we have a single reducer).
    final FileSet results = datasetInstantiator.getDataset(outputDatasetName, outputArgs);
    Location resultLocation = results.getOutputLocation();
    if (resultLocation.isDirectory()) {
      for (Location child : resultLocation.list()) {
        if (!child.isDirectory() && child.getName().startsWith("part")) {
          resultLocation = child;
          break;
        }
      }
    }
    Assert.assertFalse(resultLocation.isDirectory());

    // read output and verify result
    String line = CharStreams.readFirstLine(
      CharStreams.newReaderSupplier(
        Locations.newInputSupplier(resultLocation), Charsets.UTF_8));
    Assert.assertNotNull(line);
    String[] fields = line.split(":");
    Assert.assertEquals(2, fields.length);
    Assert.assertEquals(AppWithMapReduceUsingFileSet.FileMapper.ONLY_KEY, fields[0]);
    Assert.assertEquals(sum, Long.parseLong(fields[1]));
  }


  @Test
  public void testMapreduceWithObjectStore() throws Exception {
    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(AppWithMapReduceUsingObjectStore.class, TEMP_FOLDER_SUPPLIER);

    final ObjectStore<String> input = datasetInstantiator.getDataset("keys");

    final String testString = "persisted data";

    //Populate some input
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          input.write(Bytes.toBytes(testString), testString);
          input.write(Bytes.toBytes("distributed systems"), "distributed systems");
        }
      });

    runProgram(app, AppWithMapReduceUsingObjectStore.ComputeCounts.class, false);

    final KeyValueTable output = datasetInstantiator.getDataset("count");
    //read output and verify result
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
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
    final java.io.File outputDir = new java.io.File(tmpFolder.newFolder(), "output");

    final KeyValueTable jobConfigTable = datasetInstantiator.getDataset("jobConfig");

    // write config into dataset
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          jobConfigTable.write(Bytes.toBytes("inputPath"), Bytes.toBytes(inputPath));
          jobConfigTable.write(Bytes.toBytes("outputPath"), Bytes.toBytes(outputDir.getPath()));
        }
      });

    runProgram(app, AppWithMapReduce.ClassicWordCount.class, false);

    java.io.File[] outputFiles = outputDir.listFiles();
    Assert.assertNotNull("no output files found", outputFiles);
    Assert.assertTrue("no output files found", outputFiles.length > 0);
    java.io.File outputFile = outputFiles[0];
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
    final TimeseriesTable table = (TimeseriesTable) datasetInstantiator.getDataset("timeSeries");
    final KeyValueTable beforeSubmitTable = datasetInstantiator.getDataset("beforeSubmit");
    final KeyValueTable onFinishTable = datasetInstantiator.getDataset("onFinish");
    final Table counters = datasetInstantiator.getDataset("counters");
    final Table countersFromContext = datasetInstantiator.getDataset("countersFromContext");

    // 1) fill test data
    fillTestInputData(txExecutorFactory, datasetInstantiator, table, false);

    // 2) run job
    final long start = System.currentTimeMillis();
    runProgram(app, AppWithMapReduce.AggregateTimeseriesByTag.class, frequentFlushing);
    final long stop = System.currentTimeMillis();

    // 3) verify results
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
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

    // todo: verify metrics. Will be possible after refactor for CDAP-765
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
    final TimeseriesTable table = datasetInstantiator.getDataset("timeSeries");
    final KeyValueTable beforeSubmitTable = datasetInstantiator.getDataset("beforeSubmit");
    final KeyValueTable onFinishTable = datasetInstantiator.getDataset("onFinish");
    final Table counters = datasetInstantiator.getDataset("counters");
    final Table countersFromContext = datasetInstantiator.getDataset("countersFromContext");

    // 1) fill test data
    fillTestInputData(txExecutorFactory, datasetInstantiator, table, true);

    // 2) run job
    final long start = System.currentTimeMillis();
    runProgram(app, AppWithMapReduce.AggregateTimeseriesByTag.class, frequentFlushing);
    final long stop = System.currentTimeMillis();

    // 3) verify results
    txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware()).execute(
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
                                 DatasetInstantiator datasetInstantiator,
                                 final TimeseriesTable table,
                                 final boolean withBadData) throws TransactionFailureException, InterruptedException {
    TransactionExecutor executor = txExecutorFactory.createExecutor(datasetInstantiator.getTransactionAware());
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

  private ProgramController submit(ApplicationWithPrograms app, Class<?> programClass, boolean frequentFlushing)
    throws ClassNotFoundException {
    HashMap<String, String> userArgs = Maps.newHashMap();
    userArgs.put("metric", "metric");
    userArgs.put("startTs", "1");
    userArgs.put("stopTs", "3");
    userArgs.put("tag", "tag1");
    if (frequentFlushing) {
      userArgs.put("frequentFlushing", "true");
    }
    return submit(app, programClass, new BasicArguments(userArgs));
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

  private String createInput() throws IOException {
    java.io.File inputDir = tmpFolder.newFolder();

    java.io.File inputFile = new java.io.File(inputDir.getPath() + "/words.txt");
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

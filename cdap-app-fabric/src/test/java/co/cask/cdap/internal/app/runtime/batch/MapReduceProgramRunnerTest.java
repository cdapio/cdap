/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.DefaultId;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TxConstants;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
@Category(XSlowTests.class)
public class MapReduceProgramRunnerTest extends MapReduceRunnerTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceProgramRunnerTest.class);

  @ClassRule
  public static final ExternalResource RESOURCE = new ExternalResource() {
    @Override
    protected void before() throws Throwable {
      // Set the tx timeout to a ridiculously low value that will test that the long-running transactions
      // actually bypass that timeout.
      System.setProperty(TxConstants.Manager.CFG_TX_TIMEOUT, "1");
      System.setProperty(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, "2");
    }
  };

  /**
   * Tests that initialize() and getSplits() are called in the same transaction,
   * and with the same instance of the input dataset.
   */
  @Test
  public void testTransactionHandling() throws Exception {
    final ApplicationWithPrograms app = deployApp(AppWithTxAware.class);
    runProgram(app, AppWithTxAware.PedanticMapReduce.class,
               new BasicArguments(ImmutableMap.of("outputPath", TEMP_FOLDER_SUPPLIER.get().getPath() + "/output")));
  }

  @Test
  public void testMapreduceWithFileSet() throws Exception {
    testMapreduceWithFile("numbers", "abc, xyz", "sums", "a001",
                          AppWithMapReduceUsingFileSet.class,
                          AppWithMapReduceUsingFileSet.ComputeSum.class,
                          null, null, null);

    // test reading and writing same dataset
    // this time configure the output format to use # as the separator, overriding the dataset properties
    testMapreduceWithFile("boogie", "zzz", "boogie", "f123",
                          AppWithMapReduceUsingFileSet.class,
                          AppWithMapReduceUsingFileSet.ComputeSum.class,
                          null, null, "#"); // and also use # as the separator for validation
  }

  @Test
  public void testMapreduceWithDynamicDatasets() throws Exception {
    DatasetId rtInput1 = DefaultId.NAMESPACE.dataset("rtInput1");
    DatasetId rtInput2 = DefaultId.NAMESPACE.dataset("rtInput2");
    DatasetId rtOutput1 = DefaultId.NAMESPACE.dataset("rtOutput1");
    // create the datasets here because they are not created by the app
    dsFramework.addInstance("fileSet", rtInput1, FileSetProperties.builder()
      .setBasePath("rtInput1")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
      .build());
    dsFramework.addInstance("fileSet", rtOutput1, FileSetProperties.builder()
      .setBasePath("rtOutput1")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
      .build());
    // build runtime args for app
    Map<String, String> runtimeArguments = Maps.newHashMap();

    // Make sure there is only one mapper running at a time since this test has the Mapper writing
    // to a dataset using increment and the in-memory table doesn't really support concurrent increment
    runtimeArguments.put("mr.job.conf.mapreduce.local.map.tasks.maximum", "1");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeDatasets.INPUT_NAME, "rtInput1");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeDatasets.INPUT_PATHS, "abc, xyz");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeDatasets.OUTPUT_NAME, "rtOutput1");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeDatasets.OUTPUT_PATH, "a001");
    // test reading and writing distinct datasets, reading more than one path
    testMapreduceWithFile("rtInput1", "abc, xyz", "rtOutput1", "a001",
                          AppWithMapReduceUsingRuntimeDatasets.class,
                          AppWithMapReduceUsingRuntimeDatasets.ComputeSum.class,
                          runtimeArguments,
                          AppWithMapReduceUsingRuntimeDatasets.COUNTERS,
                          null);

    // validate that the table emitted metrics
    Collection<MetricTimeSeries> metrics =
      metricStore.query(new MetricDataQuery(
        0,
        System.currentTimeMillis() / 1000L,
        Integer.MAX_VALUE,
        "system." + Constants.Metrics.Name.Dataset.OP_COUNT,
        AggregationFunction.SUM,
        ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, DefaultId.NAMESPACE.getNamespace(),
                        Constants.Metrics.Tag.APP, AppWithMapReduceUsingRuntimeDatasets.APP_NAME,
                        Constants.Metrics.Tag.MAPREDUCE, AppWithMapReduceUsingRuntimeDatasets.MR_NAME,
                        Constants.Metrics.Tag.DATASET, "rtt"),
        Collections.<String>emptyList()));
    Assert.assertEquals(1, metrics.size());
    MetricTimeSeries ts = metrics.iterator().next();
    Assert.assertEquals(1, ts.getTimeValues().size());
    Assert.assertEquals(1, ts.getTimeValues().get(0).getValue());

    // test reading and writing same dataset
    dsFramework.addInstance("fileSet", rtInput2, FileSetProperties.builder()
      .setBasePath("rtInput2")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
      .build());
    runtimeArguments = Maps.newHashMap();
    runtimeArguments.put(AppWithMapReduceUsingRuntimeDatasets.INPUT_NAME, "rtInput2");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeDatasets.INPUT_PATHS, "zzz");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeDatasets.OUTPUT_NAME, "rtInput2");
    runtimeArguments.put(AppWithMapReduceUsingRuntimeDatasets.OUTPUT_PATH, "f123");
    testMapreduceWithFile("rtInput2", "zzz", "rtInput2", "f123",
                          AppWithMapReduceUsingRuntimeDatasets.class,
                          AppWithMapReduceUsingRuntimeDatasets.ComputeSum.class,
                          runtimeArguments,
                          AppWithMapReduceUsingRuntimeDatasets.COUNTERS,
                          null);
  }


  @Test
  public void testMapReduceMetricsControl() throws Exception {
    final ApplicationWithPrograms app = deployApp(new NamespaceId("metrics_ns").toId(), AppWithMapReduce.class);

    Map<String, String> runtimeArguments = Maps.newHashMap();
    // do not emit metrics for this app
    runtimeArguments.put("metric", "metric");
    runtimeArguments.put("startTs", "1");
    runtimeArguments.put("stopTs", "3");
    runtimeArguments.put("tag", "tag1");
    // Do not emit metrics for mapreduce
    runtimeArguments.put(SystemArguments.METRICS_ENABLED, "false");
    runProgram(app, AppWithMapReduce.AggregateTimeseriesByTag.class, new BasicArguments(runtimeArguments));
    Collection<MetricTimeSeries> metrics = getMetricTimeSeries();

    Assert.assertEquals(0, metrics.size());

    // emit metrics for mapreduce
    runtimeArguments.put(SystemArguments.METRICS_ENABLED, "true");
    runProgram(app, AppWithMapReduce.AggregateTimeseriesByTag.class, new BasicArguments(runtimeArguments));
    metrics = getMetricTimeSeries();

    Assert.assertTrue(metrics.size() > 0);
  }

  private Collection<MetricTimeSeries> getMetricTimeSeries() {
    return metricStore.query(new MetricDataQuery(
      0,
      System.currentTimeMillis() / 1000L,
      Integer.MAX_VALUE,
      "user.beforeSubmit",
      AggregationFunction.SUM,
      ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "metrics_ns",
                      Constants.Metrics.Tag.APP, "AppWithMapReduce",
                      Constants.Metrics.Tag.MAPREDUCE, "AggregateTimeseriesByTag"),
      Collections.<String>emptyList()));
  }

  private void testMapreduceWithFile(String inputDatasetName, String inputPaths,
                                     String outputDatasetName, String outputPath,
                                     Class appClass, Class mrClass,
                                     Map<String, String> extraRuntimeArgs,
                                     @Nullable final String counterTableName,
                                     @Nullable final String outputSeparator) throws Exception {

    final ApplicationWithPrograms app = deployApp(
      appClass, new AppWithMapReduceUsingFileSet.AppConfig(inputDatasetName, outputDatasetName));

    Map<String, String> runtimeArguments = Maps.newHashMap();
    Map<String, String> inputArgs = Maps.newHashMap();
    Map<String, String> outputArgs = Maps.newHashMap();
    FileSetArguments.setInputPaths(inputArgs, inputPaths);
    FileSetArguments.setOutputPath(outputArgs, outputPath);
    if (outputSeparator != null) {
      outputArgs.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + TextOutputFormat.SEPERATOR, "#");
    }
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, inputDatasetName, inputArgs));
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, outputDatasetName, outputArgs));
    if (extraRuntimeArgs != null) {
      runtimeArguments.putAll(extraRuntimeArgs);
    }

    // clear the counters in case a previous test case left behind some values
    if (counterTableName != null) {
      Transactions.execute(datasetCache.newTransactionContext(), "countersVerify", new Runnable() {
        @Override
        public void run() {
          KeyValueTable counters = datasetCache.getDataset(counterTableName);
          counters.delete(AppWithMapReduceUsingRuntimeDatasets.INPUT_RECORDS);
          counters.delete(AppWithMapReduceUsingRuntimeDatasets.REDUCE_KEYS);
        }
      });
    }

    // write a handful of numbers to a file; compute their sum, too.
    final long[] values = { 15L, 17L, 7L, 3L };
    final FileSet input = datasetCache.getDataset(inputDatasetName, inputArgs);
    long sum = 0L, count = 1;
    long inputRecords = 0;
    for (Location inputLocation : input.getInputLocations()) {
      final PrintWriter writer = new PrintWriter(inputLocation.getOutputStream());
      for (long value : values) {
        value *= count;
        writer.println(value);
        sum += value;
        inputRecords++;
      }
      writer.close();
      count++;
    }

    runProgram(app, mrClass, new BasicArguments(runtimeArguments));

    // output location in file system is a directory that contains a part file, a _SUCCESS file, and checksums
    // (.<filename>.crc) for these files. Find the actual part file. Its name begins with "part". In this case,
    // there should be only one part file (with this small data, we have a single reducer).
    final FileSet results = datasetCache.getDataset(outputDatasetName, outputArgs);
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
    String[] fields = line.split(outputSeparator == null ? ":" : outputSeparator);
    Assert.assertEquals(2, fields.length);
    Assert.assertEquals(AppWithMapReduceUsingFileSet.FileMapper.ONLY_KEY, fields[0]);
    Assert.assertEquals(sum, Long.parseLong(fields[1]));

    if (counterTableName != null) {
      final long totalInputRecords = inputRecords;
      Transactions.execute(datasetCache.newTransactionContext(), "countersVerify", new Runnable() {
        @Override
        public void run() {
          KeyValueTable counters = datasetCache.getDataset(counterTableName);
          Assert.assertEquals(totalInputRecords,
                              counters.incrementAndGet(AppWithMapReduceUsingRuntimeDatasets.INPUT_RECORDS, 0L));
          Assert.assertEquals(1L, counters.incrementAndGet(AppWithMapReduceUsingRuntimeDatasets.REDUCE_KEYS, 0L));
        }
      });
    }
  }

  @Test
  public void testMapReduceDriverResources() throws Exception {
    final ApplicationWithPrograms app = deployApp(AppWithMapReduce.class);
    MapReduceSpecification mrSpec =
      app.getSpecification().getMapReduce().get(AppWithMapReduce.ClassicWordCount.class.getSimpleName());
    Assert.assertEquals(AppWithMapReduce.ClassicWordCount.MEMORY_MB, mrSpec.getDriverResources().getMemoryMB());
  }

  @Test
  public void testMapreduceWithObjectStore() throws Exception {
    // Deploy apps to another namespace and test cross-namespace access meanwhile
    final ApplicationWithPrograms app = deployApp(new NamespaceId("someOtherNameSpace").toId(),
                                                  AppWithMapReduceUsingObjectStore.class);

    final ObjectStore<String> input = datasetCache.getDataset("someOtherNameSpace", "keys");

    // Get dataset from a non existing namespace
    try {
      datasetCache.getDataset("nonExistingNameSpace", "keys");
      Assert.fail("getDataset() should throw an exception when accessing dataset from a non-existing namespace.");
    } catch (DatasetInstantiationException e) {
      // expected
    }

    final String testString = "persisted data";

    //Populate some input
    Transactions.createTransactionExecutor(txExecutorFactory, (TransactionAware) input).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          input.write(Bytes.toBytes(testString), testString);
          input.write(Bytes.toBytes("distributed systems"), "distributed systems");
        }
      });

    runProgram(app, AppWithMapReduceUsingObjectStore.ComputeCounts.class, false, true);

    final KeyValueTable output = datasetCache.getDataset("someOtherNameSpace", "count");
    //read output and verify result
    Transactions.createTransactionExecutor(txExecutorFactory, output).execute(
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
    // deploy to namespace default by default
    final ApplicationWithPrograms app = deployApp(AppWithMapReduce.class);
    final String inputPath = createInput();
    final java.io.File outputDir = new java.io.File(TEMP_FOLDER.newFolder(), "output");

    try {
      datasetCache.getDataset("someOtherNameSpace", "jobConfig");
      Assert.fail("getDataset() should throw an exception when accessing a non-existing dataset.");
    } catch (DatasetInstantiationException e) {
      // expected
    }
    // Should work if explicitly specify the default namespace
    final KeyValueTable jobConfigTable = datasetCache.getDataset(NamespaceId.DEFAULT.getNamespace(), "jobConfig");

    // write config into dataset
    Transactions.createTransactionExecutor(txExecutorFactory, jobConfigTable).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          jobConfigTable.write(Bytes.toBytes("inputPath"), Bytes.toBytes(inputPath));
          jobConfigTable.write(Bytes.toBytes("outputPath"), Bytes.toBytes(outputDir.getPath()));
        }
      });

    runProgram(app, AppWithMapReduce.ClassicWordCount.class, false, true);

    Assert.assertEquals("true", System.getProperty("partitioner.initialize"));
    Assert.assertEquals("true", System.getProperty("partitioner.destroy"));
    Assert.assertEquals("true", System.getProperty("partitioner.set.conf"));
    Assert.assertEquals("true", System.getProperty("comparator.initialize"));
    Assert.assertEquals("true", System.getProperty("comparator.destroy"));
    Assert.assertEquals("true", System.getProperty("comparator.set.conf"));

    File[] outputFiles = outputDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("part-r-") && !name.endsWith(".crc");
      }
    });
    Assert.assertNotNull("no output files found", outputFiles);

    int lines = 0;
    for (File file : outputFiles) {
      lines += Files.readLines(file, Charsets.UTF_8).size();
    }

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
    final ApplicationWithPrograms app = deployApp(AppWithMapReduce.class);

    // we need to start a tx context and do a "get" on all datasets so that they are in datasetCache
    datasetCache.newTransactionContext();
    final TimeseriesTable table = datasetCache.getDataset("timeSeries");
    final KeyValueTable beforeSubmitTable = datasetCache.getDataset("beforeSubmit");
    final KeyValueTable onFinishTable = datasetCache.getDataset("onFinish");
    final Table counters = datasetCache.getDataset("counters");
    final Table countersFromContext = datasetCache.getDataset("countersFromContext");

    // 1) fill test data
    fillTestInputData(txExecutorFactory, table, false);

    // 2) run job
    final long start = System.currentTimeMillis();
    runProgram(app, AppWithMapReduce.AggregateTimeseriesByTag.class, frequentFlushing, true);
    final long stop = System.currentTimeMillis();

    // 3) verify results
    Transactions.createTransactionExecutor(txExecutorFactory, datasetCache.getTransactionAwares()).execute(
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
    datasetCache.dismissTransactionContext();

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

  @Test
  public void testMapReduceWithLocalFiles() throws Exception {
    ApplicationWithPrograms appWithPrograms = deployApp(AppWithLocalFiles.class);
    URI stopWordsFile = createStopWordsFile();

    final KeyValueTable kvTable = datasetCache.getDataset(AppWithLocalFiles.MR_INPUT_DATASET);
    Transactions.createTransactionExecutor(txExecutorFactory, kvTable).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          kvTable.write("2324", "a test record");
          kvTable.write("43353", "the test table");
          kvTable.write("34335", "an end record");
        }
      }
    );
    runProgram(appWithPrograms, AppWithLocalFiles.MapReduceWithLocalFiles.class,
               new BasicArguments(ImmutableMap.of(
                 AppWithLocalFiles.MR_INPUT_DATASET, "input",
                 AppWithLocalFiles.MR_OUTPUT_DATASET, "output",
                 AppWithLocalFiles.STOPWORDS_FILE_ARG, stopWordsFile.toString()
               )));
    final KeyValueTable outputKvTable = datasetCache.getDataset(AppWithLocalFiles.MR_OUTPUT_DATASET);
    Transactions.createTransactionExecutor(txExecutorFactory, outputKvTable).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          Assert.assertNull(outputKvTable.read("a"));
          Assert.assertNull(outputKvTable.read("the"));
          Assert.assertNull(outputKvTable.read("an"));
          Assert.assertEquals(2, Bytes.toInt(outputKvTable.read("test")));
          Assert.assertEquals(2, Bytes.toInt(outputKvTable.read("record")));
          Assert.assertEquals(1, Bytes.toInt(outputKvTable.read("table")));
          Assert.assertEquals(1, Bytes.toInt(outputKvTable.read("end")));
        }
      }
    );
  }

  private URI createStopWordsFile() throws IOException {
    File file = TEMP_FOLDER.newFile("stopWords.txt");
    try (OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(file))) {
      out.write("the\n");
      out.write("a\n");
      out.write("an");
    }
    return file.toURI();
  }

  // TODO: this tests failure in Map tasks. We also need to test: failure in Reduce task, kill of a job by user.
  private void testFailure(boolean frequentFlushing) throws Exception {
    // We want to verify that when mapreduce job fails:
    // * things written in initialize() remains and visible to others
    // * things written in tasks not visible to others TODO AAA: do invalidate
    // * things written in onfinish() remains and visible to others

    // NOTE: the code of this test is similar to testTimeSeriesRecordsCount() test. We put some "bad data" intentionally
    //       here to be recognized by map tasks as a message to emulate failure

    final ApplicationWithPrograms app = deployApp(AppWithMapReduce.class);

    // we need to start a tx context and do a "get" on all datasets so that they are in datasetCache
    datasetCache.newTransactionContext();
    final TimeseriesTable table = datasetCache.getDataset("timeSeries");
    final KeyValueTable beforeSubmitTable = datasetCache.getDataset("beforeSubmit");
    final KeyValueTable onFinishTable = datasetCache.getDataset("onFinish");
    final Table counters = datasetCache.getDataset("counters");
    final Table countersFromContext = datasetCache.getDataset("countersFromContext");

    // 1) fill test data
    fillTestInputData(txExecutorFactory, table, true);

    // 2) run job
    final long start = System.currentTimeMillis();
    runProgram(app, AppWithMapReduce.AggregateTimeseriesByTag.class, frequentFlushing, false);
    final long stop = System.currentTimeMillis();

    // 3) verify results
    Transactions.createTransactionExecutor(txExecutorFactory, datasetCache.getTransactionAwares()).execute(
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

    datasetCache.dismissTransactionContext();
  }

  private void fillTestInputData(TransactionExecutorFactory txExecutorFactory,
                                 final TimeseriesTable table,
                                 final boolean withBadData) throws TransactionFailureException, InterruptedException {
    TransactionExecutor executor = Transactions.createTransactionExecutor(txExecutorFactory, table);
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

  @Test
  public void testFailureInInit() throws Exception {
    final ApplicationWithPrograms app = deployApp(AppWithMapReduce.class);

    testFailureInInit("true", app, AppWithMapReduce.FaiiingMR.class, ImmutableMap.<String, String>of());
    testFailureInInit("false", app, AppWithMapReduce.FaiiingMR.class, ImmutableMap.of("failInput", "true"));
    testFailureInInit("false", app, AppWithMapReduce.FaiiingMR.class, ImmutableMap.of("failOutput", "true"));
    testFailureInInit("true", app, AppWithMapReduce.ExplicitFaiiingMR.class, ImmutableMap.<String, String>of());
    testFailureInInit("false", app, AppWithMapReduce.ExplicitFaiiingMR.class, ImmutableMap.of("failInput", "true"));
    testFailureInInit("false", app, AppWithMapReduce.ExplicitFaiiingMR.class, ImmutableMap.of("failOutput", "true"));
  }

  public void testFailureInInit(final String expected, ApplicationWithPrograms app,
                                Class<?> programClass, Map<String, String> args) throws Exception {
    // We want to verify that when a mapreduce fails during initialize(), especially
    // if an input or output format provider fails to produce its configuration, the
    // writes by that initialize() method are rolled back. (Background: prior to
    // CDAP-7476, the input/output format provider was called *after* initialize
    // returns, and therefore that transaction may have been committed already.

    // (1) initialize the table with a known value
    datasetCache.newTransactionContext();
    final KeyValueTable kvTable = datasetCache.getDataset("recorder");
    Transactions.createTransactionExecutor(txExecutorFactory, datasetCache.getTransactionAwares()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          // the table should not have initialized=true
          kvTable.write("initialized", "false");
        }
      });

    // 2) run job
    runProgram(app, programClass, args, false);

    // 3) verify results
    Transactions.createTransactionExecutor(txExecutorFactory, datasetCache.getTransactionAwares()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          // the table should not have initialized=true
          Assert.assertEquals(expected, Bytes.toString(kvTable.read("initialized")));
        }
      });
    datasetCache.dismissTransactionContext();
  }

  @Test
  public void testFailureInOutputCommitter() throws Exception {
    final ApplicationWithPrograms app = deployApp(AppWithMapReduce.class);

    // We want to verify that when a mapreduce fails when committing the dataset outputs,
    // the destroy method is still called and committed.

    // (1) setup the datasets we use
    datasetCache.newTransactionContext();
    final KeyValueTable kvTable = datasetCache.getDataset("recorder");

    Transactions.createTransactionExecutor(txExecutorFactory, datasetCache.getTransactionAwares()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          // the table should not have initialized=true
          kvTable.write("initialized", "false");
        }
      });

    // 2) run job
    runProgram(app, AppWithMapReduce.MapReduceWithFailingOutputCommitter.class, new HashMap<String, String>(), false);

    // 3) verify results
    Transactions.createTransactionExecutor(txExecutorFactory, datasetCache.getTransactionAwares()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          // the destroy() method should have recorded FAILED status in the kv table
          Assert.assertEquals(ProgramStatus.FAILED.name(), Bytes.toString(kvTable.read("status")));
        }
      });
    datasetCache.dismissTransactionContext();
  }

  private void runProgram(ApplicationWithPrograms app, Class<?> programClass,
                          boolean frequentFlushing, boolean expectedStatus) throws Exception {
    Map<String, String> userArgs = new HashMap<>();
    userArgs.put("metric", "metric");
    userArgs.put("startTs", "1");
    userArgs.put("stopTs", "3");
    userArgs.put("tag", "tag1");
    if (frequentFlushing) {
      userArgs.put("frequentFlushing", "true");
    }
    runProgram(app, programClass, userArgs, expectedStatus);
  }

  private void runProgram(ApplicationWithPrograms app, Class<?> programClass,
                          Map<String, String> userArgs, boolean expectedStatus) throws Exception {
    LOG.info("Starting {} with arguments {}", programClass.getName(), userArgs);
    Assert.assertEquals(expectedStatus, runProgram(app, programClass, new BasicArguments(userArgs)));
  }

  private String createInput() throws IOException {
    File inputDir = TEMP_FOLDER.newFolder();

    File inputFile = new File(inputDir.getPath() + "/words.txt");
    inputFile.deleteOnExit();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(inputFile))) {
      writer.write("this text has");
      writer.newLine();
      writer.write("two words text inside");
    }

    return inputDir.getPath();
  }

}

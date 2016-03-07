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
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.SingleThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.DefaultId;
import co.cask.cdap.internal.TempFolder;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.XSlowTests;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TxConstants;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Injector;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
@Category(XSlowTests.class)
public class SparkProgramRunnerTest {

  private static final TempFolder TEMP_FOLDER = new TempFolder();
  private static final Gson GSON = new Gson();

  private static Injector injector;
  private static TransactionExecutorFactory txExecutorFactory;
  private static TransactionManager txService;
  private static DatasetFramework dsFramework;
  private static DynamicDatasetCache datasetCache;
  private static MetricStore metricStore;

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
    dsFramework = injector.getInstance(DatasetFramework.class);
    datasetCache = new SingleThreadDatasetCache(
      new SystemDatasetInstantiator(dsFramework, SparkProgramRunnerTest.class.getClassLoader(), null),
      injector.getInstance(TransactionSystemClient.class),
      NamespaceId.DEFAULT, DatasetDefinition.NO_ARGUMENTS, null, null);
    metricStore = injector.getInstance(MetricStore.class);

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
    datasetCache.invalidate();
  }

  @Test
  public void testSparkWithObjectStore() throws Exception {
    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(SparkAppUsingObjectStore.class, TEMP_FOLDER_SUPPLIER);

    prepareInputData();
    runProgram(app, SparkAppUsingObjectStore.CharCountSpecification.class);
    checkOutputData();

    // validate that the table emitted metrics
    // one read + one write in beforeSubmit(), increment (= read + write) in main -> 4
    Tasks.waitFor(4L, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        Collection<MetricTimeSeries> metrics =
          metricStore.query(new MetricDataQuery(
            0,
            System.currentTimeMillis() / 1000L,
            60,
            "system." + Constants.Metrics.Name.Dataset.OP_COUNT,
            AggregationFunction.SUM,
            ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, DefaultId.NAMESPACE.getId(),
                            Constants.Metrics.Tag.APP, SparkAppUsingObjectStore.APP_NAME,
                            Constants.Metrics.Tag.SPARK, SparkAppUsingObjectStore.SPARK_NAME,
                            Constants.Metrics.Tag.DATASET, "totals"),
            Collections.<String>emptyList()));
        if (metrics.isEmpty()) {
          return 0L;
        }
        Assert.assertEquals(1, metrics.size());
        MetricTimeSeries ts = metrics.iterator().next();
        Assert.assertEquals(1, ts.getTimeValues().size());
        return ts.getTimeValues().get(0).getValue();
      }
    }, 10L, TimeUnit.SECONDS, 50L, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testScalaSparkWithObjectStore() throws Exception {
    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(ScalaSparkAppUsingObjectStore.class, TEMP_FOLDER_SUPPLIER);

    prepareInputData();
    runProgram(app, ScalaSparkAppUsingObjectStore.CharCountSpecification.class);
    checkOutputData();
  }

  @Test
  public void testSparkWithFileSet() throws Exception {
    testSparkWithFileSet(SparkAppUsingFileSet.class, SparkAppUsingFileSet.JavaCharCount.class);
  }

  @Test
  public void testSparkScalaWithFileSet() throws Exception {
    testSparkWithFileSet(SparkAppUsingFileSet.class, SparkAppUsingFileSet.ScalaCharCount.class);
  }

  private void testSparkWithFileSet(Class<?> appClass, Class<?> programClass) throws Exception {
    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(appClass, TEMP_FOLDER_SUPPLIER);

    final FileSet fileset = datasetCache.getDataset("fs");
    Location location = fileset.getLocation("nn");
    prepareFileInput(location);

    Map<String, String> inputArgs = new HashMap<>();
    FileSetArguments.setInputPath(inputArgs, "nn");
    Map<String, String> outputArgs = new HashMap<>();
    FileSetArguments.setOutputPath(inputArgs, "xx");
    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "fs", inputArgs));
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "fs", outputArgs));
    args.put("input", "fs");
    args.put("output", "fs");

    runProgram(app, programClass, args);

    validateFileOutput(fileset.getLocation("xx"));
  }

  @Test
  public void testSparkWithPartitionedFileSet() throws Exception {
    testSparkWithPartitionedFileSet(SparkAppUsingFileSet.class, SparkAppUsingFileSet.JavaCharCount.class);
  }

  @Test
  public void testSparkScalaWithPartitionedFileSet() throws Exception {
    testSparkWithPartitionedFileSet(SparkAppUsingFileSet.class, SparkAppUsingFileSet.ScalaCharCount.class);
  }

  private void testSparkWithPartitionedFileSet(Class<?> appClass, Class<?> programClass) throws Exception {
    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(appClass, TEMP_FOLDER_SUPPLIER);

    // we need to start a tx context and do a "get" on all datasets so that they are in datasetCache
    datasetCache.newTransactionContext();
    final PartitionedFileSet pfs = datasetCache.getDataset("pfs");
    final PartitionOutput partitionOutput = pfs.getPartitionOutput(
      PartitionKey.builder().addStringField("x", "nn").build());
    Location location = partitionOutput.getLocation();
    prepareFileInput(location);
    txExecutorFactory.createExecutor(datasetCache.getTransactionAwares()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          partitionOutput.addPartition();
        }
      });

    Map<String, String> inputArgs = new HashMap<>();
    PartitionedFileSetArguments.setInputPartitionFilter(
      inputArgs, PartitionFilter.builder().addRangeCondition("x", "na", "nx").build());
    Map<String, String> outputArgs = new HashMap<>();
    final PartitionKey outputKey = PartitionKey.builder().addStringField("x", "xx").build();
    PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, outputKey);
    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "pfs", inputArgs));
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "pfs", outputArgs));
    args.put("input", "pfs");
    args.put("output", "pfs");

    runProgram(app, programClass, args);

    txExecutorFactory.createExecutor(datasetCache.getTransactionAwares()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          PartitionDetail partition = pfs.getPartition(outputKey);
          Assert.assertNotNull(partition);
          validateFileOutput(partition.getLocation());
        }
      });
    datasetCache.dismissTransactionContext();
  }

  @Test
  public void testSparkWithTimePartitionedFileSet() throws Exception {
    testSparkWithTimePartitionedFileSet(SparkAppUsingFileSet.class, SparkAppUsingFileSet.JavaCharCount.class);
  }

  @Test
  public void testSparkScalaWithTimePartitionedFileSet() throws Exception {
    testSparkWithTimePartitionedFileSet(SparkAppUsingFileSet.class, SparkAppUsingFileSet.ScalaCharCount.class);
  }

  private void testSparkWithTimePartitionedFileSet(Class<?> appClass, Class<?> programClass) throws Exception {

    final long customOutputPartitionKey = 123456789L;
    final long customInputPartitionKey = 987654321L;

    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(appClass, TEMP_FOLDER_SUPPLIER);

    // we need to start a tx context and do a "get" on all datasets so that they are in datasetCache
    datasetCache.newTransactionContext();
    final TimePartitionedFileSet tpfs = datasetCache.getDataset("tpfs");
    long inputTime = System.currentTimeMillis();
    final long outputTime = inputTime + TimeUnit.HOURS.toMillis(1);

    addTimePartition(tpfs, inputTime);
    addTimePartition(tpfs, customInputPartitionKey);

    Map<String, String> inputArgs = new HashMap<>();
    TimePartitionedFileSetArguments.setInputStartTime(inputArgs, inputTime - 100);
    TimePartitionedFileSetArguments.setInputEndTime(inputArgs, inputTime + 100);
    Map<String, String> outputArgs = new HashMap<>();
    TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, outputTime);
    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "tpfs", inputArgs));
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "tpfs", outputArgs));
    args.put("input", "tpfs");
    args.put("output", "tpfs");
    args.put("outputKey", String.valueOf(customOutputPartitionKey));
    args.put("inputKey", String.valueOf(customInputPartitionKey));

    runProgram(app, programClass, args);

    txExecutorFactory.createExecutor(datasetCache.getTransactionAwares()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          PartitionDetail partition = tpfs.getPartitionByTime(outputTime);
          Assert.assertNotNull("Output partition is null while for running without custom dataset arguments",
                               partition);
          validateFileOutput(partition.getLocation());

          PartitionDetail customPartition = tpfs.getPartitionByTime(customOutputPartitionKey);
          Assert.assertNotNull("Output partition is null while for running with custom dataset arguments",
                               customPartition);
          validateFileOutput(customPartition.getLocation());
        }
      });
    datasetCache.dismissTransactionContext();
  }

  private void addTimePartition(TimePartitionedFileSet tpfs, long inputTime) throws IOException,
    TransactionFailureException, InterruptedException {
    final PartitionOutput partitionOutput = tpfs.getPartitionOutput(inputTime);
    Location location = partitionOutput.getLocation();
    prepareFileInput(location);
    txExecutorFactory.createExecutor(datasetCache.getTransactionAwares()).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          partitionOutput.addPartition();
        }
      });
  }

  @Test
  public void testSparkWithCustomFileSet() throws Exception {
    testSparkWithCustomFileSet(SparkAppUsingFileSet.class, SparkAppUsingFileSet.JavaCharCount.class);
  }

  @Test
  public void testSparkScalaWithCustomFileSet() throws Exception {
    testSparkWithCustomFileSet(SparkAppUsingFileSet.class, SparkAppUsingFileSet.ScalaCharCount.class);
  }

  private void testSparkWithCustomFileSet(Class<?> appClass, Class<?> programClass) throws Exception {
    final ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(appClass, TEMP_FOLDER_SUPPLIER);

    final SparkAppUsingFileSet.MyFileSet myfileset = datasetCache.getDataset("myfs");
    final FileSet fileset = myfileset.getEmbeddedFileSet();
    Location location = fileset.getLocation("nn");
    prepareFileInput(location);

    Map<String, String> inputArgs = new HashMap<>();
    FileSetArguments.setInputPath(inputArgs, "nn");
    Map<String, String> outputArgs = new HashMap<>();
    FileSetArguments.setOutputPath(inputArgs, "xx");
    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "myfs", inputArgs));
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "myfs", outputArgs));
    args.put("input", "myfs");
    args.put("output", "myfs");

    runProgram(app, programClass, args);

    validateFileOutput(fileset.getLocation("xx"));

    // verify that onSuccess() was called and onFailure() was not
    Assert.assertTrue(myfileset.getSuccessLocation().exists());
    Assert.assertFalse(myfileset.getFailureLocation().exists());
    myfileset.getSuccessLocation().delete();

    // run the program again. It should fail due to existing output.
    expectProgramError(app, programClass, args, FileAlreadyExistsException.class);

    // Then we can verify that onFailure() was called.
    Assert.assertFalse(myfileset.getSuccessLocation().exists());
    Assert.assertTrue(myfileset.getFailureLocation().exists());
    myfileset.getSuccessLocation().delete();
  }

  @Test
  public void testJavaSparkWithGetDataset() throws Exception {
    testSparkWithGetDataset(SparkAppUsingGetDataset.class, SparkAppUsingGetDataset.JavaSparkLogParser.class);
  }

  @Test
  public void testScalaSparkWithGetDataset() throws Exception {
    testSparkWithGetDataset(SparkAppUsingGetDataset.class, SparkAppUsingGetDataset.ScalaSparkLogParser.class);
  }

  private void testSparkWithGetDataset(Class<?> appClass, Class<?> programClass) throws Exception {
    ApplicationWithPrograms app =
      AppFabricTestHelper.deployApplicationWithManager(appClass, TEMP_FOLDER_SUPPLIER);
    FileSet fileset = datasetCache.getDataset("logs");
    Location location = fileset.getLocation("nn");
    prepareInputFileSetWithLogData(location);

    Map<String, String> inputArgs = new HashMap<>();
    FileSetArguments.setInputPath(inputArgs, "nn");
    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "logs", inputArgs));
    args.put("input", "logs");
    args.put("output", "logStats");

    runProgram(app, programClass, args);

    KeyValueTable logStatsTable = datasetCache.getDataset("logStats");
    validateGetDatasetOutput(logStatsTable);
  }

  @Test
  public void testJavaSparkWithLocalFiles() throws Exception {
    testSparkWithLocalFiles(SparkAppUsingLocalFiles.class, SparkAppUsingLocalFiles.JavaSparkUsingLocalFiles.class,
                            "java");
  }

  @Test
  public void testScalaSparkWithLocalFiles() throws Exception {
    testSparkWithLocalFiles(SparkAppUsingLocalFiles.class, SparkAppUsingLocalFiles.ScalaSparkUsingLocalFiles.class,
                            "scala");
  }

  private void testSparkWithLocalFiles(Class<?> appClass, Class<?> programClass, String prefix) throws Exception {
    ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(appClass, TEMP_FOLDER_SUPPLIER);
    URI localFile = createLocalPropertiesFile(prefix);
    runProgram(app, programClass,
               ImmutableMap.of(SparkAppUsingLocalFiles.LOCAL_FILE_RUNTIME_ARG, localFile.toString()));
    KeyValueTable kvTable = datasetCache.getDataset(SparkAppUsingLocalFiles.OUTPUT_DATASET_NAME);
    Map<String, String> expected = ImmutableMap.of("a", "1", "b", "2", "c", "3");
    CloseableIterator<KeyValue<byte[], byte[]>> scan = kvTable.scan(null, null);
    try {
      for (int i = 0; i < 3; i++) {
        KeyValue<byte[], byte[]> next = scan.next();
        Assert.assertEquals(expected.get(Bytes.toString(next.getKey())), Bytes.toString(next.getValue()));
      }
      Assert.assertFalse(scan.hasNext());
    } finally {
      scan.close();
    }
  }

  private URI createLocalPropertiesFile(String filePrefix) throws IOException {
    File file = TEMP_FOLDER.newFile(filePrefix + "-local.properties");
    try (FileOutputStream fos = new FileOutputStream(file);
         OutputStreamWriter out = new OutputStreamWriter(fos)) {
      out.write("a=1\n");
      out.write("b = 2\n");
      out.write("c= 3");
    }
    return file.toURI();
  }

  private void validateGetDatasetOutput(KeyValueTable logStatsTable) {
    SparkAppUsingGetDataset.LogKey fredKey1 = new SparkAppUsingGetDataset.LogKey(
      "10.10.10.10", "FRED", "GET http://bar.com/image.jpg HTTP/1.1", 200);
    SparkAppUsingGetDataset.LogKey fredKey2 = new SparkAppUsingGetDataset.LogKey(
      "10.10.10.10", "FRED", "GET http://bar.com/image.jpg HTTP/1.1", 404);
    SparkAppUsingGetDataset.LogKey bradKey1 = new SparkAppUsingGetDataset.LogKey(
      "20.20.20.20", "BRAD", "GET http://bar.com/image.jpg HTTP/1.1", 200);
    SparkAppUsingGetDataset.LogKey bradKey2 = new SparkAppUsingGetDataset.LogKey(
      "20.20.20.20", "BRAD", "GET http://bar.com/image.jpg HTTP/1.1", 404);
    SparkAppUsingGetDataset.LogStats fredStats1 = new SparkAppUsingGetDataset.LogStats(2, 100);
    SparkAppUsingGetDataset.LogStats fredStats2 = new SparkAppUsingGetDataset.LogStats(1, 50);
    SparkAppUsingGetDataset.LogStats bradStats1 = new SparkAppUsingGetDataset.LogStats(1, 50);
    SparkAppUsingGetDataset.LogStats bradStats2 = new SparkAppUsingGetDataset.LogStats(1, 50);

    Map<SparkAppUsingGetDataset.LogKey, SparkAppUsingGetDataset.LogStats> expected = ImmutableMap.of(
      fredKey1, fredStats1, fredKey2, fredStats2, bradKey1, bradStats1, bradKey2, bradStats2
    );

    CloseableIterator<KeyValue<byte[], byte[]>> scan = logStatsTable.scan(null, null);
    try {
      // must have 4 records
      for (int i = 0; i < 4; i++) {
        Assert.assertTrue("Expected next for i = " + i, scan.hasNext());
        KeyValue<byte[], byte[]> next = scan.next();
        SparkAppUsingGetDataset.LogKey logKey =
          GSON.fromJson(Bytes.toString(next.getKey()), SparkAppUsingGetDataset.LogKey.class);
        SparkAppUsingGetDataset.LogStats logStats =
          GSON.fromJson(Bytes.toString(next.getValue()), SparkAppUsingGetDataset.LogStats.class);
        Assert.assertEquals(expected.get(logKey), logStats);
      }
      // no more records
      Assert.assertFalse(scan.hasNext());
    } finally {
      scan.close();
    }
  }

  private void prepareInputFileSetWithLogData(Location location) throws IOException {
    try (OutputStreamWriter out = new OutputStreamWriter(location.getOutputStream())) {
      out.write("10.10.10.10 - FRED [18/Jan/2013:17:56:07 +1100] \"GET http://bar.com/image.jpg " +
                  "HTTP/1.1\" 200 50 \"http://foo.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                  "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                  ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                  "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 " +
                  "\"\" 62.24.11.25 images.com 1358492167 - Whatup\n");
      out.write("20.20.20.20 - BRAD [18/Jan/2013:17:56:07 +1100] \"GET http://bar.com/image.jpg " +
                  "HTTP/1.1\" 200 50 \"http://foo.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                  "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                  ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                  "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 " +
                  "\"\" 62.24.11.25 images.com 1358492167 - Whatup\n");
      out.write("10.10.10.10 - FRED [18/Jan/2013:17:56:07 +1100] \"GET http://bar.com/image.jpg " +
                  "HTTP/1.1\" 404 50 \"http://foo.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                  "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                  ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                  "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 " +
                  "\"\" 62.24.11.25 images.com 1358492167 - Whatup\n");
      out.write("10.10.10.10 - FRED [18/Jan/2013:17:56:07 +1100] \"GET http://bar.com/image.jpg " +
                  "HTTP/1.1\" 200 50 \"http://foo.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                  "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                  ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                  "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 " +
                  "\"\" 62.24.11.25 images.com 1358492167 - Whatup\n");
      out.write("20.20.20.20 - BRAD [18/Jan/2013:17:56:07 +1100] \"GET http://bar.com/image.jpg " +
                  "HTTP/1.1\" 404 50 \"http://foo.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
                  "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
                  ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
                  "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 " +
                  "\"\" 62.24.11.25 images.com 1358492167 - Whatup\n");
    }
  }

  private void validateFileOutput(Location location) throws Exception {
    Assert.assertTrue(location.isDirectory());
    for (Location child : location.list()) {
      if (child.getName().startsWith("part-r-")) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(child.getInputStream()))) {
          String line = reader.readLine();
          Assert.assertNotNull(line);
          Assert.assertEquals("13 characters:13", line);
          line = reader.readLine();
          Assert.assertNotNull(line);
          Assert.assertEquals("7 chars:7", line);
          line = reader.readLine();
          Assert.assertNull(line);
          return;
        }
      }
    }
    Assert.fail("Output directory does not contain any part file: " + location.list());
  }

  private void prepareFileInput(Location location) throws IOException {
    try (OutputStreamWriter out = new OutputStreamWriter(location.getOutputStream())) {
      out.write("13 characters\n");
      out.write("7 chars\n");
    }
  }

  private void prepareInputData() throws TransactionFailureException, InterruptedException {

    //Populate some input
    txExecutorFactory.createExecutor(datasetCache).execute(
      new TransactionExecutor.Subroutine() {
        @Override
        public void apply() {
          ObjectStore<String> input = datasetCache.getDataset("keys");
          input.write(Bytes.toBytes(testString1), testString1);
          input.write(Bytes.toBytes(testString2), testString2);
        }
      });
  }

  private void checkOutputData() throws TransactionFailureException, InterruptedException {
    final KeyValueTable output = datasetCache.getDataset("count");
    //read output and verify result
    txExecutorFactory.createExecutor(datasetCache.getTransactionAwares()).execute(
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
    runProgram(app, programClass, RuntimeArguments.NO_ARGUMENTS);
  }

  private void runProgram(ApplicationWithPrograms app, Class<?> programClass, Map<String, String> args)
    throws Exception {
    //noinspection ThrowableResultOfMethodCallIgnored
    waitForCompletion(submit(app, programClass, args));
  }

  private void expectProgramError(ApplicationWithPrograms app, Class<?> programClass, Map<String, String> args,
                                  Class<? extends Throwable> expected)
    throws Exception {
    // TODO: this should throw an exception but there seems to be a race condition where the
    // TODO:      spark program runner does not capture the failure. For now, do not validate
    //Throwable error = waitForCompletion(submit(app, programClass, args));
    //Assert.assertTrue(expected.isAssignableFrom(error.getClass()));
    runProgram(app, programClass, args);
  }

  private Throwable waitForCompletion(ProgramController controller) throws InterruptedException {
    final AtomicReference<Throwable> errorCause = new AtomicReference<>();
    final CountDownLatch completion = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {
      @Override
      public void completed() {
        completion.countDown();
      }

      @Override
      public void error(Throwable cause) {
        completion.countDown();
        errorCause.set(cause);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    completion.await(10, TimeUnit.MINUTES);
    return errorCause.get();
  }

  private ProgramController submit(ApplicationWithPrograms app,
                                   Class<?> programClass,
                                   Map<String, String> userArgs) throws ClassNotFoundException {

    ProgramRunnerFactory runnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    Program program = getProgram(app, programClass);
    Assert.assertNotNull(program);
    ProgramRunner runner = runnerFactory.create(program.getType());

    BasicArguments systemArgs = new BasicArguments(ImmutableMap.of(ProgramOptionConstants.RUN_ID,
                                                                   RunIds.generate().getId()));

    return runner.run(program, new SimpleProgramOptions(program.getName(), systemArgs, new BasicArguments(userArgs)));
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

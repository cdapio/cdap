/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.spark;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.DefaultId;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.spark.app.CharCountProgram;
import co.cask.cdap.spark.app.ClassicSparkProgram;
import co.cask.cdap.spark.app.DatasetSQLSpark;
import co.cask.cdap.spark.app.Person;
import co.cask.cdap.spark.app.PythonSpark;
import co.cask.cdap.spark.app.ScalaCharCountProgram;
import co.cask.cdap.spark.app.ScalaClassicSparkProgram;
import co.cask.cdap.spark.app.ScalaCrossNSProgram;
import co.cask.cdap.spark.app.ScalaDynamicSpark;
import co.cask.cdap.spark.app.ScalaSparkLogParser;
import co.cask.cdap.spark.app.ScalaStreamFormatSpecSpark;
import co.cask.cdap.spark.app.SparkAppUsingGetDataset;
import co.cask.cdap.spark.app.SparkAppUsingLocalFiles;
import co.cask.cdap.spark.app.SparkAppUsingObjectStore;
import co.cask.cdap.spark.app.SparkLogParser;
import co.cask.cdap.spark.app.StreamFormatSpecSpark;
import co.cask.cdap.spark.app.StreamSQLSpark;
import co.cask.cdap.spark.app.TestSparkApp;
import co.cask.cdap.spark.app.TransactionSpark;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Unit-tests for testing Spark program.
 */
public class SparkTestRun extends TestFrameworkTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static final Logger LOG = LoggerFactory.getLogger(SparkTestRun.class);

  private static final String TEST_STRING_1 = "persisted data";
  private static final String TEST_STRING_2 = "distributed systems";

  private static final Map<Class<? extends Application>, File> ARTIFACTS = new IdentityHashMap<>();

  @BeforeClass
  public static void init() throws IOException {
    ARTIFACTS.put(TestSparkApp.class, createArtifactJar(TestSparkApp.class));
    ARTIFACTS.put(SparkAppUsingObjectStore.class, createArtifactJar(SparkAppUsingObjectStore.class));
    ARTIFACTS.put(SparkAppUsingGetDataset.class, createArtifactJar(SparkAppUsingGetDataset.class));
    ARTIFACTS.put(SparkAppUsingLocalFiles.class, createArtifactJar(SparkAppUsingLocalFiles.class));
  }

  private ApplicationManager deploy(Class<? extends Application> appClass) throws Exception {
    return deployWithArtifact(appClass, ARTIFACTS.get(appClass));
  }

  private ApplicationManager deploy(NamespaceId namespaceId, Class<? extends Application> appClass) throws Exception {
    return deployWithArtifact(namespaceId, appClass, ARTIFACTS.get(appClass));
  }

  @Test
  public void testStreamSQL() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);

    // Create a stream and set the format and schema
    StreamManager streamManager = getStreamManager("sqlStream");
    streamManager.createStream();
    Schema schema = Schema.recordOf("person",
                                    Schema.Field.of("firstName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("lastName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("age", Schema.of(Schema.Type.INT)));
    streamManager.setStreamProperties(new StreamProperties(86400L, new FormatSpecification("csv", schema), 1000));

    // Send some events. Send them with one milliseconds apart so that we can test the timestamp filter
    streamManager.send("Bob,Robert,15");
    TimeUnit.MILLISECONDS.sleep(1);
    streamManager.send("Eddy,Edison,35");
    TimeUnit.MILLISECONDS.sleep(1);
    streamManager.send("Thomas,Edison,60");
    TimeUnit.MILLISECONDS.sleep(1);
    streamManager.send("Tom,Thomson,50");
    TimeUnit.MILLISECONDS.sleep(1);
    streamManager.send("Roy,Thomson,8");
    TimeUnit.MILLISECONDS.sleep(1);
    streamManager.send("Jane,Jenny,6");

    // Run the testing spark program
    SparkManager sparkManager = appManager.getSparkManager(StreamSQLSpark.class.getSimpleName())
      .start(Collections.singletonMap("input.stream", "sqlStream"));
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);
  }

  @Test
  public void testDatasetSQL() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);

    DataSetManager<ObjectMappedTable<Person>> tableManager = getDataset("PersonTable");

    ObjectMappedTable<Person> table = tableManager.get();
    table.write("1", new Person("Bob", 10));
    table.write("2", new Person("Bill", 20));
    table.write("3", new Person("Berry", 30));
    tableManager.flush();

    SparkManager sparkManager = appManager.getSparkManager(DatasetSQLSpark.class.getSimpleName()).start();
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    // The program executes "SELECT * FROM Person WHERE age > 10", hence expected two new entries for Bill and Berry.
    tableManager.flush();

    Person person = table.read("new:2");
    Assert.assertEquals("Bill", person.name());
    Assert.assertEquals(20, person.age());

    person = table.read("new:3");
    Assert.assertEquals("Berry", person.name());
    Assert.assertEquals(30, person.age());

    // Shouldn't have new Bob
    Assert.assertNull(table.read("new:1"));
  }

  @Test
  public void testClassicSpark() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);

    for (Class<?> sparkClass : Arrays.asList(TestSparkApp.ClassicSpark.class, TestSparkApp.ScalaClassicSpark.class)) {
      final SparkManager sparkManager = appManager.getSparkManager(sparkClass.getSimpleName()).start();
      sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
    }

    KeyValueTable resultTable = this.<KeyValueTable>getDataset("ResultTable").get();
    Assert.assertEquals(1L, Bytes.toLong(resultTable.read(ClassicSparkProgram.class.getName())));
    Assert.assertEquals(1L, Bytes.toLong(resultTable.read(ScalaClassicSparkProgram.class.getName())));
  }

  @Test
  public void testDynamicSpark() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);

    // Populate data into the stream
    StreamManager streamManager = getStreamManager("SparkStream");
    for (int i = 0; i < 10; i++) {
      streamManager.send("Line " + (i + 1));
    }

    SparkManager sparkManager = appManager.getSparkManager(ScalaDynamicSpark.class.getSimpleName());
    sparkManager.start(ImmutableMap.of("input", "SparkStream",
                                       "output", "ResultTable",
                                       "tmpdir", TMP_FOLDER.newFolder().getAbsolutePath()));

    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // Validate the result written to dataset
    KeyValueTable resultTable = this.<KeyValueTable>getDataset("ResultTable").get();
    // There should be ten "Line"
    Assert.assertEquals(10, Bytes.toInt(resultTable.read("Line")));
    // Each number should appear once
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(1, Bytes.toInt(resultTable.read(Integer.toString(i + 1))));
    }
  }

  @Test
  public void testStreamFormatSpec() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);

    StreamManager stream = getStreamManager("PeopleStream");
    stream.send("Old Man,50");
    stream.send("Baby,1");
    stream.send("Young Guy,18");
    stream.send("Small Kid,5");
    stream.send("Legal Drinker,21");

    Map<String, String> outputArgs = new HashMap<>();
    FileSetArguments.setOutputPath(outputArgs, "output");

    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.putAll(RuntimeArguments.addScope(Scope.DATASET, "PeopleFileSet", outputArgs));
    runtimeArgs.put("stream.name", "PeopleStream");
    runtimeArgs.put("output.dataset", "PeopleFileSet");
    runtimeArgs.put("sql.statement", "SELECT name, age FROM people WHERE age >= 21");

    List<String> programs = Arrays.asList(
      ScalaStreamFormatSpecSpark.class.getSimpleName(),
      StreamFormatSpecSpark.class.getSimpleName()
    );
    for (String sparkProgramName : programs) {
      // Clean the output before starting
      DataSetManager<FileSet> fileSetManager = getDataset("PeopleFileSet");
      Location outputDir = fileSetManager.get().getLocation("output");
      outputDir.delete(true);

      SparkManager sparkManager = appManager.getSparkManager(sparkProgramName);
      sparkManager.start(runtimeArgs);

      sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 180, TimeUnit.SECONDS);

      // Find the output part file. There is only one because the program repartition to 1
      Location outputFile = Iterables.find(outputDir.list(), new Predicate<Location>() {
        @Override
        public boolean apply(Location input) {
          return input.getName().startsWith("part-r-");
        }
      });

      // Verify the result
      List<String> lines = CharStreams.readLines(CharStreams.newReaderSupplier(Locations.newInputSupplier(outputFile),
                                                                               Charsets.UTF_8));
      Map<String, Integer> result = new HashMap<>();
      for (String line : lines) {
        String[] parts = line.split(":");
        result.put(parts[0], Integer.parseInt(parts[1]));
      }
      Assert.assertEquals(ImmutableMap.of("Old Man", 50, "Legal Drinker", 21), result);
    }
  }

  @Test
  public void testPySpark() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);

    // Write something to the stream
    StreamManager streamManager = getStreamManager("SparkStream");
    for (int i = 0; i < 100; i++) {
      streamManager.send("Event " + i);
    }

    File outputDir = new File(TMP_FOLDER.newFolder(), "output");
    SparkManager sparkManager = appManager.getSparkManager(PythonSpark.class.getSimpleName())
      .start(ImmutableMap.of("input.stream", "SparkStream", "output.path", outputDir.getAbsolutePath()));
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    // Verify the result
    File resultFile = Iterables.find(DirUtils.listFiles(outputDir), new Predicate<File>() {
      @Override
      public boolean apply(File input) {
        return !input.getName().endsWith(".crc") && !input.getName().startsWith("_SUCCESS");
      }
    });


    List<String> lines = Files.readAllLines(resultFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertTrue(!lines.isEmpty());

    // Expected only even number
    int count = 0;
    for (String line : lines) {
      line = line.trim();
      if (!line.isEmpty()) {
        Assert.assertEquals("Event " + count, line);
        count += 2;
      }
    }

    Assert.assertEquals(100, count);

    final Map<String, String> tags = ImmutableMap.of(
      Constants.Metrics.Tag.NAMESPACE, NamespaceId.DEFAULT.getNamespace(),
      Constants.Metrics.Tag.APP, TestSparkApp.class.getSimpleName(),
      Constants.Metrics.Tag.SPARK, PythonSpark.class.getSimpleName());

    Tasks.waitFor(100L, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return getMetricsManager().getTotalMetric(tags, "user.body");
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }


  @Test
  public void testSparkProgramStatusSchedule() throws Exception {
    ApplicationManager appManager = deploy(TestSparkApp.class);
    ScheduleId scheduleId = new ScheduleId(NamespaceId.DEFAULT.getNamespace(), TestSparkApp.class.getSimpleName(),
                                           "schedule");
    appManager.enableSchedule(scheduleId);

    // Start the upstream program
    appManager.getSparkManager(TestSparkApp.ScalaClassicSpark.class.getSimpleName()).start();

    // Wait for the downstream to complete
    WorkflowManager workflowManager =
      appManager.getWorkflowManager(TestSparkApp.TriggeredWorkflow.class.getSimpleName());
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // Run again with the kryo serializer
    appManager.getSparkManager(TestSparkApp.ScalaClassicSpark.class.getSimpleName())
      .start(Collections.singletonMap("spark.serializer", "org.apache.spark.serializer.KryoSerializer"));

    // Wait for the downstream to complete again
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.MINUTES);

  }

  @Test
  public void testSparkWithObjectStore() throws Exception {
    ApplicationManager applicationManager = deploy(SparkAppUsingObjectStore.class);

    DataSetManager<ObjectStore<String>> keysManager = getDataset("keys");
    prepareInputData(keysManager);

    SparkManager sparkManager = applicationManager.getSparkManager(CharCountProgram.class.getSimpleName()).start();
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> countManager = getDataset("count");
    checkOutputData(countManager);

    // validate that the table emitted metrics
    // one read + one write in beforeSubmit(), increment (= read + write) in main -> 4
    Tasks.waitFor(4L, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        Collection<MetricTimeSeries> metrics =
          getMetricsManager().query(new MetricDataQuery(
            0,
            System.currentTimeMillis() / 1000L,
            Integer.MAX_VALUE,
            "system." + Constants.Metrics.Name.Dataset.OP_COUNT,
            AggregationFunction.SUM,
            ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, DefaultId.NAMESPACE.getNamespace(),
                            Constants.Metrics.Tag.APP, SparkAppUsingObjectStore.class.getSimpleName(),
                            Constants.Metrics.Tag.SPARK, CharCountProgram.class.getSimpleName(),
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
    ApplicationManager applicationManager = deploy(SparkAppUsingObjectStore.class);

    DataSetManager<ObjectStore<String>> keysManager = getDataset("keys");
    prepareInputData(keysManager);

    SparkManager sparkManager = applicationManager.getSparkManager(ScalaCharCountProgram.class.getSimpleName()).start();
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> countManager = getDataset("count");
    checkOutputData(countManager);
  }

  @Test
  public void testScalaSparkCrossNSStream() throws Exception {
    // create a namespace for stream and create the stream in it
    NamespaceMeta crossNSStreamMeta = new NamespaceMeta.Builder().setName("streamSpaceForSpark").build();
    getNamespaceAdmin().create(crossNSStreamMeta);
    StreamManager streamManager = getStreamManager(crossNSStreamMeta.getNamespaceId().stream("testStream"));

    // create a namespace for dataset and add the dataset instance in it
    NamespaceMeta crossNSDatasetMeta = new NamespaceMeta.Builder().setName("crossNSDataset").build();
    getNamespaceAdmin().create(crossNSDatasetMeta);
    addDatasetInstance(crossNSDatasetMeta.getNamespaceId().dataset("count"), "keyValueTable");

    // write something to the stream
    streamManager.createStream();
    for (int i = 0; i < 50; i++) {
      streamManager.send(String.valueOf(i));
    }

    // deploy the spark app in another namespace (default)
    ApplicationManager applicationManager = deploy(SparkAppUsingObjectStore.class);

    Map<String, String> args = ImmutableMap.of(ScalaCrossNSProgram.STREAM_NAMESPACE(),
                                               crossNSStreamMeta.getNamespaceId().getNamespace(),
                                               ScalaCrossNSProgram.DATASET_NAMESPACE(),
                                               crossNSDatasetMeta.getNamespaceId().getNamespace(),
                                               ScalaCrossNSProgram.DATASET_NAME(), "count");
    SparkManager sparkManager =
      applicationManager.getSparkManager(ScalaCrossNSProgram.class.getSimpleName()).start(args);
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    // get the dataset from the other namespace where we expect it to exist and compare the data
    DataSetManager<KeyValueTable> countManager = getDataset(crossNSDatasetMeta.getNamespaceId().dataset("count"));
    KeyValueTable results = countManager.get();
    for (int i = 0; i < 50; i++) {
      byte[] key = String.valueOf(i).getBytes(Charsets.UTF_8);
      Assert.assertArrayEquals(key, results.read(key));
    }
  }

  @Test
  public void testScalaSparkCrossNSDataset() throws Exception {
    // Deploy and create a dataset in namespace datasetSpaceForSpark
    NamespaceMeta inputDSNSMeta = new NamespaceMeta.Builder().setName("datasetSpaceForSpark").build();
    getNamespaceAdmin().create(inputDSNSMeta);
    deploy(inputDSNSMeta.getNamespaceId(), SparkAppUsingObjectStore.class);
    DataSetManager<ObjectStore<String>> keysManager = getDataset(inputDSNSMeta.getNamespaceId().dataset("keys"));
    prepareInputData(keysManager);

    Map<String, String> args = ImmutableMap.of(ScalaCharCountProgram.INPUT_DATASET_NAMESPACE(),
                                               inputDSNSMeta.getNamespaceId().getNamespace(),
                                               ScalaCharCountProgram.INPUT_DATASET_NAME(), "keys");

    ApplicationManager applicationManager = deploy(SparkAppUsingObjectStore.class);
    SparkManager sparkManager =
      applicationManager.getSparkManager(ScalaCharCountProgram.class.getSimpleName()).start(args);
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> countManager = getDataset("count");
    checkOutputData(countManager);
  }


  @Test
  public void testTransaction() throws Exception {
    ApplicationManager applicationManager = deploy(TestSparkApp.class);
    StreamManager streamManager = getStreamManager("SparkStream");

    // Write some sentences to the stream
    streamManager.send("red fox");
    streamManager.send("brown fox");
    streamManager.send("grey fox");
    streamManager.send("brown bear");
    streamManager.send("black bear");

    // Run the spark program
    SparkManager sparkManager = applicationManager.getSparkManager(TransactionSpark.class.getSimpleName());
    sparkManager.start(ImmutableMap.of(
      "source.stream", "SparkStream",
      "keyvalue.table", "KeyValueTable",
      "result.all.dataset", "SparkResult",
      "result.threshold", "2",
      "result.threshold.dataset", "SparkThresholdResult"
    ));

    // Verify result from dataset before the Spark program terminates
    final DataSetManager<KeyValueTable> resultManager = getDataset("SparkThresholdResult");
    final KeyValueTable resultTable = resultManager.get();

    // Expect the threshold result dataset, with threshold >=2, contains [brown, fox, bear]
    Tasks.waitFor(ImmutableSet.of("brown", "fox", "bear"), new Callable<Set<String>>() {
      @Override
      public Set<String> call() throws Exception {
        resultManager.flush();    // This is to start a new TX
        LOG.info("Reading from threshold result");
        try (CloseableIterator<KeyValue<byte[], byte[]>> itor = resultTable.scan(null, null)) {
          return ImmutableSet.copyOf(Iterators.transform(itor, new Function<KeyValue<byte[], byte[]>, String>() {
            @Override
            public String apply(KeyValue<byte[], byte[]> input) {
              String word = Bytes.toString(input.getKey());
              LOG.info("{}, {}", word, Bytes.toInt(input.getValue()));
              return word;
            }
          }));
        }
      }
    }, 3, TimeUnit.MINUTES, 1, TimeUnit.SECONDS);

    sparkManager.stop();
    sparkManager.waitForRun(ProgramRunStatus.KILLED, 60, TimeUnit.SECONDS);
  }

  @Test
  public void testSparkWithGetDataset() throws Exception {
    testSparkWithGetDataset(SparkAppUsingGetDataset.class, SparkLogParser.class.getSimpleName());
    testSparkWithGetDataset(SparkAppUsingGetDataset.class, ScalaSparkLogParser.class.getSimpleName());
  }

  @Test
  public void testSparkWithLocalFiles() throws Exception {
    testSparkWithLocalFiles(SparkAppUsingLocalFiles.class,
                            SparkAppUsingLocalFiles.JavaSparkUsingLocalFiles.class.getSimpleName(), "java");
    testSparkWithLocalFiles(SparkAppUsingLocalFiles.class,
                            SparkAppUsingLocalFiles.ScalaSparkUsingLocalFiles.class.getSimpleName(), "scala");
  }

  private void prepareInputData(DataSetManager<ObjectStore<String>> manager) {
    ObjectStore<String> keys = manager.get();
    keys.write(Bytes.toBytes(TEST_STRING_1), TEST_STRING_1);
    keys.write(Bytes.toBytes(TEST_STRING_2), TEST_STRING_2);
    manager.flush();
  }

  private void checkOutputData(DataSetManager<KeyValueTable> manager) {
    KeyValueTable count = manager.get();
    //read output and verify result
    byte[] val = count.read(Bytes.toBytes(TEST_STRING_1));
    Assert.assertTrue(val != null);
    Assert.assertEquals(Bytes.toInt(val), TEST_STRING_1.length());

    val = count.read(Bytes.toBytes(TEST_STRING_2));
    Assert.assertTrue(val != null);
    Assert.assertEquals(Bytes.toInt(val), TEST_STRING_2.length());
  }

  private void testSparkWithGetDataset(Class<? extends Application> appClass, String sparkProgram) throws Exception {
    ApplicationManager applicationManager = deploy(appClass);

    DataSetManager<FileSet> filesetManager = getDataset("logs");
    FileSet fileset = filesetManager.get();
    Location location = fileset.getLocation("nn");
    prepareInputFileSetWithLogData(location);

    Map<String, String> inputArgs = new HashMap<>();
    FileSetArguments.setInputPath(inputArgs, "nn");
    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "logs", inputArgs));
    args.put("input", "logs");
    args.put("output", "logStats");

    SparkManager sparkManager = applicationManager.getSparkManager(sparkProgram).start(args);
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> logStatsManager = getDataset("logStats");
    KeyValueTable logStatsTable = logStatsManager.get();
    validateGetDatasetOutput(logStatsTable);

    // Cleanup after run
    location.delete(true);
    logStatsManager.flush();
    try (CloseableIterator<KeyValue<byte[], byte[]>> scan = logStatsTable.scan(null, null)) {
      while (scan.hasNext()) {
        logStatsTable.delete(scan.next().getKey());
      }
    }
    logStatsManager.flush();
  }

  private void testSparkWithLocalFiles(Class<? extends Application> appClass,
                                       String sparkProgram, String prefix) throws Exception {
    ApplicationManager applicationManager = deploy(appClass);
    URI localFile = createLocalPropertiesFile(prefix);

    SparkManager sparkManager = applicationManager.getSparkManager(sparkProgram)
      .start(Collections.singletonMap(SparkAppUsingLocalFiles.LOCAL_FILE_RUNTIME_ARG, localFile.toString()));
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> kvTableManager = getDataset(SparkAppUsingLocalFiles.OUTPUT_DATASET_NAME);
    KeyValueTable kvTable = kvTableManager.get();
    Map<String, String> expected = ImmutableMap.of("a", "1", "b", "2", "c", "3");
    List<byte[]> deleteKeys = new ArrayList<>();
    try (CloseableIterator<KeyValue<byte[], byte[]>> scan = kvTable.scan(null, null)) {
      for (int i = 0; i < 3; i++) {
        KeyValue<byte[], byte[]> next = scan.next();
        Assert.assertEquals(expected.get(Bytes.toString(next.getKey())), Bytes.toString(next.getValue()));
        deleteKeys.add(next.getKey());
      }
      Assert.assertFalse(scan.hasNext());
    }

    // Cleanup after run
    kvTableManager.flush();
    for (byte[] key : deleteKeys) {
      kvTable.delete(key);
    }
    kvTableManager.flush();
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

    try (CloseableIterator<KeyValue<byte[], byte[]>> scan = logStatsTable.scan(null, null)) {
      // must have 4 records
      for (int i = 0; i < 4; i++) {
        Assert.assertTrue("Expected next for i = " + i, scan.hasNext());
        KeyValue<byte[], byte[]> next = scan.next();
        SparkAppUsingGetDataset.LogKey logKey =
          new Gson().fromJson(Bytes.toString(next.getKey()), SparkAppUsingGetDataset.LogKey.class);
        SparkAppUsingGetDataset.LogStats logStats =
          new Gson().fromJson(Bytes.toString(next.getValue()), SparkAppUsingGetDataset.LogStats.class);
        Assert.assertEquals(expected.get(logKey), logStats);
      }
      // no more records
      Assert.assertFalse(scan.hasNext());
    }
  }

  private URI createLocalPropertiesFile(String filePrefix) throws IOException {
    File file = TMP_FOLDER.newFile(filePrefix + "-local.properties");
    try (OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(file))) {
      out.write("a=1\n");
      out.write("b = 2\n");
      out.write("c= 3");
    }
    return file.toURI();
  }
}

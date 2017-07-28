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

package co.cask.cdap.datastreams;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.aggregator.FieldCountAggregator;
import co.cask.cdap.etl.mock.batch.aggregator.GroupFilterAggregator;
import co.cask.cdap.etl.mock.batch.joiner.DupeFlagger;
import co.cask.cdap.etl.mock.batch.joiner.MockJoiner;
import co.cask.cdap.etl.mock.spark.Window;
import co.cask.cdap.etl.mock.spark.compute.StringValueFilterCompute;
import co.cask.cdap.etl.mock.spark.streaming.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.mock.transform.FilterErrorTransform;
import co.cask.cdap.etl.mock.transform.FlattenErrorTransform;
import co.cask.cdap.etl.mock.transform.IdentityTransform;
import co.cask.cdap.etl.mock.transform.NullFieldSplitterTransform;
import co.cask.cdap.etl.mock.transform.SleepTransform;
import co.cask.cdap.etl.mock.transform.StringValueFilterTransform;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.spark.Compat;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MetricsManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class DataStreamsTest extends HydratorTestBase {

  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
  private static int startCount = 0;
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupStreamingArtifacts(APP_ARTIFACT_ID, DataStreamsApp.class);
  }

  @Test
  public void testTransformComputeWithMacros() throws Exception {
    Schema schema = Schema.recordOf(
      "test",
      Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );
    List<StructuredRecord> input = new ArrayList<>();
    StructuredRecord samuelRecord = StructuredRecord.builder(schema).set("id", "123").set("name", "samuel").build();
    StructuredRecord jacksonRecord = StructuredRecord.builder(schema).set("id", "456").set("name", "jackson").build();
    StructuredRecord dwayneRecord = StructuredRecord.builder(schema).set("id", "789").set("name", "dwayne").build();
    StructuredRecord johnsonRecord = StructuredRecord.builder(schema).set("id", "0").set("name", "johnson").build();
    input.add(samuelRecord);
    input.add(jacksonRecord);
    input.add(dwayneRecord);
    input.add(johnsonRecord);

    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(schema, input)))
      .addStage(new ETLStage("sink", MockSink.getPlugin("${output}")))
      .addStage(new ETLStage("filter1", StringValueFilterTransform.getPlugin("${field}", "${val1}")))
      .addStage(new ETLStage("filter2", StringValueFilterCompute.getPlugin("${field}", "${val2}")))
      .addStage(new ETLStage("sleep", SleepTransform.getPlugin(2L)))
      .addConnection("source", "sleep")
      .addConnection("sleep", "filter1")
      .addConnection("filter1", "filter2")
      .addConnection("filter2", "sink")
      .setBatchInterval("1s")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("simpleApp");
    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    final Set<StructuredRecord> expected = new HashSet<>();
    expected.add(samuelRecord);
    expected.add(jacksonRecord);

    testTransformComputeRun(appManager, expected, "dwayne", "johnson", "macroOutput1");
    validateMetric(appId, "source.records.out", 4);
    validateMetric(appId, "sleep.records.in", 4);
    validateMetric(appId, "sleep.records.out", 4);
    validateMetric(appId, "filter1.records.in", 4);
    validateMetric(appId, "filter1.records.out", 3);
    validateMetric(appId, "filter2.records.in", 3);
    validateMetric(appId, "filter2.records.out", 2);
    validateMetric(appId, "sink.records.in", 2);
    Assert.assertTrue(getMetric(appId, "sleep." + co.cask.cdap.etl.common.Constants.Metrics.TOTAL_TIME) > 0L);

    expected.clear();
    expected.add(dwayneRecord);
    expected.add(johnsonRecord);
    testTransformComputeRun(appManager, expected, "samuel", "jackson", "macroOutput2");
  }

  private void testTransformComputeRun(ApplicationManager appManager, final Set<StructuredRecord> expected,
                                       String val1, String val2, final String outputName) throws Exception {
    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start(ImmutableMap.of("field", "name", "val1", val1, "val2", val2, "output", outputName));
    sparkManager.waitForStatus(true, 10, 1);

    // since dataset name is a macro, the dataset isn't created until it is needed. Wait for it to exist
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return getDataset(outputName).get() != null;
      }
    }, 1, TimeUnit.MINUTES);

    final DataSetManager<Table> outputManager = getDataset(outputName);
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          Set<StructuredRecord> outputRecords = new HashSet<>();
          outputRecords.addAll(MockSink.readOutput(outputManager));
          return expected.equals(outputRecords);
        }
      },
      1,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStatus(false, 10, 1);
  }

  @Test
  public void testAggregatorJoinerMacrosWithCheckpoints() throws Exception {
    /*
                 |--> aggregator --> sink1
        users1 --|
                 |----|
                      |--> dupeFlagger --> sink2
        users2 -------|
     */
    Schema userSchema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));


    List<StructuredRecord> users1 = ImmutableList.of(
      StructuredRecord.builder(userSchema).set("id", 1L).set("name", "Samuel").build(),
      StructuredRecord.builder(userSchema).set("id", 2L).set("name", "Dwayne").build(),
      StructuredRecord.builder(userSchema).set("id", 3L).set("name", "Terry").build());

    List<StructuredRecord> users2 = ImmutableList.of(
      StructuredRecord.builder(userSchema).set("id", 1L).set("name", "Samuel").build(),
      StructuredRecord.builder(userSchema).set("id", 2L).set("name", "Dwayne").build(),
      StructuredRecord.builder(userSchema).set("id", 4L).set("name", "Terry").build(),
      StructuredRecord.builder(userSchema).set("id", 5L).set("name", "Christopher").build());

    DataStreamsConfig pipelineConfig = DataStreamsConfig.builder()
      .setBatchInterval("5s")
      .addStage(new ETLStage("users1", MockSource.getPlugin(userSchema, users1)))
      .addStage(new ETLStage("users2", MockSource.getPlugin(userSchema, users2)))
      .addStage(new ETLStage("sink1", MockSink.getPlugin("sink1")))
      .addStage(new ETLStage("sink2", MockSink.getPlugin("sink2")))
      .addStage(new ETLStage("aggregator", FieldCountAggregator.getPlugin("${aggfield}", "${aggType}")))
      .addStage(new ETLStage("dupeFlagger", DupeFlagger.getPlugin("users1", "${flagField}")))
      .addConnection("users1", "aggregator")
      .addConnection("aggregator", "sink1")
      .addConnection("users1", "dupeFlagger")
      .addConnection("users2", "dupeFlagger")
      .addConnection("dupeFlagger", "sink2")
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, pipelineConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ParallelAggApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // run it once with this set of macros
    Map<String, String> arguments = new HashMap<>();
    arguments.put("aggfield", "id");
    arguments.put("aggType", "long");
    arguments.put("flagField", "isDupe");

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start(arguments);
    sparkManager.waitForStatus(true, 10, 1);

    final DataSetManager<Table> sink1 = getDataset("sink1");
    final DataSetManager<Table> sink2 = getDataset("sink2");

    Schema aggSchema = Schema.recordOf(
      "user.count",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("ct", Schema.of(Schema.Type.LONG)));
    final Set<StructuredRecord> expectedAggregates = ImmutableSet.of(
      StructuredRecord.builder(aggSchema).set("id", 0L).set("ct", 3L).build(),
      StructuredRecord.builder(aggSchema).set("id", 1L).set("ct", 1L).build(),
      StructuredRecord.builder(aggSchema).set("id", 2L).set("ct", 1L).build(),
      StructuredRecord.builder(aggSchema).set("id", 3L).set("ct", 1L).build());

    Schema outputSchema = Schema.recordOf(
      "user.flagged",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("isDupe", Schema.of(Schema.Type.BOOLEAN)));
    final Set<StructuredRecord> expectedJoined = ImmutableSet.of(
      StructuredRecord.builder(outputSchema).set("id", 1L).set("name", "Samuel").set("isDupe", true).build(),
      StructuredRecord.builder(outputSchema).set("id", 2L).set("name", "Dwayne").set("isDupe", true).build(),
      StructuredRecord.builder(outputSchema).set("id", 3L).set("name", "Terry").set("isDupe", false).build());

    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          sink1.flush();
          sink2.flush();
          Set<StructuredRecord> actualAggs = new HashSet<>();
          Set<StructuredRecord> actualJoined = new HashSet<>();
          actualAggs.addAll(MockSink.readOutput(sink1));
          actualJoined.addAll(MockSink.readOutput(sink2));
          return expectedAggregates.equals(actualAggs) && expectedJoined.equals(actualJoined);
        }
      },
      1,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStatus(false, 30, 1);

    MockSink.clear(sink1);
    MockSink.clear(sink2);

    // run it again with different macros to make sure they are re-evaluated and not stored in the checkpoint
    arguments = new HashMap<>();
    arguments.put("aggfield", "name");
    arguments.put("aggType", "string");
    arguments.put("flagField", "dupe");

    sparkManager.start(arguments);
    sparkManager.waitForStatus(true, 10, 1);

    aggSchema = Schema.recordOf(
      "user.count",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("ct", Schema.of(Schema.Type.LONG)));
    final Set<StructuredRecord> expectedAggregates2 = ImmutableSet.of(
      StructuredRecord.builder(aggSchema).set("name", "all").set("ct", 3L).build(),
      StructuredRecord.builder(aggSchema).set("name", "Samuel").set("ct", 1L).build(),
      StructuredRecord.builder(aggSchema).set("name", "Dwayne").set("ct", 1L).build(),
      StructuredRecord.builder(aggSchema).set("name", "Terry").set("ct", 1L).build());

    outputSchema = Schema.recordOf(
      "user.flagged",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("dupe", Schema.of(Schema.Type.BOOLEAN)));
    final Set<StructuredRecord> expectedJoined2 = ImmutableSet.of(
      StructuredRecord.builder(outputSchema).set("id", 1L).set("name", "Samuel").set("dupe", true).build(),
      StructuredRecord.builder(outputSchema).set("id", 2L).set("name", "Dwayne").set("dupe", true).build(),
      StructuredRecord.builder(outputSchema).set("id", 3L).set("name", "Terry").set("dupe", false).build());

    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          sink1.flush();
          sink2.flush();
          Set<StructuredRecord> actualAggs = new HashSet<>();
          Set<StructuredRecord> actualJoined = new HashSet<>();
          actualAggs.addAll(MockSink.readOutput(sink1));
          actualJoined.addAll(MockSink.readOutput(sink2));
          return expectedAggregates2.equals(actualAggs) && expectedJoined2.equals(actualJoined);
        }
      },
      1,
      TimeUnit.MINUTES);

    sparkManager.stop();
  }

  @Test
  public void testParallelAggregators() throws Exception {
    String sink1Name = "pAggOutput1";
    String sink2Name = "pAggOutput2";

    Schema inputSchema = Schema.recordOf(
      "testRecord",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item", Schema.of(Schema.Type.LONG))
    );

    List<StructuredRecord> input1 = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 1L).build(),
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 2L).build());

    List<StructuredRecord> input2 = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("user", "samuel").set("item", 3L).build(),
      StructuredRecord.builder(inputSchema).set("user", "john").set("item", 4L).build(),
      StructuredRecord.builder(inputSchema).set("user", "john").set("item", 3L).build());

    /*
       source1 --|--> agg1 --> sink1
                 |
       source2 --|--> agg2 --> sink2
     */
    DataStreamsConfig pipelineConfig = DataStreamsConfig.builder()
      .setBatchInterval("5s")
      .addStage(new ETLStage("source1", MockSource.getPlugin(inputSchema, input1)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(inputSchema, input2)))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1Name)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2Name)))
      .addStage(new ETLStage("agg1", FieldCountAggregator.getPlugin("user", "string")))
      .addStage(new ETLStage("agg2", FieldCountAggregator.getPlugin("item", "long")))
      .addConnection("source1", "agg1")
      .addConnection("source1", "agg2")
      .addConnection("source2", "agg1")
      .addConnection("source2", "agg2")
      .addConnection("agg1", "sink1")
      .addConnection("agg2", "sink2")
      .disableCheckpoints()
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, pipelineConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ParallelAggApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    Schema outputSchema1 = Schema.recordOf(
      "user.count",
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("ct", Schema.of(Schema.Type.LONG))
    );
    Schema outputSchema2 = Schema.recordOf(
      "item.count",
      Schema.Field.of("item", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("ct", Schema.of(Schema.Type.LONG))
    );

    // check output
    final DataSetManager<Table> sinkManager1 = getDataset(sink1Name);
    final Set<StructuredRecord> expected1 = ImmutableSet.of(
      StructuredRecord.builder(outputSchema1).set("user", "all").set("ct", 5L).build(),
      StructuredRecord.builder(outputSchema1).set("user", "samuel").set("ct", 3L).build(),
      StructuredRecord.builder(outputSchema1).set("user", "john").set("ct", 2L).build());

    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          sinkManager1.flush();
          Set<StructuredRecord> outputRecords = new HashSet<>();
          outputRecords.addAll(MockSink.readOutput(sinkManager1));
          return expected1.equals(outputRecords);
        }
      },
      1,
      TimeUnit.MINUTES);

    final DataSetManager<Table> sinkManager2 = getDataset(sink2Name);
    final Set<StructuredRecord> expected2 = ImmutableSet.of(
      StructuredRecord.builder(outputSchema2).set("item", 0L).set("ct", 5L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 1L).set("ct", 1L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 2L).set("ct", 1L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 3L).set("ct", 2L).build(),
      StructuredRecord.builder(outputSchema2).set("item", 4L).set("ct", 1L).build());

    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          sinkManager2.flush();
          Set<StructuredRecord> outputRecords = new HashSet<>();
          outputRecords.addAll(MockSink.readOutput(sinkManager2));
          return expected2.equals(outputRecords);
        }
      },
      1,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStatus(false, 10, 1);

    validateMetric(appId, "source1.records.out", 2);
    validateMetric(appId, "source2.records.out", 3);
    validateMetric(appId, "agg1.records.in", 5);
    validateMetric(appId, "agg1.records.out", 3);
    validateMetric(appId, "agg2.records.in", 5);
    validateMetric(appId, "agg2.records.out", 5);
    validateMetric(appId, "sink1.records.in", 3);
    validateMetric(appId, "sink2.records.in", 5);
  }

  @Test
  public void testWindower() throws Exception {
    /*
     * source --> window(width=10,interval=1) --> aggregator --> filter --> sink
     */
    Schema schema = Schema.recordOf("data", Schema.Field.of("x", Schema.of(Schema.Type.STRING)));
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(schema).set("x", "abc").build(),
      StructuredRecord.builder(schema).set("x", "abc").build(),
      StructuredRecord.builder(schema).set("x", "abc").build());

    String sinkName = "windowOut";
    // source sleeps 1 second between outputs
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(schema, input, 1000L)))
      .addStage(new ETLStage("window", Window.getPlugin(30, 1)))
      .addStage(new ETLStage("agg", FieldCountAggregator.getPlugin("x", "string")))
      .addStage(new ETLStage("filter", StringValueFilterTransform.getPlugin("x", "all")))
      .addStage(new ETLStage("sink", MockSink.getPlugin(sinkName)))
      .addConnection("source", "window")
      .addConnection("window", "agg")
      .addConnection("agg", "filter")
      .addConnection("filter", "sink")
      .setBatchInterval("1s")
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("WindowerApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    // the sink should contain at least one record with count of 3, and no records with more than 3.
    // less than 3 if the window doesn't contain all 3 records yet, but there should eventually be a window
    // that contains all 3.
    final DataSetManager<Table> outputManager = getDataset(sinkName);
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          boolean sawThree = false;
          for (StructuredRecord record : MockSink.readOutput(outputManager)) {
            long count = record.get("ct");
            if (count == 3L) {
              sawThree = true;
            }
            Assert.assertTrue(count <= 3L);
          }
          return sawThree;
        }
      },
      2,
      TimeUnit.MINUTES);

    sparkManager.stop();
  }

  @Test
  public void testJoin() throws Exception {
    /*
     * source1 ----> t1 ------
     *                        | --> innerjoin ----> t4 ------
     * source2 ----> t2 ------                                 |
     *                                                         | ---> outerjoin --> sink1
     *                                                         |
     * source3 -------------------- t3 ------------------------
     */

    Schema inputSchema1 = Schema.recordOf(
      "customerRecord",
      Schema.Field.of("customer_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("customer_name", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema2 = Schema.recordOf(
      "itemRecord",
      Schema.Field.of("item_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("item_price", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("cust_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("cust_name", Schema.of(Schema.Type.STRING))
    );

    Schema inputSchema3 = Schema.recordOf(
      "transactionRecord",
      Schema.Field.of("t_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("i_id", Schema.of(Schema.Type.STRING))
    );

    Schema outSchema2 = Schema.recordOf(
      "join.output",
      Schema.Field.of("t_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("c_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("i_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("customer_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("customer_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("item_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("item_price", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("cust_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("cust_name", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    StructuredRecord recordSamuel = StructuredRecord.builder(inputSchema1).set("customer_id", "1")
      .set("customer_name", "samuel").build();
    StructuredRecord recordBob = StructuredRecord.builder(inputSchema1).set("customer_id", "2")
      .set("customer_name", "bob").build();
    StructuredRecord recordJane = StructuredRecord.builder(inputSchema1).set("customer_id", "3")
      .set("customer_name", "jane").build();

    StructuredRecord recordCar = StructuredRecord.builder(inputSchema2).set("item_id", "11").set("item_price", 10000L)
      .set("cust_id", "1").set("cust_name", "samuel").build();
    StructuredRecord recordBike = StructuredRecord.builder(inputSchema2).set("item_id", "22").set("item_price", 100L)
      .set("cust_id", "3").set("cust_name", "jane").build();

    StructuredRecord recordTrasCar = StructuredRecord.builder(inputSchema3).set("t_id", "1").set("c_id", "1")
      .set("i_id", "11").build();
    StructuredRecord recordTrasBike = StructuredRecord.builder(inputSchema3).set("t_id", "2").set("c_id", "3")
      .set("i_id", "22").build();
    StructuredRecord recordTrasPlane = StructuredRecord.builder(inputSchema3).set("t_id", "3").set("c_id", "4")
      .set("i_id", "33").build();

    List<StructuredRecord> input1 = ImmutableList.of(recordSamuel, recordBob, recordJane);
    List<StructuredRecord> input2 = ImmutableList.of(recordCar, recordBike);
    List<StructuredRecord> input3 = ImmutableList.of(recordTrasCar, recordTrasBike, recordTrasPlane);

    String outputName = "multiJoinOutputSink";
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(new ETLStage("source1", MockSource.getPlugin(inputSchema1, input1)))
      .addStage(new ETLStage("source2", MockSource.getPlugin(inputSchema2, input2)))
      .addStage(new ETLStage("source3", MockSource.getPlugin(inputSchema3, input3)))
      .addStage(new ETLStage("t1", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t2", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t3", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("t4", IdentityTransform.getPlugin()))
      .addStage(new ETLStage("innerjoin", MockJoiner.getPlugin("t1.customer_id=t2.cust_id",
                                                               "t1,t2", "")))
      .addStage(new ETLStage("outerjoin", MockJoiner.getPlugin("t4.item_id=t3.i_id",
                                                                 "", "")))
      .addStage(new ETLStage("multijoinSink", MockSink.getPlugin(outputName)))
      .addConnection("source1", "t1")
      .addConnection("source2", "t2")
      .addConnection("source3", "t3")
      .addConnection("t1", "innerjoin")
      .addConnection("t2", "innerjoin")
      .addConnection("innerjoin", "t4")
      .addConnection("t3", "outerjoin")
      .addConnection("t4", "outerjoin")
      .addConnection("outerjoin", "multijoinSink")
      .setBatchInterval("5s")
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("JoinerApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    StructuredRecord joinRecordSamuel = StructuredRecord.builder(outSchema2)
      .set("customer_id", "1").set("customer_name", "samuel")
      .set("item_id", "11").set("item_price", 10000L).set("cust_id", "1").set("cust_name", "samuel")
      .set("t_id", "1").set("c_id", "1").set("i_id", "11").build();

    StructuredRecord joinRecordJane = StructuredRecord.builder(outSchema2)
      .set("customer_id", "3").set("customer_name", "jane")
      .set("item_id", "22").set("item_price", 100L).set("cust_id", "3").set("cust_name", "jane")
      .set("t_id", "2").set("c_id", "3").set("i_id", "22").build();

    StructuredRecord joinRecordPlane = StructuredRecord.builder(outSchema2)
      .set("t_id", "3").set("c_id", "4").set("i_id", "33").build();
    final Set<StructuredRecord> expected = ImmutableSet.of(joinRecordSamuel, joinRecordJane, joinRecordPlane);

    final DataSetManager<Table> outputManager = getDataset(outputName);
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          outputManager.flush();
          Set<StructuredRecord> outputRecords = new HashSet<>();
          outputRecords.addAll(MockSink.readOutput(outputManager));
          return expected.equals(outputRecords);
        }
      },
      4,
      TimeUnit.MINUTES);

    sparkManager.stop();
    sparkManager.waitForStatus(false, 10, 1);

    validateMetric(appId, "source1.records.out", 3);
    validateMetric(appId, "source2.records.out", 2);
    validateMetric(appId, "source3.records.out", 3);
    validateMetric(appId, "t1.records.in", 3);
    validateMetric(appId, "t1.records.out", 3);
    validateMetric(appId, "t2.records.in", 2);
    validateMetric(appId, "t2.records.out", 2);
    validateMetric(appId, "t3.records.in", 3);
    validateMetric(appId, "t3.records.out", 3);
    validateMetric(appId, "t4.records.in", 2);
    validateMetric(appId, "t4.records.out", 2);
    validateMetric(appId, "innerjoin.records.in", 5);
    validateMetric(appId, "innerjoin.records.out", 2);
    validateMetric(appId, "outerjoin.records.in", 5);
    validateMetric(appId, "outerjoin.records.out", 3);
    validateMetric(appId, "multijoinSink.records.in", 3);
  }

  @Test
  public void testErrorTransform() throws Exception {
    String sink1TableName = "errTestOut1";
    String sink2TableName = "errTestOut2";

    Schema inputSchema = Schema.recordOf("user", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("name", "Leo").build(),
      StructuredRecord.builder(inputSchema).set("name", "Ralph").build(),
      StructuredRecord.builder(inputSchema).set("name", "Don").build(),
      StructuredRecord.builder(inputSchema).set("name", "Mike").build(),
      StructuredRecord.builder(inputSchema).set("name", "April").build());
    /*
     *
     * source--> filter1 --> filter2 --> agg1 --> agg2
     *              |           |         |        |
     *              |-----------|---------|--------|--------|--> flatten errors --> sink1
     *                                                      |
     *                                                      |--> filter errors --> sink2
     * arrows coming out the right represent output records
     * arrows coming out the bottom represent error records
     * this will test multiple stages from multiple phases emitting errors to the same stage
     * as well as errors from one stage going to multiple stages
     */
    DataStreamsConfig config = DataStreamsConfig.builder()
      .setBatchInterval("5s")
      .addStage(new ETLStage("source", MockSource.getPlugin(inputSchema, input)))
      .addStage(new ETLStage("filter1", StringValueFilterTransform.getPlugin("name", "Leo")))
      .addStage(new ETLStage("filter2", StringValueFilterTransform.getPlugin("name", "Ralph")))
      .addStage(new ETLStage("agg1", GroupFilterAggregator.getPlugin("name", "Don")))
      .addStage(new ETLStage("agg2", GroupFilterAggregator.getPlugin("name", "Mike")))
      .addStage(new ETLStage("errorflatten", FlattenErrorTransform.getPlugin()))
      .addStage(new ETLStage("errorfilter", FilterErrorTransform.getPlugin(3)))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1TableName)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2TableName)))
      .addConnection("source", "filter1")
      .addConnection("filter1", "filter2")
      .addConnection("filter2", "agg1")
      .addConnection("agg1", "agg2")
      .addConnection("filter1", "errorflatten")
      .addConnection("filter1", "errorfilter")
      .addConnection("filter2", "errorflatten")
      .addConnection("filter2", "errorfilter")
      .addConnection("agg1", "errorflatten")
      .addConnection("agg1", "errorfilter")
      .addConnection("agg2", "errorflatten")
      .addConnection("agg2", "errorfilter")
      .addConnection("errorflatten", "sink1")
      .addConnection("errorfilter", "sink2")
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("ErrTransformTest");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    Schema flattenSchema =
      Schema.recordOf("erroruser",
                      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("errMsg", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("errCode", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                      Schema.Field.of("errStage", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    final Set<StructuredRecord> expected = ImmutableSet.of(
      StructuredRecord.builder(flattenSchema)
        .set("name", "Leo").set("errMsg", "bad string value").set("errCode", 1).set("errStage", "filter1").build(),
      StructuredRecord.builder(flattenSchema)
        .set("name", "Ralph").set("errMsg", "bad string value").set("errCode", 1).set("errStage", "filter2").build(),
      StructuredRecord.builder(flattenSchema)
        .set("name", "Don").set("errMsg", "bad val").set("errCode", 3).set("errStage", "agg1").build(),
      StructuredRecord.builder(flattenSchema)
        .set("name", "Mike").set("errMsg", "bad val").set("errCode", 3).set("errStage", "agg2").build());
    final DataSetManager<Table> sink1Table = getDataset(sink1TableName);
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          sink1Table.flush();
          Set<StructuredRecord> outputRecords = new HashSet<>();
          outputRecords.addAll(MockSink.readOutput(sink1Table));
          return expected.equals(outputRecords);
        }
      },
      4,
      TimeUnit.MINUTES);

    final Set<StructuredRecord> expected2 = ImmutableSet.of(
      StructuredRecord.builder(inputSchema).set("name", "Leo").build(),
      StructuredRecord.builder(inputSchema).set("name", "Ralph").build());
    final DataSetManager<Table> sink2Table = getDataset(sink2TableName);
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          sink2Table.flush();
          Set<StructuredRecord> outputRecords = new HashSet<>();
          outputRecords.addAll(MockSink.readOutput(sink2Table));
          return expected2.equals(outputRecords);
        }
      },
      4,
      TimeUnit.MINUTES);
  }

  @Test
  public void testSplitterTransform() throws Exception {
    Schema schema = Schema.recordOf("user",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    StructuredRecord user0 = StructuredRecord.builder(schema).set("id", 0L).build();
    StructuredRecord user1 = StructuredRecord.builder(schema).set("id", 1L).set("email", "one@example.com").build();
    StructuredRecord user2 = StructuredRecord.builder(schema).set("id", 2L).set("name", "two").build();
    StructuredRecord user3 = StructuredRecord.builder(schema)
      .set("id", 3L).set("name", "three").set("email", "three@example.com").build();

    String sink1Name = "splitSink1";
    String sink2Name = "splitSink2";

    /*
     *
     *                                            |null --> sink1
     *                       |null--> splitter2 --|
     * source --> splitter1--|                    |non-null --|
     *                       |                                |--> sink2
     *                       |non-null------------------------|
     */
    DataStreamsConfig config = DataStreamsConfig.builder()
      .setBatchInterval("5s")
      .addStage(new ETLStage("source", MockSource.getPlugin(schema, ImmutableList.of(user0, user1, user2, user3))))
      .addStage(new ETLStage("splitter1", NullFieldSplitterTransform.getPlugin("name")))
      .addStage(new ETLStage("splitter2", NullFieldSplitterTransform.getPlugin("email")))
      .addStage(new ETLStage("sink1", MockSink.getPlugin(sink1Name)))
      .addStage(new ETLStage("sink2", MockSink.getPlugin(sink2Name)))
      .addConnection("source", "splitter1")
      .addConnection("splitter1", "splitter2", "null")
      .addConnection("splitter1", "sink2", "non-null")
      .addConnection("splitter2", "sink1", "null")
      .addConnection("splitter2", "sink2", "non-null")
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app("SplitterTest");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // run pipeline
    SparkManager sparkManager = appManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    sparkManager.start();
    sparkManager.waitForStatus(true, 10, 1);

    // check output
    // sink1 should only have records where both name and email are null (user0)
    final DataSetManager<Table> sink1Manager = getDataset(sink1Name);
    final Set<StructuredRecord> expected1 = ImmutableSet.of(user0);
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          sink1Manager.flush();
          Set<StructuredRecord> outputRecords = new HashSet<>();
          outputRecords.addAll(MockSink.readOutput(sink1Manager));
          return expected1.equals(outputRecords);
        }
      },
      4,
      TimeUnit.MINUTES);

    // sink2 should have anything with a non-null name or non-null email
    final DataSetManager<Table> sink2Manager = getDataset(sink2Name);
    final Set<StructuredRecord> expected2 = ImmutableSet.of(user1, user2, user3);
    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          sink2Manager.flush();
          Set<StructuredRecord> outputRecords = new HashSet<>();
          outputRecords.addAll(MockSink.readOutput(sink2Manager));
          return expected2.equals(outputRecords);
        }
      },
      4,
      TimeUnit.MINUTES);

    validateMetric(appId, "source.records.out", 4);
    validateMetric(appId, "splitter1.records.in", 4);
    validateMetric(appId, "splitter1.records.out.non-null", 2);
    validateMetric(appId, "splitter1.records.out.null", 2);
    validateMetric(appId, "splitter2.records.in", 2);
    validateMetric(appId, "splitter2.records.out.non-null", 1);
    validateMetric(appId, "splitter2.records.out.null", 1);
    validateMetric(appId, "sink1.records.in", 1);
    validateMetric(appId, "sink2.records.in", 3);
  }

  private void validateMetric(ApplicationId appId, String metric,
                              long expected) throws TimeoutException, InterruptedException {
    MetricsManager metricsManager = getMetricsManager();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName(),
                                               Constants.Metrics.Tag.SPARK, DataStreamsSparkLauncher.NAME);
    String metricName = "user." + metric;
    metricsManager.waitForTotalMetricCount(tags, metricName, expected, 10, TimeUnit.SECONDS);
    metricsManager.waitForTotalMetricCount(tags, metricName, expected, 10, TimeUnit.SECONDS);
    // wait for won't throw an exception if the metric count is greater than expected
    Assert.assertEquals(expected, metricsManager.getTotalMetric(tags, metricName));
  }

  private long getMetric(ApplicationId appId, String metric) {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName(),
                                               Constants.Metrics.Tag.SPARK, DataStreamsSparkLauncher.NAME);
    return getMetricsManager().getTotalMetric(tags, "user." + metric);
  }
}

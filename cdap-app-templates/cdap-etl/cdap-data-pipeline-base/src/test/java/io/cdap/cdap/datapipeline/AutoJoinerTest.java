/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDistribution;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.mock.batch.MockExternalSource;
import io.cdap.cdap.etl.mock.batch.MockSQLEngine;
import io.cdap.cdap.etl.mock.batch.MockSQLEngineWithCapabilities;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.batch.joiner.MockAutoJoiner;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.proto.v2.ETLTransformationPushdown;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for AutoJoiner plugins.
 */
public class AutoJoinerTest extends HydratorTestBase {
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");

  private static int startCount = 0;
  private static final Schema USER_SCHEMA = Schema.recordOf(
    "user",
    Schema.Field.of("region", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("user_id", Schema.of(Schema.Type.INT)),
    Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
  private static final Schema PURCHASE_SCHEMA = Schema.recordOf(
    "purchase",
    Schema.Field.of("region", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("purchase_id", Schema.of(Schema.Type.INT)),
    Schema.Field.of("user_id", Schema.of(Schema.Type.INT)));
  private static final Schema INTEREST_SCHEMA = Schema.recordOf(
    "interest",
    Schema.Field.of("region", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("user_id", Schema.of(Schema.Type.INT)),
    Schema.Field.of("interest", Schema.of(Schema.Type.STRING)));

  private static final Schema AGE_SCHEMA = Schema.recordOf(
    "age",
    Schema.Field.of("region", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("user_id", Schema.of(Schema.Type.INT)),
    Schema.Field.of("age", Schema.of(Schema.Type.INT)));

  private static final StructuredRecord USER_ALICE = StructuredRecord.builder(USER_SCHEMA)
    .set("region", "us")
    .set("user_id", 0)
    .set("name", "alice").build();
  private static final StructuredRecord USER_ALYCE = StructuredRecord.builder(USER_SCHEMA)
    .set("region", "eu")
    .set("user_id", 0)
    .set("name", "alyce").build();
  private static final StructuredRecord USER_BOB = StructuredRecord.builder(USER_SCHEMA)
    .set("region", "us")
    .set("user_id", 1)
    .set("name", "bob").build();
  private static final StructuredRecord USER_JOHN = StructuredRecord.builder(USER_SCHEMA)
    .set("region", "us")
    .set("user_id", 2)
    .set("name", "john").build();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                                                                       Constants.Security.Store.PROVIDER, "file",
                                                                       Constants.AppFabric.SPARK_COMPAT,
                                                                       Compat.SPARK_COMPAT);

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    setupBatchArtifacts(APP_ARTIFACT_ID, DataPipelineApp.class);
  }

  @After
  public void cleanupTest() {
    getMetricsManager().resetAll();
  }

  @Test
  public void testCaseSensitivity() throws Exception {
    Schema weird1 = Schema.recordOf(
      "weird1",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("ID", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("Id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    Schema weird2 = Schema.recordOf(
      "weird2",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("ID", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("val", Schema.of(Schema.Type.STRING)));

    String input1 = UUID.randomUUID().toString();
    String input2 = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("i1", MockSource.getPlugin(input1, weird1)))
      .addStage(new ETLStage("i2", MockSource.getPlugin(input2, weird2)))
      .addStage(new ETLStage("join", MockAutoJoiner.getPlugin(Arrays.asList("i1", "i2"),
                                                              Arrays.asList("id", "ID"),
                                                              Arrays.asList("i1", "i2"), Collections.emptyList(),
                                                              Collections.emptyList(), true)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("i1", "join")
      .addConnection("i2", "join")
      .addConnection("join", "sink")
      .setEngine(Engine.SPARK)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    List<StructuredRecord> input1Data = new ArrayList<>();
    input1Data.add(StructuredRecord.builder(weird1)
                     .set("id", 0)
                     .set("ID", 99L)
                     .set("Id", 0)
                     .set("name", "zero").build());
    input1Data.add(StructuredRecord.builder(weird1)
                     .set("id", 1)
                     .set("ID", 0L)
                     .set("Id", 0)
                     .set("name", "one").build());
    DataSetManager<Table> inputManager = getDataset(input1);
    MockSource.writeInput(inputManager, input1Data);

    List<StructuredRecord> input2Data = new ArrayList<>();
    input2Data.add(StructuredRecord.builder(weird2)
                     .set("id", 0)
                     .set("ID", 99L)
                     .set("val", "0").build());
    input2Data.add(StructuredRecord.builder(weird2)
                     .set("id", 1)
                     .set("ID", 99L)
                     .set("val", "1").build());
    input2Data.add(StructuredRecord.builder(weird2)
                     .set("id", 0)
                     .set("ID", 0L)
                     .set("val", "2").build());
    inputManager = getDataset(input2);
    MockSource.writeInput(inputManager, input2Data);

    Schema expectedSchema = Schema.recordOf(
      "i1.i2",
      Schema.Field.of("i1_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("i1_ID", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("i1_Id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("i1_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("i2_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("i2_ID", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("i2_val", Schema.of(Schema.Type.STRING)));
    StructuredRecord expected = StructuredRecord.builder(expectedSchema)
      .set("i1_id", 0)
      .set("i1_ID", 99L)
      .set("i1_Id", 0)
      .set("i1_name", "zero")
      .set("i2_id", 0)
      .set("i2_ID", 99L)
      .set("i2_val", "0").build();

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    Map<String, String> args = Collections.singletonMap(MockAutoJoiner.PARTITIONS_ARGUMENT, "1");
    workflowManager.startAndWaitForGoodRun(args, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> actual = MockSink.readOutput(outputManager);
    Assert.assertEquals(Collections.singletonList(expected), actual);
  }

  @Test
  public void testBroadcastJoin() throws Exception {
    Schema expectedSchema = Schema.recordOf("purchases.users",
                                            Schema.Field.of("purchases_region", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("purchases_purchase_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("purchases_user_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("users_region", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("users_user_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("users_name", Schema.of(Schema.Type.STRING)));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());

    testSimpleAutoJoin(Arrays.asList("users", "purchases"), Collections.singletonList("users"),
                       expected, Engine.SPARK);
    testSimpleAutoJoin(Arrays.asList("users", "purchases"), Collections.singletonList("purchases"),
                       expected, Engine.SPARK);
  }

  @Test
  public void testBroadcastJoinUsingSQLEngine() throws Exception {
    Schema expectedSchema = Schema.recordOf("purchases.users",
                                            Schema.Field.of("purchases_region", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("purchases_purchase_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("purchases_user_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("users_region", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("users_user_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("users_name", Schema.of(Schema.Type.STRING)));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());

    testSimpleAutoJoinUsingSQLEngine(Arrays.asList("users", "purchases"), Collections.singletonList("users"),
                                     expected, expectedSchema, Engine.SPARK);
    testSimpleAutoJoinUsingSQLEngine(Arrays.asList("users", "purchases"), Collections.singletonList("purchases"),
                                     expected, expectedSchema, Engine.SPARK);
    testSimpleAutoJoinUsingSQLEngineWithCapabilities(Arrays.asList("users", "purchases"),
                                                     Collections.singletonList("users"),
                                                     expected, expectedSchema, Engine.SPARK);
    testSimpleAutoJoinUsingSQLEngineWithCapabilities(Arrays.asList("users", "purchases"),
                                                     Collections.singletonList("purchases"),
                                                     expected, expectedSchema, Engine.SPARK);
  }


  @Test
  public void testAutoInnerJoin() throws Exception {
    Schema expectedSchema = Schema.recordOf("purchases.users",
                                            Schema.Field.of("purchases_region", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("purchases_purchase_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("purchases_user_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("users_region", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("users_user_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("users_name", Schema.of(Schema.Type.STRING)));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());

    testSimpleAutoJoin(Arrays.asList("users", "purchases"), expected, Engine.SPARK);
    testSimpleAutoJoin(Arrays.asList("users", "purchases"), expected, Engine.MAPREDUCE);
  }

  @Test
  public void testAutoInnerJoinUsingSQLEngine() throws Exception {
    Schema expectedSchema = Schema.recordOf("purchases.users",
                                            Schema.Field.of("purchases_region", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("purchases_purchase_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("purchases_user_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("users_region", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("users_user_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("users_name", Schema.of(Schema.Type.STRING)));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());

    testSimpleAutoJoinUsingSQLEngine(Arrays.asList("users", "purchases"), Collections.emptyList(), expected,
                                     expectedSchema, Engine.SPARK);
    testSimpleAutoJoinUsingSQLEngineWithCapabilities(Arrays.asList("users", "purchases"), Collections.emptyList(),
                                                     expected, expectedSchema, Engine.SPARK);
  }

  @Test
  public void testAutoInnerJoinSkewed() throws Exception {
    Schema expectedSchema = Schema.recordOf("interests.users",
                                            Schema.Field.of("interests_region", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("interests_user_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("interests_interest", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("users_region", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("users_user_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("users_name", Schema.of(Schema.Type.STRING)));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("interests_region", "us")
                   .set("interests_interest", "hiking")
                   .set("interests_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());

    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("interests_region", "us")
                   .set("interests_interest", "running")
                   .set("interests_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());

    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("interests_region", "us")
                   .set("interests_interest", "cooking")
                   .set("interests_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());

    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("interests_region", "us")
                   .set("interests_interest", "hiking")
                   .set("interests_user_id", 1)
                   .set("users_region", "us")
                   .set("users_user_id", 1)
                   .set("users_name", "bob").build());

    testSimpleAutoJoinSkewed(Arrays.asList("users", "interests"), expected, Engine.SPARK);
    testSimpleAutoJoinSkewed(Arrays.asList("users", "interests"), expected, Engine.MAPREDUCE);
  }

  @Test
  public void testAutoLeftOuterJoinSkewed() throws Exception {
    Schema expectedSchema = Schema.recordOf("interests.users",
                                            Schema.Field.of("interests_region", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("interests_user_id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("interests_interest", Schema.of(Schema.Type.STRING)),
                                            Schema.Field
                                              .of("users_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                            Schema.Field
                                              .of("users_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                            Schema.Field
                                              .of("users_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("interests_region", "us")
                   .set("interests_interest", "hiking")
                   .set("interests_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());

    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("interests_region", "us")
                   .set("interests_interest", "running")
                   .set("interests_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());

    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("interests_region", "us")
                   .set("interests_interest", "cooking")
                   .set("interests_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());

    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("interests_region", "us")
                   .set("interests_interest", "hiking")
                   .set("interests_user_id", 1)
                   .set("users_region", "us")
                   .set("users_user_id", 1)
                   .set("users_name", "bob").build());

    testSimpleAutoJoinSkewed(Collections.singletonList("interests"), expected, Engine.SPARK);
    testSimpleAutoJoinSkewed(Collections.singletonList("interests"), expected, Engine.MAPREDUCE);
  }

  @Test
  public void testAutoLeftOuterJoin() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "purchases.users",
      Schema.Field.of("purchases_region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("purchases_purchase_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("purchases_user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("users_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("users_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 456)
                   .set("purchases_user_id", 2).build());

    testSimpleAutoJoin(Collections.singletonList("purchases"), expected, Engine.SPARK);
    testSimpleAutoJoin(Collections.singletonList("purchases"), expected, Engine.MAPREDUCE);
  }

  @Test
  public void testAutoRightOuterJoin() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "purchases.users",
      Schema.Field.of("purchases_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("purchases_purchase_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("purchases_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("users_user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("users_name", Schema.of(Schema.Type.STRING)));
    Set<StructuredRecord> expected = new HashSet<>();

    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("users_region", "eu")
                   .set("users_user_id", 0)
                   .set("users_name", "alyce").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("users_region", "us")
                   .set("users_user_id", 1)
                   .set("users_name", "bob").build());

    testSimpleAutoJoin(Collections.singletonList("users"), expected, Engine.SPARK);
    testSimpleAutoJoin(Collections.singletonList("users"), expected, Engine.MAPREDUCE);
  }

  @Test
  public void testAutoOuterJoin() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "purchases.users",
      Schema.Field.of("purchases_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("purchases_purchase_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("purchases_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("users_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 456)
                   .set("purchases_user_id", 2).build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("users_region", "eu")
                   .set("users_user_id", 0)
                   .set("users_name", "alyce").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("users_region", "us")
                   .set("users_user_id", 1)
                   .set("users_name", "bob").build());

    testSimpleAutoJoin(Collections.emptyList(), expected, Engine.SPARK);
    testSimpleAutoJoin(Collections.emptyList(), expected, Engine.MAPREDUCE);
  }

  private void testSimpleAutoJoin(List<String> required, Set<StructuredRecord> expected,
                                  Engine engine) throws Exception {
    testSimpleAutoJoin(required, Collections.emptyList(), expected, engine);
  }

  private void testSimpleAutoJoin(List<String> required, List<String> broadcast,
                                  Set<StructuredRecord> expected, Engine engine) throws Exception {
    /*
         users ------|
                     |--> join --> sink
         purchases --|

         joinOn: users.region = purchases.region and users.user_id = purchases.user_id
     */
    String userInput = UUID.randomUUID().toString();
    String purchaseInput = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("users", MockSource.getPlugin(userInput, USER_SCHEMA)))
      .addStage(new ETLStage("purchases", MockSource.getPlugin(purchaseInput, PURCHASE_SCHEMA)))
      .addStage(new ETLStage("join", MockAutoJoiner.getPlugin(Arrays.asList("purchases", "users"),
                                                              Arrays.asList("region", "user_id"),
                                                              required, broadcast, Collections.emptyList(), true)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("users", "join")
      .addConnection("purchases", "join")
      .addConnection("join", "sink")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    List<StructuredRecord> userData = Arrays.asList(USER_ALICE, USER_ALYCE, USER_BOB);
    DataSetManager<Table> inputManager = getDataset(userInput);
    MockSource.writeInput(inputManager, userData);

    List<StructuredRecord> purchaseData = new ArrayList<>();
    purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 0)
                       .set("purchase_id", 123).build());
    purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 2)
                       .set("purchase_id", 456).build());
    inputManager = getDataset(purchaseInput);
    MockSource.writeInput(inputManager, purchaseData);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    Map<String, String> args = Collections.singletonMap(MockAutoJoiner.PARTITIONS_ARGUMENT, "1");
    workflowManager.startAndWaitForGoodRun(args, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(expected, new HashSet<>(outputRecords));

    validateMetric(5, appId, "join.records.in");
    validateMetric(expected.size(), appId, "join.records.out");
    if (engine != Engine.SPARK) {
      //In SPARK number of partitions hint is ignored, so additional sinks are created
      validateMetric(1, appId, "sink." + MockSink.INITIALIZED_COUNT_METRIC);
    }
  }

  private void testSimpleAutoJoinSkewed(List<String> required,
                                        Set<StructuredRecord> expected, Engine engine) throws Exception {
    /*
         users ------|
                     |--> join --> sink
         purchases --|

         joinOn: users.region = purchases.region and users.user_id = purchases.user_id
     */
    int skewFactor = 3;
    String userInput = UUID.randomUUID().toString();
    String interestInput = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("users", MockSource.getPlugin(userInput, USER_SCHEMA)))
      .addStage(new ETLStage("interests", MockSource.getPlugin(interestInput, INTEREST_SCHEMA)))
      .addStage(new ETLStage("join", MockAutoJoiner.getPlugin(Arrays.asList("interests", "users"),
                                                              Arrays.asList("region", "user_id"),
                                                              required, Collections.emptyList(), true,
                                                              new JoinDistribution(skewFactor,
                                                                                   "interests"))))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("users", "join")
      .addConnection("interests", "join")
      .addConnection("join", "sink")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    List<StructuredRecord> userData = Arrays.asList(USER_ALICE, USER_BOB, USER_JOHN);
    DataSetManager<Table> inputManager = getDataset(userInput);
    MockSource.writeInput(inputManager, userData);

    List<StructuredRecord> interestData = new ArrayList<>();

    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("interest", "hiking")
                       .set("user_id", 0).build());
    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("interest", "running")
                       .set("user_id", 0).build());
    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("interest", "cooking")
                       .set("user_id", 0).build());

    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("interest", "hiking")
                       .set("user_id", 1).build());

    inputManager = getDataset(interestInput);
    MockSource.writeInput(inputManager, interestData);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    Map<String, String> args = Collections.singletonMap(MockAutoJoiner.PARTITIONS_ARGUMENT, "1");
    workflowManager.startAndWaitForGoodRun(args, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(expected, new HashSet<>(outputRecords));

    validateMetric(7, appId, "join.records.in");
    validateMetric(expected.size(), appId, "join.records.out");
    if (engine != Engine.SPARK) {
      //In SPARK number of partitions hint is ignored, so additional sinks are created
      validateMetric(1, appId, "sink." + MockSink.INITIALIZED_COUNT_METRIC);
    }
  }

  private void testSimpleAutoJoinUsingSQLEngine(List<String> required, List<String> broadcast,
                                                Set<StructuredRecord> expected, Schema expectedSchema,
                                                Engine engine) throws Exception {

    File joinInputDir = TMP_FOLDER.newFolder();
    File joinOutputDir = TMP_FOLDER.newFolder();

    // Write input to simulate the join operation results
    // If any of the sides of the operation is a join, then we don't need to write as the SQL engine won't be used.
    if (broadcast.isEmpty()) {
      String joinFile = "join-file1.txt";
      MockSQLEngine.writeInput(new File(joinInputDir, joinFile).getAbsolutePath(), expected);
    }

    /*
         users ------|
                     |--> join --> sink
         purchases --|


         joinOn: users.region = purchases.region and users.user_id = purchases.user_id
     */
    String userInput = UUID.randomUUID().toString();
    String purchaseInput = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();
    String sqlEnginePlugin = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setPushdownEnabled(true)
      .setTransformationPushdown(
        new ETLTransformationPushdown(MockSQLEngine.getPlugin(sqlEnginePlugin,
                                                              joinInputDir.getAbsolutePath(),
                                                              joinOutputDir.getAbsolutePath(),
                                                              expectedSchema)))
      .addStage(new ETLStage("users", MockSource.getPlugin(userInput, USER_SCHEMA)))
      .addStage(new ETLStage("purchases", MockSource.getPlugin(purchaseInput, PURCHASE_SCHEMA)))
      .addStage(new ETLStage("join", MockAutoJoiner.getPlugin(Arrays.asList("purchases", "users"),
                                                              Arrays.asList("region", "user_id"),
                                                              required, broadcast, Collections.emptyList(), true)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("users", "join")
      .addConnection("purchases", "join")
      .addConnection("join", "sink")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    List<StructuredRecord> userData = Arrays.asList(USER_ALICE, USER_ALYCE, USER_BOB);
    DataSetManager<Table> inputManager = getDataset(userInput);
    MockSource.writeInput(inputManager, userData);

    List<StructuredRecord> purchaseData = new ArrayList<>();
    purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 0)
                       .set("purchase_id", 123).build());
    purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 2)
                       .set("purchase_id", 456).build());
    inputManager = getDataset(purchaseInput);
    MockSource.writeInput(inputManager, purchaseData);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    Map<String, String> args = Collections.singletonMap(MockAutoJoiner.PARTITIONS_ARGUMENT, "1");
    workflowManager.startAndWaitForGoodRun(args, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(expected, new HashSet<>(outputRecords));

    validateMetric(5, appId, "join.records.in");
    validateMetric(expected.size(), appId, "join.records.out");

    if (broadcast.isEmpty()) {
      // Ensure all records were written to the SQL engine
      Assert.assertEquals(5, MockSQLEngine.countLinesInDirectory(joinOutputDir));
    } else {
      // Ensure no records are written to the SQL engine if the join contains a broadcast.
      Assert.assertEquals(0, MockSQLEngine.countLinesInDirectory(joinOutputDir));
    }
  }

  private void testSimpleAutoJoinUsingSQLEngineWithCapabilities(List<String> required, List<String> broadcast,
                                                                Set<StructuredRecord> expected, Schema expectedSchema,
                                                                Engine engine) throws Exception {

    File joinOutputDir = TMP_FOLDER.newFolder();

    /*
         users ------|
                     |--> join --> sink
         purchases --|


         joinOn: users.region = purchases.region and users.user_id = purchases.user_id
     */
    String userInput = UUID.randomUUID().toString();
    String purchaseInput = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();
    String sqlEnginePlugin = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .setPushdownEnabled(true)
      .setTransformationPushdown(
        new ETLTransformationPushdown(MockSQLEngineWithCapabilities.getPlugin(sqlEnginePlugin,
                                                                              joinOutputDir.getAbsolutePath(),
                                                                              expectedSchema,
                                                                              expected)))
      .addStage(new ETLStage("users", MockSource.getPlugin(userInput, USER_SCHEMA)))
      .addStage(new ETLStage("purchases", MockSource.getPlugin(purchaseInput, PURCHASE_SCHEMA)))
      .addStage(new ETLStage("join", MockAutoJoiner.getPlugin(Arrays.asList("purchases", "users"),
                                                              Arrays.asList("region", "user_id"),
                                                              required, broadcast, Collections.emptyList(), true)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("users", "join")
      .addConnection("purchases", "join")
      .addConnection("join", "sink")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    List<StructuredRecord> userData = Arrays.asList(USER_ALICE, USER_ALYCE, USER_BOB);
    DataSetManager<Table> inputManager = getDataset(userInput);
    MockSource.writeInput(inputManager, userData);

    List<StructuredRecord> purchaseData = new ArrayList<>();
    purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 0)
                       .set("purchase_id", 123).build());
    purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 2)
                       .set("purchase_id", 456).build());
    inputManager = getDataset(purchaseInput);
    MockSource.writeInput(inputManager, purchaseData);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    Map<String, String> args = Collections.singletonMap(MockAutoJoiner.PARTITIONS_ARGUMENT, "1");
    workflowManager.startAndWaitForGoodRun(args, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(expected, new HashSet<>(outputRecords));

    validateMetric(5, appId, "join.records.in");
    validateMetric(expected.size(), appId, "join.records.out");

    if (broadcast.isEmpty()) {
      // Ensure all records were written to the SQL engine
      Assert.assertEquals(5, MockSQLEngine.countLinesInDirectory(joinOutputDir));
    } else {
      // Ensure no records are written to the SQL engine if the join contains a broadcast.
      Assert.assertEquals(0, MockSQLEngine.countLinesInDirectory(joinOutputDir));
    }
  }

  @Test
  public void testDoubleBroadcastJoin() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "purchases.users.interests",
      Schema.Field.of("purchases_region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("purchases_purchase_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("purchases_user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("users_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("users_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("interests_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("interests_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("interests_interest", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice")
                   .set("interests_region", "us")
                   .set("interests_user_id", 0)
                   .set("interests_interest", "food").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice")
                   .set("interests_region", "us")
                   .set("interests_user_id", 0)
                   .set("interests_interest", "sports").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 456)
                   .set("purchases_user_id", 2)
                   .set("interests_region", "us")
                   .set("interests_user_id", 2)
                   .set("interests_interest", "gaming").build());

    testTripleAutoJoin(Collections.singletonList("purchases"), Arrays.asList("purchases", "interests"),
                       expected, Engine.SPARK, Arrays.asList("purchases", "users", "interests"));
  }

  @Test
  public void testTripleAutoSingleRequiredJoin() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "purchases.users.interests",
      Schema.Field.of("purchases_region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("purchases_purchase_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("purchases_user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("users_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("users_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("interests_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("interests_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("interests_interest", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice")
                   .set("interests_region", "us")
                   .set("interests_user_id", 0)
                   .set("interests_interest", "food").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice")
                   .set("interests_region", "us")
                   .set("interests_user_id", 0)
                   .set("interests_interest", "sports").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 456)
                   .set("purchases_user_id", 2)
                   .set("interests_region", "us")
                   .set("interests_user_id", 2)
                   .set("interests_interest", "gaming").build());

    //First required : all left joins
    testTripleAutoJoin(Collections.singletonList("purchases"), expected, Engine.SPARK,
                       Arrays.asList("purchases", "users", "interests"));
    testTripleAutoJoin(Collections.singletonList("purchases"), expected, Engine.MAPREDUCE, Collections.emptyList());

    //Middle Required : right join + left join.
    testTripleAutoJoin(Collections.singletonList("purchases"), expected, Engine.SPARK,
                       Arrays.asList("users", "purchases", "interests"));
    testTripleAutoJoin(Collections.singletonList("purchases"), expected, Engine.MAPREDUCE,
                       Arrays.asList("users", "purchases", "interests"));

    //Last Required : outer + left join with coalesce from first 2
    testTripleAutoJoin(Collections.singletonList("purchases"), expected, Engine.SPARK,
                       Arrays.asList("users", "interests", "purchases"));

  }

  @Test
  public void testTripleAutoTwoRequiredJoin() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "purchases.users.interests",
      Schema.Field.of("purchases_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("purchases_purchase_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("purchases_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("users_user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("users_name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("interests_region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("interests_user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("interests_interest", Schema.of(Schema.Type.STRING)));

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice")
                   .set("interests_region", "us")
                   .set("interests_user_id", 0)
                   .set("interests_interest", "food").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice")
                   .set("interests_region", "us")
                   .set("interests_user_id", 0)
                   .set("interests_interest", "sports").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("users_region", "us")
                   .set("users_user_id", 1)
                   .set("users_name", "bob")
                   .set("interests_region", "us")
                   .set("interests_user_id", 1)
                   .set("interests_interest", "gardening").build());

    testTripleAutoJoin(Arrays.asList("users", "interests"), expected, Engine.SPARK, Collections.emptyList());
    testTripleAutoJoin(Arrays.asList("users", "interests"), expected, Engine.MAPREDUCE, Collections.emptyList());
  }

  @Test
  public void testTripleAutoNoneRequiredJoin() throws Exception {
    /*
    In this case, all the JOINS will be full outer joins
    i.e.
    Purchases (outer) Users (outer) Interests
    */

    Schema expectedSchema = Schema.recordOf(
      "purchases.users.interests",
      Schema.Field.of("purchases_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("purchases_purchase_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("purchases_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("users_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("interests_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("interests_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("interests_interest", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 456)
                   .set("purchases_user_id", 2)
                   .set("interests_region", "us")
                   .set("interests_user_id", 2)
                   .set("interests_interest", "gaming").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("users_region", "eu")
                   .set("users_user_id", 0)
                   .set("users_name", "alyce").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice")
                   .set("interests_region", "us")
                   .set("interests_user_id", 0)
                   .set("interests_interest", "food").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("purchases_region", "us")
                   .set("purchases_purchase_id", 123)
                   .set("purchases_user_id", 0)
                   .set("users_region", "us")
                   .set("users_user_id", 0)
                   .set("users_name", "alice")
                   .set("interests_region", "us")
                   .set("interests_user_id", 0)
                   .set("interests_interest", "sports").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("users_region", "us")
                   .set("users_user_id", 1)
                   .set("users_name", "bob")
                   .set("interests_region", "us")
                   .set("interests_user_id", 1)
                   .set("interests_interest", "gardening").build());

    /*
    The output should not be affected by order of joins
     */
    testTripleAutoJoin(Collections.emptyList(), expected, Engine.SPARK,
                       Arrays.asList("purchases", "users", "interests"));
    testTripleAutoJoin(Collections.emptyList(), expected, Engine.SPARK,
                       Arrays.asList("purchases", "interests", "users"));
    testTripleAutoJoin(Collections.emptyList(), expected, Engine.SPARK,
                       Arrays.asList("users", "purchases", "interests"));
  }

  private void testTripleAutoJoin(List<String> required, Set<StructuredRecord> expected,
                                  Engine engine, List<String> tablesInOrderToJoin) throws Exception {
    //Default order :
    if (tablesInOrderToJoin == null || tablesInOrderToJoin.isEmpty()) {
      tablesInOrderToJoin = Arrays.asList("purchases", "users", "interests");
    }

    testTripleAutoJoin(required, Collections.emptyList(), expected, engine, tablesInOrderToJoin);
  }

  private void testTripleAutoJoin(List<String> required, List<String> broadcast, Set<StructuredRecord> expected,
                                  Engine engine, List<String> tablesInOrderToJoin) throws Exception {
    /*
         users ------|
                     |
         purchases --|--> join --> sink
                     |
         interests --|

         joinOn: users.region = purchases.region = interests.region and
                 users.user_id = purchases.user_id = interests.user_id
     */
    String userInput = UUID.randomUUID().toString();
    String purchaseInput = UUID.randomUUID().toString();
    String interestInput = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("users", MockSource.getPlugin(userInput, USER_SCHEMA)))
      .addStage(new ETLStage("purchases", MockSource.getPlugin(purchaseInput, PURCHASE_SCHEMA)))
      .addStage(new ETLStage("interests", MockSource.getPlugin(interestInput, INTEREST_SCHEMA)))
      .addStage(new ETLStage("join", MockAutoJoiner.getPlugin(tablesInOrderToJoin,
                                                              Arrays.asList("region", "user_id"),
                                                              required, broadcast,
                                                              Collections.emptyList(), true)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("users", "join")
      .addConnection("purchases", "join")
      .addConnection("interests", "join")
      .addConnection("join", "sink")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    List<StructuredRecord> userData = Arrays.asList(USER_ALICE, USER_ALYCE, USER_BOB);
    DataSetManager<Table> inputManager = getDataset(userInput);
    MockSource.writeInput(inputManager, userData);

    List<StructuredRecord> purchaseData = new ArrayList<>();
    purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 0)
                       .set("purchase_id", 123).build());
    purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 2)
                       .set("purchase_id", 456).build());
    inputManager = getDataset(purchaseInput);
    MockSource.writeInput(inputManager, purchaseData);

    List<StructuredRecord> interestData = new ArrayList<>();
    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 0)
                       .set("interest", "food")
                       .build());
    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 0)
                       .set("interest", "sports")
                       .build());
    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 1)
                       .set("interest", "gardening")
                       .build());
    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 2)
                       .set("interest", "gaming")
                       .build());
    inputManager = getDataset(interestInput);
    MockSource.writeInput(inputManager, interestData);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForGoodRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Set<StructuredRecord> actual = new HashSet<>();
    Schema expectedSchema = expected.iterator().hasNext() ? expected.iterator().next().getSchema() : null;

    if (expectedSchema == null || expected.iterator().next().getSchema() == outputRecords.get(0).getSchema()) {
      actual = new HashSet<>(outputRecords);
    } else {
      //reorder the output columns of the join result (actual) to match the column order of expected
      for (StructuredRecord sr : outputRecords) {
        actual.add(StructuredRecord.builder(expectedSchema)
                     .set("purchases_region", sr.get("purchases_region"))
                     .set("purchases_purchase_id",  sr.get("purchases_purchase_id"))
                     .set("purchases_user_id",  sr.get("purchases_user_id"))
                     .set("users_region",  sr.get("users_region"))
                     .set("users_user_id",  sr.get("users_user_id"))
                     .set("users_name",  sr.get("users_name"))
                     .set("interests_region",  sr.get("interests_region"))
                     .set("interests_user_id",  sr.get("interests_user_id"))
                     .set("interests_interest",  sr.get("interests_interest")).build());
      }
    }

    Assert.assertEquals(expected, actual);

    validateMetric(9, appId, "join.records.in");
    validateMetric(expected.size(), appId, "join.records.out");
  }

  @Test
  public void testNullNotEqual() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "items.attributes",
      Schema.Field.of("items_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("items_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("items_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("attributes_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("attributes_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("attributes_attr", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("items_id", 0)
                   .set("items_region", "us")
                   .set("items_name", "bacon")
                   .set("attributes_region", "us")
                   .set("attributes_id", 0)
                   .set("attributes_attr", "food")
                   .build());
    expected.add(StructuredRecord.builder(expectedSchema).set("items_id", 1).build());
    expected.add(StructuredRecord.builder(expectedSchema).set("items_region", "us").build());

    testNullEquality(Engine.SPARK, false, expected);
    testNullEquality(Engine.MAPREDUCE, false, expected);
  }

  @Test
  public void testNullIsEqual() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "items.attributes",
      Schema.Field.of("items_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("items_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("items_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("attributes_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("attributes_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("attributes_attr", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("items_id", 0)
                   .set("items_region", "us")
                   .set("items_name", "bacon")
                   .set("attributes_region", "us")
                   .set("attributes_id", 0)
                   .set("attributes_attr", "food")
                   .build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("items_id", 1)
                   .set("attributes_id", 1)
                   .set("attributes_attr", "car").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("items_region", "us")
                   .set("attributes_region", "us").build());

    testNullEquality(Engine.SPARK, true, expected);
    testNullEquality(Engine.MAPREDUCE, true, expected);
  }

  private void testNullEquality(Engine engine, boolean nullIsEqual, Set<StructuredRecord> expected) throws Exception {
    Schema itemSchema = Schema.recordOf(
      "item",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Schema attributeSchema = Schema.recordOf(
      "attribute",
      Schema.Field.of("region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("attr", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    /*
         items -------|
                      |--> join --> sink
         attributes --|

         joinOn: items.region = attributes.region and items.id = attributes.id
     */
    String itemsInput = UUID.randomUUID().toString();
    String attributesInput = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("items", MockSource.getPlugin(itemsInput, itemSchema)))
      .addStage(new ETLStage("attributes", MockSource.getPlugin(attributesInput, attributeSchema)))
      .addStage(new ETLStage("join", MockAutoJoiner.getPlugin(Arrays.asList("items", "attributes"),
                                                              Arrays.asList("region", "id"),
                                                              Collections.singletonList("items"),
                                                              Collections.emptyList(),
                                                              Collections.emptyList(),
                                                              nullIsEqual)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("items", "join")
      .addConnection("attributes", "join")
      .addConnection("join", "sink")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    List<StructuredRecord> itemData = new ArrayList<>();
    itemData.add(StructuredRecord.builder(itemSchema)
                   .set("region", "us")
                   .set("id", 0)
                   .set("name", "bacon")
                   .build());
    itemData.add(StructuredRecord.builder(itemSchema)
                   .set("id", 1)
                   .build());
    itemData.add(StructuredRecord.builder(itemSchema)
                   .set("region", "us")
                   .build());
    DataSetManager<Table> inputManager = getDataset(itemsInput);
    MockSource.writeInput(inputManager, itemData);

    List<StructuredRecord> attributesData = new ArrayList<>();
    attributesData.add(StructuredRecord.builder(attributeSchema)
                         .set("region", "us")
                         .set("id", 0)
                         .set("attr", "food").build());
    attributesData.add(StructuredRecord.builder(attributeSchema).set("id", 1).set("attr", "car").build());
    attributesData.add(StructuredRecord.builder(attributeSchema).set("region", "us").build());
    inputManager = getDataset(attributesInput);
    MockSource.writeInput(inputManager, attributesData);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForGoodRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(expected, new HashSet<>(outputRecords));
  }

  @Test
  public void testLeftOuterAutoJoinWithMacros() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "Record0",
      Schema.Field.of("region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("purchase_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("region", "us")
                   .set("purchase_id", 123)
                   .set("user_id", 0)
                   .set("name", "alice").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("region", "us")
                   .set("purchase_id", 456)
                   .set("user_id", 2).build());

    testAutoJoinWithMacros(Engine.MAPREDUCE, Collections.singletonList("purchases"), expectedSchema, expected,
                           false, false);
    testAutoJoinWithMacros(Engine.SPARK, Collections.singletonList("purchases"), expectedSchema, expected,
                           false, false);
  }

  @Test
  public void testInnerAutoJoinWithMacros() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "Record0",
      Schema.Field.of("region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("purchase_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("region", "us")
                   .set("purchase_id", 123)
                   .set("user_id", 0)
                   .set("name", "alice").build());

    testAutoJoinWithMacros(Engine.SPARK, Arrays.asList("purchases", "users"), expectedSchema, expected,
                           false, false);

    expectedSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("purchase_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("region", "us")
                   .set("purchase_id", 123)
                   .set("user_id", 0)
                   .set("name", "alice").build());
    testAutoJoinWithMacros(Engine.MAPREDUCE, Arrays.asList("purchases", "users"), expectedSchema, expected,
                           false, false);
  }

  @Test
  public void testAutoJoinWithMacrosAndEmptyInput() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "joined",
      Schema.Field.of("region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("purchase_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("region", "us")
                   .set("purchase_id", 123)
                   .set("user_id", 0).build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("region", "us")
                   .set("purchase_id", 456)
                   .set("user_id", 2).build());

    // right side empty
    testAutoJoinWithMacros(Engine.SPARK, Collections.singletonList("purchases"), expectedSchema, expected,
                           true, false);
    testAutoJoinWithMacros(Engine.MAPREDUCE, Collections.singletonList("purchases"), expectedSchema, expected,
                           true, false);

    // left side empty
    expected.clear();
    testAutoJoinWithMacros(Engine.MAPREDUCE, Collections.singletonList("purchases"), expectedSchema, expected,
                           false, true);
    testAutoJoinWithMacros(Engine.SPARK, Collections.singletonList("purchases"), expectedSchema, expected,
                           false, true);

    // both sides empty
    expected.clear();
    testAutoJoinWithMacros(Engine.MAPREDUCE, Collections.singletonList("purchases"), expectedSchema, expected,
                           true, true);
    testAutoJoinWithMacros(Engine.SPARK, Collections.singletonList("purchases"), expectedSchema, expected,
                           true, true);
  }

  @Test
  public void testOuterOrJoin() throws Exception {
    /*
         users ------|
                     |--> join --> sink
         emails -----|

         joinOn: users.first_name = emails.name or users.full_name = emails.name
     */
    Schema userSchema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("first_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("full name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Schema emailSchema = Schema.recordOf(
      "email",
      Schema.Field.of("email", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Schema expectedSchema = Schema.recordOf(
      "user names.emails",
      Schema.Field.of("user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    String userInput = UUID.randomUUID().toString();
    String emailsInput = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();

    List<JoinField> select = new ArrayList<>();
    select.add(new JoinField("user names", "id", "user_id"));
    select.add(new JoinField("emails", "name"));
    select.add(new JoinField("emails", "email"));

    JoinCondition.OnExpression condition = JoinCondition.onExpression()
      .addDatasetAlias("user names", "U")
      .addDatasetAlias("emails", "E")
      .setExpression("U.first_name = E.name OR U.`full name` = E.name")
      .build();
    Map<String, String> joinerProperties = MockAutoJoiner.getProperties(Arrays.asList("user names", "emails"),
                                                                        Collections.emptyList(),
                                                                        Collections.emptyList(),
                                                                        Collections.emptyList(),
                                                                        select, false, null, condition);
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("user names", MockSource.getPlugin(userInput, userSchema)))
      .addStage(new ETLStage("emails", MockSource.getPlugin(emailsInput, emailSchema)))
      .addStage(new ETLStage("join", new ETLPlugin(MockAutoJoiner.NAME, BatchJoiner.PLUGIN_TYPE, joinerProperties)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("user names", "join")
      .addConnection("emails", "join")
      .addConnection("join", "sink")
      .setEngine(Engine.SPARK)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    List<StructuredRecord> records = new ArrayList<>();
    records.add(StructuredRecord.builder(userSchema).set("id", 0).set("first_name", "Billy").build());
    records.add(StructuredRecord.builder(userSchema).set("id", 1).set("full name", "Bobby Bob").build());
    records.add(StructuredRecord.builder(userSchema).set("id", 2)
                  .set("first_name", "Bob").set("full name", "Bob Loblaw").build());
    DataSetManager<Table> inputManager = getDataset(userInput);
    MockSource.writeInput(inputManager, records);

    records.clear();
    records.add(StructuredRecord.builder(emailSchema).set("email", "billy@example.com").set("name", "Billy").build());
    records.add(StructuredRecord.builder(emailSchema).set("email", "b@example.com").set("name", "Bob Loblaw").build());
    records.add(StructuredRecord.builder(emailSchema).set("email", "c@example.com").set("name", "Chris").build());
    inputManager = getDataset(emailsInput);
    MockSource.writeInput(inputManager, records);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForGoodRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("user_id", 0).set("name", "Billy").set("email", "billy@example.com").build());
    expected.add(StructuredRecord.builder(expectedSchema).set("user_id", 1).build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("user_id", 2).set("name", "Bob Loblaw").set("email", "b@example.com").build());
    expected.add(StructuredRecord.builder(expectedSchema).set("name", "Chris").set("email", "c@example.com").build());

    Assert.assertEquals(expected, new HashSet<>(outputRecords));
  }

  @Test
  public void testInnerBetweenCondition() throws Exception {
    /*
         users ----------|
                         |--> join --> sink
         age_groups -----|

         joinOn: users.age > age_groups.lo and (users.age <= age_groups.hi or age_groups.hi is null)
     */
    Schema userSchema = Schema.recordOf(
      "user",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("age", Schema.nullableOf(Schema.of(Schema.Type.INT))));
    Schema ageGroupSchema = Schema.recordOf(
      "age_group",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("lo", Schema.of(Schema.Type.INT)),
      Schema.Field.of("hi", Schema.nullableOf(Schema.of(Schema.Type.INT))));
    Schema expectedSchema = Schema.recordOf(
      "users.age_groups",
      Schema.Field.of("username", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("age_group", Schema.of(Schema.Type.STRING)));
    String userInput = UUID.randomUUID().toString();
    String agesInput = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();

    List<JoinField> select = new ArrayList<>();
    select.add(new JoinField("users", "name", "username"));
    select.add(new JoinField("age_groups", "name", "age_group"));

    JoinCondition.OnExpression condition = JoinCondition.onExpression()
      .setExpression("users.age >= age_groups.lo and (users.age < age_groups.hi or age_groups.hi is null)")
      .build();
    Map<String, String> joinerProperties = MockAutoJoiner.getProperties(Arrays.asList("users", "age_groups"),
                                                                        Collections.emptyList(),
                                                                        Arrays.asList("users", "age_groups"),
                                                                        Collections.emptyList(),
                                                                        select, false, null, condition);
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("users", MockSource.getPlugin(userInput, userSchema)))
      .addStage(new ETLStage("age_groups", MockSource.getPlugin(agesInput, ageGroupSchema)))
      .addStage(new ETLStage("join", new ETLPlugin(MockAutoJoiner.NAME, BatchJoiner.PLUGIN_TYPE, joinerProperties)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("users", "join")
      .addConnection("age_groups", "join")
      .addConnection("join", "sink")
      .setEngine(Engine.SPARK)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    List<StructuredRecord> records = new ArrayList<>();
    records.add(StructuredRecord.builder(userSchema).set("name", "Alice").set("age", 35).build());
    records.add(StructuredRecord.builder(userSchema).set("name", "Bob").build());
    records.add(StructuredRecord.builder(userSchema).set("name", "Carl").set("age", 13).build());
    records.add(StructuredRecord.builder(userSchema).set("name", "Dave").set("age", 0).build());
    records.add(StructuredRecord.builder(userSchema).set("name", "Elaine").set("age", 68).build());
    records.add(StructuredRecord.builder(userSchema).set("name", "Fred").set("age", 4).build());
    DataSetManager<Table> inputManager = getDataset(userInput);
    MockSource.writeInput(inputManager, records);

    records.clear();
    records.add(StructuredRecord.builder(ageGroupSchema).set("name", "infant").set("lo", 0).set("hi", 2).build());
    records.add(StructuredRecord.builder(ageGroupSchema).set("name", "toddler").set("lo", 2).set("hi", 5).build());
    records.add(StructuredRecord.builder(ageGroupSchema).set("name", "child").set("lo", 5).set("hi", 13).build());
    records.add(StructuredRecord.builder(ageGroupSchema).set("name", "teen").set("lo", 13).set("hi", 20).build());
    records.add(StructuredRecord.builder(ageGroupSchema).set("name", "adult").set("lo", 20).set("hi", 65).build());
    records.add(StructuredRecord.builder(ageGroupSchema).set("name", "senior").set("lo", 65).build());
    inputManager = getDataset(agesInput);
    MockSource.writeInput(inputManager, records);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForGoodRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema).set("username", "Alice").set("age_group", "adult").build());
    expected.add(StructuredRecord.builder(expectedSchema).set("username", "Carl").set("age_group", "teen").build());
    expected.add(StructuredRecord.builder(expectedSchema).set("username", "Dave").set("age_group", "infant").build());
    expected.add(StructuredRecord.builder(expectedSchema).set("username", "Elaine").set("age_group", "senior").build());
    expected.add(StructuredRecord.builder(expectedSchema).set("username", "Fred").set("age_group", "toddler").build());

    Assert.assertEquals(expected, new HashSet<>(outputRecords));

    validateMetric(6, appId, "users.records.out");
    validateMetric(6, appId, "age_groups.records.out");
    validateMetric(12, appId, "join.records.in");
    validateMetric(expected.size(), appId, "join.records.out");
  }

  @Test
  public void testLeftOuterComplexConditionBroadcast() throws Exception {
    /*
         sales ----------|
                         |--> join --> sink
         categories -----|

         joinOn:
           sales.price > 1000 and sales.date > 2020-01-01 and
           (sales.category <=> categories.id or (sales.category is null and sales.department = categories.department))
     */
    Schema salesSchema = Schema.recordOf(
      "sale",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("date", Schema.of(Schema.LogicalType.DATETIME)),
      Schema.Field.of("category", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("department", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    Schema categorySchema = Schema.recordOf(
      "category",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("department", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("flag", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
    Schema expectedSchema = Schema.recordOf(
      "sales.categories",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("flag", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
    String salesInput = UUID.randomUUID().toString();
    String categoriesInput = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();

    List<JoinField> select = new ArrayList<>();
    select.add(new JoinField("sales", "id"));
    select.add(new JoinField("categories", "flag"));

    /*
           sales.price > 1000 and sales.date > 2020-01-01 and
           (sales.category <=> categories.id or (sales.category is null and sales.department = categories.department))
     */
    JoinCondition.OnExpression condition = JoinCondition.onExpression()
      .addDatasetAlias("sales", "S")
      .addDatasetAlias("categories", "C")
      .setExpression("S.price > 1000 and S.date > '2020-01-01 00:00:00' and " +
                       "(S.category = C.id or (S.category is null and S.department = C.department))")
      .build();
    Map<String, String> joinerProperties = MockAutoJoiner.getProperties(Arrays.asList("sales", "categories"),
                                                                        Collections.emptyList(),
                                                                        Collections.singletonList("sales"),
                                                                        Collections.singletonList("categories"),
                                                                        select, false, null, condition);
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("sales", MockSource.getPlugin(salesInput, salesSchema)))
      .addStage(new ETLStage("categories", MockSource.getPlugin(categoriesInput, categorySchema)))
      .addStage(new ETLStage("join", new ETLPlugin(MockAutoJoiner.NAME, BatchJoiner.PLUGIN_TYPE, joinerProperties)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("sales", "join")
      .addConnection("categories", "join")
      .addConnection("join", "sink")
      .setEngine(Engine.SPARK)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    List<StructuredRecord> records = new ArrayList<>();
    records.add(StructuredRecord.builder(salesSchema)
                  .set("id", 0).set("price", 123.45d).set("date", "2021-01-01 00:00:00")
                  .set("category", "electronics").set("department", "entertainment").build());
    records.add(StructuredRecord.builder(salesSchema)
                  .set("id", 1).set("price", 1000.01d).set("date", "2020-01-01 00:00:01")
                  .set("department", "home").build());
    records.add(StructuredRecord.builder(salesSchema)
                  .set("id", 2).set("price", 5000d).set("date", "2021-01-01 00:00:00")
                  .set("category", "furniture").build());
    records.add(StructuredRecord.builder(salesSchema)
                  .set("id", 3).set("price", 2000d).set("date", "2019-12-31 23:59:59")
                  .set("category", "furniture").build());
    records.add(StructuredRecord.builder(salesSchema)
                  .set("id", 4).set("price", 2000d).set("date", "2020-01-01 12:00:00")
                  .set("category", "tv").set("department", "entertainment").build());
    DataSetManager<Table> inputManager = getDataset(salesInput);
    MockSource.writeInput(inputManager, records);

    records.clear();
    records.add(StructuredRecord.builder(categorySchema)
                  .set("id", "electronics").set("department", "entertainment").set("flag", false).build());
    records.add(StructuredRecord.builder(categorySchema)
                  .set("id", "furniture").set("department", "home").set("flag", true).build());
    records.add(StructuredRecord.builder(categorySchema)
                  .set("id", "tv").set("department", "entertainment").set("flag", false).build());
    inputManager = getDataset(categoriesInput);
    MockSource.writeInput(inputManager, records);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForGoodRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema).set("id", 0).build());
    expected.add(StructuredRecord.builder(expectedSchema).set("id", 1).set("flag", true).build());
    expected.add(StructuredRecord.builder(expectedSchema).set("id", 2).set("flag", true).build());
    expected.add(StructuredRecord.builder(expectedSchema).set("id", 3).build());
    expected.add(StructuredRecord.builder(expectedSchema).set("id", 4).set("flag", false).build());

    Assert.assertEquals(expected, new HashSet<>(outputRecords));
  }

  private void testAutoJoinWithMacros(Engine engine, List<String> required, Schema expectedSchema,
                                      Set<StructuredRecord> expectedRecords, boolean excludeUsers,
                                      boolean excludePurchases) throws Exception {
    /*
         users ------|
                     |--> join --> sink
         purchases --|

         joinOn: users.region = purchases.region and users.user_id = purchases.user_id
     */
    String userInput = UUID.randomUUID().toString();
    String purchaseInput = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();

    Map<String, String> joinerProps = new HashMap<>();
    joinerProps.put(MockAutoJoiner.Conf.STAGES, "${stages}");
    joinerProps.put(MockAutoJoiner.Conf.KEY, "${key}");
    joinerProps.put(MockAutoJoiner.Conf.REQUIRED, "${required}");
    joinerProps.put(MockAutoJoiner.Conf.SELECT, "${select}");
    if (engine == Engine.SPARK || (required.size() < 2 && engine == Engine.MAPREDUCE)) {
      joinerProps.put(MockAutoJoiner.Conf.SCHEMA, "${schema}");
    }
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("users", MockSource.getPlugin(userInput)))
      .addStage(new ETLStage("purchases", MockSource.getPlugin(purchaseInput)))
      .addStage(new ETLStage("join", new ETLPlugin(MockAutoJoiner.NAME, BatchJoiner.PLUGIN_TYPE, joinerProps)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("users", "join")
      .addConnection("purchases", "join")
      .addConnection("join", "sink")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    if (!excludeUsers) {
      List<StructuredRecord> userData = Arrays.asList(USER_ALICE, USER_ALYCE, USER_BOB);
      DataSetManager<Table> inputManager = getDataset(userInput);
      MockSource.writeInput(inputManager, userData);
    }

    if (!excludePurchases) {
      List<StructuredRecord> purchaseData = new ArrayList<>();
      purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                         .set("region", "us")
                         .set("user_id", 0)
                         .set("purchase_id", 123).build());
      purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                         .set("region", "us")
                         .set("user_id", 2)
                         .set("purchase_id", 456).build());
      DataSetManager<Table> inputManager = getDataset(purchaseInput);
      MockSource.writeInput(inputManager, purchaseData);
    }

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    List<JoinField> selectedFields = new ArrayList<>();
    selectedFields.add(new JoinField("purchases", "region"));
    selectedFields.add(new JoinField("purchases", "purchase_id"));
    selectedFields.add(new JoinField("purchases", "user_id"));
    selectedFields.add(new JoinField("users", "name"));
    Map<String, String> joinerProperties = MockAutoJoiner.getProperties(Arrays.asList("purchases", "users"),
                                                                        Arrays.asList("region", "user_id"),
                                                                        required, Collections.emptyList(),
                                                                        selectedFields, true);
    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put("stages", joinerProperties.get(MockAutoJoiner.Conf.STAGES));
    runtimeArgs.put("key", joinerProperties.get(MockAutoJoiner.Conf.KEY));
    runtimeArgs.put("required", joinerProperties.get(MockAutoJoiner.Conf.REQUIRED));
    runtimeArgs.put("select", joinerProperties.get(MockAutoJoiner.Conf.SELECT));
    runtimeArgs.put("schema", expectedSchema.toString());
    workflowManager.startAndWaitForGoodRun(runtimeArgs, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(expectedRecords, new HashSet<>(outputRecords));
  }

  private void validateMetric(long expected, ApplicationId appId,
                              String metric) throws TimeoutException, InterruptedException {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName(),
                                               Constants.Metrics.Tag.WORKFLOW, SmartWorkflow.NAME);
    getMetricsManager().waitForTotalMetricCount(tags, "user." + metric, expected, 20, TimeUnit.SECONDS);
    // wait for won't throw an exception if the metric count is greater than expected
    Assert.assertEquals("Wrong metric value for " + metric,
                        expected, getMetricsManager().getTotalMetric(tags, "user." + metric));
  }


  @Test
  public void testQuadAutoOneRequiredJoin() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "ages.purchases.users.interests",
      Schema.Field.of("ages_region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("ages_user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("ages_age", Schema.of(Schema.Type.INT)),
      Schema.Field.of("purchases_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("purchases_purchase_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("purchases_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("users_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("interests_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("interests_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("interests_interest", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("ages_region", "us")
                   .set("ages_user_id", 10)
                   .set("ages_age", 20).build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("ages_region", "us")
                   .set("ages_user_id", 1)
                   .set("ages_age", 30)
                   .set("purchases_region", null)
                   .set("purchases_purchase_id", null)
                   .set("purchases_user_id", null)
                   .set("users_region", "us")
                   .set("users_user_id", 1)
                   .set("users_name", "bob")
                   .set("interests_region", "us")
                   .set("interests_user_id", 1)
                   .set("interests_interest", "gardening").build());
    //First is required : all left joins
    testQuadAutoJoin(Collections.singletonList("ages"), Collections.emptyList(), expected, Engine.SPARK,
                       Arrays.asList("ages", "purchases", "users", "interests"));

    //Last is required : outer + outer + outer + right
    testQuadAutoJoin(Collections.singletonList("ages"), Collections.emptyList(), expected, Engine.SPARK,
                       Arrays.asList("purchases", "interests", "users", "ages"));
  }

  private void testQuadAutoJoin(List<String> required, List<String> broadcast, Set<StructuredRecord> expected,
                                  Engine engine, List<String> tablesInOrderToJoin) throws Exception {
    /*
         users ------|
                     |
         purchases --|--> join --> sink
                     |
         interests --|
                     |
         age --------|

         joinOn: users.region = purchases.region = interests.region = age.region and
                 users.user_id = purchases.user_id = interests.user_id = age.user_id
     */
    String userInput = UUID.randomUUID().toString();
    String purchaseInput = UUID.randomUUID().toString();
    String interestInput = UUID.randomUUID().toString();
    String ageInput = UUID.randomUUID().toString();
    String output = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("users", MockSource.getPlugin(userInput, USER_SCHEMA)))
      .addStage(new ETLStage("purchases", MockSource.getPlugin(purchaseInput, PURCHASE_SCHEMA)))
      .addStage(new ETLStage("interests", MockSource.getPlugin(interestInput, INTEREST_SCHEMA)))
      .addStage(new ETLStage("ages", MockSource.getPlugin(ageInput, AGE_SCHEMA)))
      .addStage(new ETLStage("join", MockAutoJoiner.getPlugin(tablesInOrderToJoin,
                                                              Arrays.asList("region", "user_id"),
                                                              required, broadcast,
                                                              Collections.emptyList(), true)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(output)))
      .addConnection("users", "join")
      .addConnection("purchases", "join")
      .addConnection("interests", "join")
      .addConnection("ages", "join")
      .addConnection("join", "sink")
      .setEngine(engine)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, config);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input data
    List<StructuredRecord> userData = Arrays.asList(USER_ALICE, USER_ALYCE, USER_BOB);
    DataSetManager<Table> inputManager = getDataset(userInput);
    MockSource.writeInput(inputManager, userData);

    List<StructuredRecord> purchaseData = new ArrayList<>();
    purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 0)
                       .set("purchase_id", 123).build());
    purchaseData.add(StructuredRecord.builder(PURCHASE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 2)
                       .set("purchase_id", 456).build());
    inputManager = getDataset(purchaseInput);
    MockSource.writeInput(inputManager, purchaseData);

    List<StructuredRecord> interestData = new ArrayList<>();
    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 0)
                       .set("interest", "food")
                       .build());
    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 0)
                       .set("interest", "sports")
                       .build());
    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 1)
                       .set("interest", "gardening")
                       .build());
    interestData.add(StructuredRecord.builder(INTEREST_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 2)
                       .set("interest", "gaming")
                       .build());
    inputManager = getDataset(interestInput);
    MockSource.writeInput(inputManager, interestData);

    List<StructuredRecord> ageData = new ArrayList<>();
    ageData.add(StructuredRecord.builder(AGE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 10)
                       .set("age", 20)
                       .build());
    ageData.add(StructuredRecord.builder(AGE_SCHEMA)
                       .set("region", "us")
                       .set("user_id", 1)
                       .set("age", 30)
                       .build());
    inputManager = getDataset(ageInput);
    MockSource.writeInput(inputManager, ageData);


    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForGoodRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Set<StructuredRecord> actual = new HashSet<>();
    Schema expectedSchema = expected.iterator().hasNext() ? expected.iterator().next().getSchema() : null;

    if (expectedSchema == null || expected.iterator().next().getSchema() == outputRecords.get(0).getSchema()) {
      actual = new HashSet<>(outputRecords);
    } else {
      //reorder the output columns of the join result (actual) to match the column order of expected
      for (StructuredRecord sr : outputRecords) {
        actual.add(StructuredRecord.builder(expectedSchema)
                     .set("ages_region", sr.get("ages_region"))
                     .set("ages_age",  sr.get("ages_age"))
                     .set("ages_user_id",  sr.get("ages_user_id"))
                     .set("purchases_region", sr.get("purchases_region"))
                     .set("purchases_purchase_id",  sr.get("purchases_purchase_id"))
                     .set("purchases_user_id",  sr.get("purchases_user_id"))
                     .set("users_region",  sr.get("users_region"))
                     .set("users_user_id",  sr.get("users_user_id"))
                     .set("users_name",  sr.get("users_name"))
                     .set("interests_region",  sr.get("interests_region"))
                     .set("interests_user_id",  sr.get("interests_user_id"))
                     .set("interests_interest",  sr.get("interests_interest")).build());
      }
    }

    Assert.assertEquals(expected, actual);

    validateMetric(11, appId, "join.records.in");
    validateMetric(expected.size(), appId, "join.records.out");
  }

}

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
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.batch.joiner.MockAutoJoiner;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
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
    workflowManager.startAndWaitForRun(args, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(expected, new HashSet<>(outputRecords));

    validateMetric(5, appId, "join.records.in");
    validateMetric(expected.size(), appId, "join.records.out");
    validateMetric(1, appId, "sink." + MockSink.INITIALIZED_COUNT_METRIC);
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
                       expected, Engine.SPARK);
  }

  @Test
  public void testTripleAutoLeftRequiredJoin() throws Exception {
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

    testTripleAutoJoin(Collections.singletonList("purchases"), expected, Engine.SPARK);
    testTripleAutoJoin(Collections.singletonList("purchases"), expected, Engine.MAPREDUCE);
  }

  @Test
  public void testTripleAutoMiddleRequiredJoin() throws Exception {
    Schema expectedSchema = Schema.recordOf(
      "purchases.users.interests",
      Schema.Field.of("purchases_region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("purchases_purchase_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("purchases_user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("users_region", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("users_user_id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("users_name", Schema.of(Schema.Type.STRING)),
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
                   .set("users_region", "eu")
                   .set("users_user_id", 0)
                   .set("users_name", "alyce").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("users_region", "us")
                   .set("users_user_id", 1)
                   .set("users_name", "bob")
                   .set("interests_region", "us")
                   .set("interests_user_id", 1)
                   .set("interests_interest", "gardening").build());

    testTripleAutoJoin(Collections.singletonList("users"), expected, Engine.SPARK);
    testTripleAutoJoin(Collections.singletonList("users"), expected, Engine.MAPREDUCE);
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

    testTripleAutoJoin(Arrays.asList("users", "interests"), expected, Engine.SPARK);
    testTripleAutoJoin(Arrays.asList("users", "interests"), expected, Engine.MAPREDUCE);
  }

  private void testTripleAutoJoin(List<String> required, Set<StructuredRecord> expected,
                                  Engine engine) throws Exception {
    testTripleAutoJoin(required, Collections.emptyList(), expected, engine);
  }

  private void testTripleAutoJoin(List<String> required, List<String> broadcast,
                                  Set<StructuredRecord> expected, Engine engine) throws Exception {
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
      .addStage(new ETLStage("join", MockAutoJoiner.getPlugin(Arrays.asList("purchases", "users", "interests"),
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
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(expected, new HashSet<>(outputRecords));

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
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(output);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(expected, new HashSet<>(outputRecords));
  }

  @Test
  public void testLeftOuterAutoJoinWithMacros() throws Exception {
    Schema expectedSchema;
    if (Compat.SPARK_COMPAT.equals(SparkCompat.SPARK1_2_10.getCompat())) {
      // spark1 has a schema bug, so it has more nullable fields than needed.
      expectedSchema = Schema.recordOf(
        "Record0",
        Schema.Field.of("region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("purchase_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
        Schema.Field.of("user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
        Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    } else {
      expectedSchema = Schema.recordOf(
        "Record0",
        Schema.Field.of("region", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("purchase_id", Schema.of(Schema.Type.INT)),
        Schema.Field.of("user_id", Schema.of(Schema.Type.INT)),
        Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }

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
    Schema expectedSchema;
    if (Compat.SPARK_COMPAT.equals(SparkCompat.SPARK1_2_10.getCompat())) {
      // spark1 has a schema bug, so it has more nullable fields than needed.
      expectedSchema = Schema.recordOf(
        "Record0",
        Schema.Field.of("region", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("purchase_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
        Schema.Field.of("user_id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
        Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    } else {
      expectedSchema = Schema.recordOf(
        "Record0",
        Schema.Field.of("region", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("purchase_id", Schema.of(Schema.Type.INT)),
        Schema.Field.of("user_id", Schema.of(Schema.Type.INT)),
        Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    }
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
    workflowManager.startAndWaitForRun(runtimeArgs, ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

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
    Assert.assertEquals(expected, getMetricsManager().getTotalMetric(tags, "user." + metric));
  }
}

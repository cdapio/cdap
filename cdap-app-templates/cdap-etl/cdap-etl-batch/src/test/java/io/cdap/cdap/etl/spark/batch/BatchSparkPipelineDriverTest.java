/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.spark.SparkCollection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BatchSparkPipelineDriverTest {

  private static final String STAGE_NAME = "some-stage";
  private BatchSparkPipelineDriver driver;
  private BatchSQLEngineAdapter adapter;

  @Before
  public void setUp() {
    adapter = mock(BatchSQLEngineAdapter.class);
    when(adapter.canJoin(anyString(), any(JoinDefinition.class))).thenReturn(true);
    driver = new BatchSparkPipelineDriver(adapter);
  }

  @Test
  public void testSQLEngineDoesNotSupportJoin() {
    when(adapter.canJoin(anyString(), any(JoinDefinition.class))).thenReturn(false);

    List<JoinStage> noneBroadcast = Arrays.asList(
      JoinStage.builder("a", null).setBroadcast(false).build(),
      JoinStage.builder("b", null).setBroadcast(false).build(),
      JoinStage.builder("c", null).setBroadcast(false).build()
    );

    JoinDefinition joinDefinition = mock(JoinDefinition.class);
    doReturn(noneBroadcast).when(joinDefinition).getStages();

    Map<String, SparkCollection<Object>> collections = new HashMap<>();
    collections.put("a", mock(RDDCollection.class));
    collections.put("b", mock(RDDCollection.class));
    collections.put("c", mock(RDDCollection.class));

    Assert.assertFalse(driver.canJoinOnSQLEngine(STAGE_NAME, joinDefinition, collections));
  }

  @Test
  public void testShouldJoinOnSQLEngineWithoutBroadcast() {
    List<JoinStage> noneBroadcast = Arrays.asList(
      JoinStage.builder("a", null).setBroadcast(false).build(),
      JoinStage.builder("b", null).setBroadcast(false).build(),
      JoinStage.builder("c", null).setBroadcast(false).build()
    );

    JoinDefinition joinDefinition = mock(JoinDefinition.class);
    doReturn(noneBroadcast).when(joinDefinition).getStages();

    Map<String, SparkCollection<Object>> collections = new HashMap<>();
    collections.put("a", mock(RDDCollection.class));
    collections.put("b", mock(RDDCollection.class));
    collections.put("c", mock(RDDCollection.class));

    Assert.assertTrue(driver.canJoinOnSQLEngine(STAGE_NAME, joinDefinition, collections));
  }

  @Test
  public void testShouldNotJoinOnSQLEngineWithBroadcast() {
    List<JoinStage> noneBroadcast = Arrays.asList(
      JoinStage.builder("a", null).setBroadcast(false).build(),
      JoinStage.builder("b", null).setBroadcast(false).build(),
      JoinStage.builder("c", null).setBroadcast(true).build()
    );

    JoinDefinition joinDefinition = mock(JoinDefinition.class);
    doReturn(noneBroadcast).when(joinDefinition).getStages();

    Map<String, SparkCollection<Object>> collections = new HashMap<>();
    collections.put("a", mock(RDDCollection.class));
    collections.put("b", mock(RDDCollection.class));
    collections.put("c", mock(RDDCollection.class));

    Assert.assertFalse(driver.canJoinOnSQLEngine(STAGE_NAME, joinDefinition, collections));
  }

  @Test
  public void testShouldJoinOnSQLEngineWithBroadcastAndAlreadyPushedCollection() {
    List<JoinStage> noneBroadcast = Arrays.asList(
      JoinStage.builder("a", null).setBroadcast(false).build(),
      JoinStage.builder("b", null).setBroadcast(false).build(),
      JoinStage.builder("c", null).setBroadcast(true).build()
    );

    JoinDefinition joinDefinition = mock(JoinDefinition.class);
    doReturn(noneBroadcast).when(joinDefinition).getStages();

    Map<String, SparkCollection<Object>> collections = new HashMap<>();
    collections.put("a", mock(SQLEngineCollection.class));
    collections.put("b", mock(RDDCollection.class));
    collections.put("c", mock(RDDCollection.class));

    Assert.assertTrue(driver.canJoinOnSQLEngine(STAGE_NAME, joinDefinition, collections));
  }
}

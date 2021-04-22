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

import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.spark.SparkCollection;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.doReturn;

public class BatchSparkPipelineDriverTest {

  @Test
  public void testShouldJoinOnSQLEngineWithoutBroadcast() {
    List<JoinStage> noneBroadcast = Arrays.asList(
      JoinStage.builder("a", null).setBroadcast(false).build(),
      JoinStage.builder("b", null).setBroadcast(false).build(),
      JoinStage.builder("c", null).setBroadcast(false).build()
    );

    JoinDefinition joinDefinition = Mockito.mock(JoinDefinition.class);
    doReturn(noneBroadcast).when(joinDefinition).getStages();

    Map<String, SparkCollection<Object>> collections = new HashMap<>();
    collections.put("a", Mockito.mock(RDDCollection.class));
    collections.put("b", Mockito.mock(RDDCollection.class));
    collections.put("c", Mockito.mock(RDDCollection.class));

    Assert.assertTrue(BatchSparkPipelineDriver.canJoinOnSQLEngine(joinDefinition, collections));
  }

  @Test
  public void testShouldNotJoinOnSQLEngineWithBroadcast() {
    List<JoinStage> noneBroadcast = Arrays.asList(
      JoinStage.builder("a", null).setBroadcast(false).build(),
      JoinStage.builder("b", null).setBroadcast(false).build(),
      JoinStage.builder("c", null).setBroadcast(true).build()
    );

    JoinDefinition joinDefinition = Mockito.mock(JoinDefinition.class);
    doReturn(noneBroadcast).when(joinDefinition).getStages();

    Map<String, SparkCollection<Object>> collections = new HashMap<>();
    collections.put("a", Mockito.mock(RDDCollection.class));
    collections.put("b", Mockito.mock(RDDCollection.class));
    collections.put("c", Mockito.mock(RDDCollection.class));

    Assert.assertFalse(BatchSparkPipelineDriver.canJoinOnSQLEngine(joinDefinition, collections));
  }

  @Test
  public void testShouldJoinOnSQLEngineWithBroadcastAndAlreadyPushedCollection() {
    List<JoinStage> noneBroadcast = Arrays.asList(
      JoinStage.builder("a", null).setBroadcast(false).build(),
      JoinStage.builder("b", null).setBroadcast(false).build(),
      JoinStage.builder("c", null).setBroadcast(true).build()
    );

    JoinDefinition joinDefinition = Mockito.mock(JoinDefinition.class);
    doReturn(noneBroadcast).when(joinDefinition).getStages();

    Map<String, SparkCollection<Object>> collections = new HashMap<>();
    collections.put("a", Mockito.mock(SQLEngineCollection.class));
    collections.put("b", Mockito.mock(RDDCollection.class));
    collections.put("c", Mockito.mock(RDDCollection.class));

    Assert.assertTrue(BatchSparkPipelineDriver.canJoinOnSQLEngine(joinDefinition, collections));
  }
}

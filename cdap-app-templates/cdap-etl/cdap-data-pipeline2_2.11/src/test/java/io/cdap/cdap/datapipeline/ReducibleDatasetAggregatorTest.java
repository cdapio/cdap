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

package io.cdap.cdap.datapipeline;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.common.Constants;
import org.junit.Test;

import java.util.Map;

public class ReducibleDatasetAggregatorTest extends ReducibleAggregatorTestBase {
  /**
   * Settings to test with dataset aggregation.
   */
  private static final Map<String, String> DATASET_AGGREGATE = ImmutableMap.of(
    Constants.DATASET_AGGREGATE_ENABLED, "true",
    Constants.DATASET_AGGREGATE_IGNORE_PARTITIONS, "false"
  );

  @Test
  public void testSimpleAggregator() throws Exception {
    testSimpleAggregator(Engine.SPARK, DATASET_AGGREGATE);
  }

  @Test
  public void testFieldCountAgg() throws Exception {
    testFieldCountAgg(Engine.SPARK, DATASET_AGGREGATE);
  }
}

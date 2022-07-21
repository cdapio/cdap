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

import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.common.Constants;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class ReducibleAggregatorTest extends ReducibleAggregatorTestBase {
  /**
   * Common arguments for spark 1 and 2 to test without dataset aggregation
   */
  private static final Map<String, String> DEFAULT_ARGUMENTS = Collections.singletonMap(
    Constants.DATASET_AGGREGATE_ENABLED, "false"
  );

  @Test
  public void testSimpleAggregator() throws Exception {
    testSimpleAggregator(Engine.MAPREDUCE, DEFAULT_ARGUMENTS);
    testSimpleAggregator(Engine.SPARK, DEFAULT_ARGUMENTS);
  }

  @Test
  public void testFieldCountAgg() throws Exception {
    testFieldCountAgg(Engine.MAPREDUCE, DEFAULT_ARGUMENTS);
    testFieldCountAgg(Engine.SPARK, DEFAULT_ARGUMENTS);
  }

}

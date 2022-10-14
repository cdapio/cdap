/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.app.runtime.spark.distributed.SparkTwillRunnable;
import io.cdap.cdap.common.lang.FilterClassLoader;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SparkResourceFilter}.
 */
public class SparkResourceFilterTest {
  @Test
  public void testSparkAPIClassPassesSparkFilter() {
    FilterClassLoader.Filter filter = new SparkResourceFilter();
    Assert.assertTrue(filter.acceptResource(JavaSparkExecutionContext.class.getName().replace('.', '/')));
  }

  @Test
  public void testSparkTwillRunnablePassesSparkFilter() {
    FilterClassLoader.Filter filter = new SparkResourceFilter();
    Assert.assertTrue(filter.acceptResource(SparkTwillRunnable.class.getName().replace('.', '/')));
  }
}

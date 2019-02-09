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

package co.cask.cdap.gateway.discovery;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link RouteFallbackStrategy}.
 */
public class RouteFallbackStrategyTest {

  @Test
  public void testEnumCreation() throws Exception {
    Assert.assertEquals(RouteFallbackStrategy.RANDOM, RouteFallbackStrategy.valueOfRouteFallbackStrategy(""));
    Assert.assertEquals(RouteFallbackStrategy.RANDOM, RouteFallbackStrategy.valueOfRouteFallbackStrategy(null));
    Assert.assertEquals(RouteFallbackStrategy.LARGEST, RouteFallbackStrategy.valueOfRouteFallbackStrategy("largest"));
    Assert.assertEquals(RouteFallbackStrategy.LARGEST, RouteFallbackStrategy.valueOfRouteFallbackStrategy("Largest"));
    Assert.assertEquals(RouteFallbackStrategy.SMALLEST, RouteFallbackStrategy.valueOfRouteFallbackStrategy("smallest"));
    Assert.assertEquals(RouteFallbackStrategy.SMALLEST, RouteFallbackStrategy.valueOfRouteFallbackStrategy("Smallest"));
    Assert.assertEquals(RouteFallbackStrategy.RANDOM, RouteFallbackStrategy.valueOfRouteFallbackStrategy("random"));
    Assert.assertEquals(RouteFallbackStrategy.RANDOM, RouteFallbackStrategy.valueOfRouteFallbackStrategy("Random"));
    Assert.assertEquals(RouteFallbackStrategy.RANDOM, RouteFallbackStrategy.valueOfRouteFallbackStrategy("nothing"));
    Assert.assertEquals(RouteFallbackStrategy.DROP, RouteFallbackStrategy.valueOfRouteFallbackStrategy("drop"));
    Assert.assertEquals(RouteFallbackStrategy.DROP, RouteFallbackStrategy.valueOfRouteFallbackStrategy("Drop"));
  }
}

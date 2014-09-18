/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.metrics.query;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TimeseriesIdTest {

  @Test
  public void testEquality() {
    TimeseriesId id1 = new TimeseriesId("app.f.flow.flowlet.0", "process.events", null, "0");
    TimeseriesId id2 = new TimeseriesId("app.f.flow.flowlet.0", "process.events", null, "0");
    Assert.assertTrue(id1.equals(id2));
    Assert.assertTrue(id2.equals(id1));
    Assert.assertEquals(id1.hashCode(), id2.hashCode());

    id1 = new TimeseriesId("app.f.flow.flowlet.0", "process.events", "tag1", "0");
    id2 = new TimeseriesId("app.f.flow.flowlet.0", "process.events", "tag1", "0");
    Assert.assertTrue(id1.equals(id2));
    Assert.assertTrue(id2.equals(id1));
    Assert.assertEquals(id1.hashCode(), id2.hashCode());
  }
}

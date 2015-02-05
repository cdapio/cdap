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

package co.cask.cdap.metrics.data;

import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryMetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryOrderedTableService;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class MetricsEntityCodecTest {

  @Test
  public void testCodec() {
    InMemoryOrderedTableService.create("MetricEntityCodecTest");
    MetricsTable table = new InMemoryMetricsTable("MetricEntityCodecTest");
    MetricsEntityCodec codec = new MetricsEntityCodec(new EntityTable(table), 4, 2, 2);

    Assert.assertEquals("app.f.flow.flowlet", codec.decode(MetricsEntityType.CONTEXT,
                                                           codec.encode(MetricsEntityType.CONTEXT,
                                                                        "app.f.flow.flowlet")));
    Assert.assertEquals("app.f.flow2.flowlet2", codec.decode(MetricsEntityType.CONTEXT,
                                                             codec.encode(MetricsEntityType.CONTEXT,
                                                                          "app.f.flow2.flowlet2")));

    Assert.assertEquals("data.in", codec.decode(MetricsEntityType.METRIC,
                                                codec.encode(MetricsEntityType.METRIC, "data.in")));
    Assert.assertEquals("data.out", codec.decode(MetricsEntityType.METRIC,
                                                 codec.encode(MetricsEntityType.METRIC, "data.out")));

    Assert.assertEquals("23423-3235-3453", codec.decode(MetricsEntityType.RUN,
                                                        codec.encode(MetricsEntityType.RUN, "23423-3235-3453")));
  }
}

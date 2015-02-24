/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryMetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryOrderedTableService;
import co.cask.cdap.metrics.MetricsConstants;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
/**
 *
 */
public class DataMigrationTest {


  private byte[] getKey(MetricsEntityCodec entityCodec,
                        String context, String runId, String metric, String tag, int timeBase) {
    Preconditions.checkArgument(context != null, "Context cannot be null.");
    Preconditions.checkArgument(runId != null, "RunId cannot be null.");
    Preconditions.checkArgument(metric != null, "Metric cannot be null.");

    return Bytes.concat(entityCodec.paddedEncode(MetricsEntityType.CONTEXT, context, 0),
                        entityCodec.paddedEncode(MetricsEntityType.METRIC, metric, 0),
                        entityCodec.paddedEncode(MetricsEntityType.TAG, tag == null ?
                          MetricsConstants.EMPTY_TAG : tag, 0),
                        Bytes.toBytes(timeBase),
                        entityCodec.paddedEncode(MetricsEntityType.RUN, runId, 0));
  }

  @Test
  public void testSimpleMigration() throws Exception {
    InMemoryOrderedTableService.create("testMigrate");
    MetricsTable table = new InMemoryMetricsTable("testMigrate");

    EntityTable entityTable = new EntityTable(table, 16777215);
    MetricsEntityCodec codec = new MetricsEntityCodec(entityTable, MetricsConstants.DEFAULT_CONTEXT_DEPTH,
                                                      MetricsConstants.DEFAULT_METRIC_DEPTH,
                                                      MetricsConstants.DEFAULT_TAG_DEPTH);
    // 1. Read metrics {context, metric} construct row-key using codec
    // 2. With the rowKey, call constructMetricValue on the rowKey
    // 3. With the metricValue, get Context name (tag-values) and metric name and test they are
    // equal to the initial values.

    String context = "zap.f.xflow.xflowlet";
    ImmutableMap<String, String> tagValues = ImmutableMap.<String, String>builder().put("namespace", "default")
      .put("app", "zap")
      .put("flow", "xflow")
      .put("flowlet", "xflowlet")
      .put("run", "0")
      .build();

    String metric = "input.reads";
    byte[] rowKey = getKey(codec, context, "0", metric, null, 10);
    //DataMigration26 migration26 = new DataMigration26(codec);
    //MetricValue value = migration26.getMetricValue(rowKey, "system");
    //Assert.assertEquals(tagValues, value.getTags());
    //Assert.assertEquals(metric, value.getName());
  }
}

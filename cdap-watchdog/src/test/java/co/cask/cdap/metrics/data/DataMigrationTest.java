package co.cask.cdap.metrics.data;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryMetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryOrderedTableService;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.transport.MetricValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Map;

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
                        entityCodec.paddedEncode(MetricsEntityType.TAG, tag == null ? MetricsConstants.EMPTY_TAG : tag, 0),
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
    // 3. With the metricValue, get Context name (tag-values) and metric name and test they are equal to the initial values.

    String context = "zap.f.xflow.xflowlet" ;
    ImmutableMap<String, String> tagValues = ImmutableMap.<String, String> builder().put("ns", "default")
      .put("app", "zap")
      .put("ptp", "f")
      .put("prg", "xflow")
      .put("flt", "xflowlet")
      .put("run", "0")
      .build();

    String metric = "input.reads";
    byte[] rowKey = getKey(codec, context, "0", metric, null, 10);
    DataMigration26 migration26 = new DataMigration26(codec);
    MetricValue value = migration26.getMetricValue(rowKey, "system");
    Assert.assertEquals(tagValues, value.getTags());
    Assert.assertEquals(metric, value.getName());
  }
}

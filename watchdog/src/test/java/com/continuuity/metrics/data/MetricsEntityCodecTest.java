package com.continuuity.metrics.data;

import com.continuuity.data2.OperationException;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryMetricsTableClient;
import com.continuuity.data2.dataset.lib.table.inmemory.InMemoryOcTableService;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class MetricsEntityCodecTest {

  @Test
  public void testCodec() throws OperationException {
    InMemoryOcTableService.create("MetricEntityCodecTest");
    MetricsTable table = new InMemoryMetricsTableClient("MetricEntityCodecTest");
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

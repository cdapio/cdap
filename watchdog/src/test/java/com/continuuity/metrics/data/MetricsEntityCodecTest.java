package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.engine.memory.MemoryOVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class MetricsEntityCodecTest {

  @Test
  public void testCodec() throws OperationException {
    OrderedVersionedColumnarTable table = MemoryOVCTableHandle.getInstance()
                                                              .getTable(Bytes.toBytes("MetricEntityCodecTest"));
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

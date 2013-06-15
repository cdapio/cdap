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
public class MetricEntityCodecTest {

  @Test
  public void testCodec() throws OperationException {
    OrderedVersionedColumnarTable table = MemoryOVCTableHandle.getInstance()
                                                              .getTable(Bytes.toBytes("MetricEntityCodecTest"));
    MetricEntityCodec codec = new MetricEntityCodec(new EntityTable(table), 4, 2, 2);

    Assert.assertEquals("app.f.flow.flowlet", codec.decode(MetricEntityType.CONTEXT,
                                                           codec.encode(MetricEntityType.CONTEXT,
                                                                        "app.f.flow.flowlet")));
    Assert.assertEquals("app.f.flow2.flowlet2", codec.decode(MetricEntityType.CONTEXT,
                                                             codec.encode(MetricEntityType.CONTEXT,
                                                                          "app.f.flow2.flowlet2")));

    Assert.assertEquals("data.in", codec.decode(MetricEntityType.METRIC,
                                                codec.encode(MetricEntityType.METRIC, "data.in")));
    Assert.assertEquals("data.out", codec.decode(MetricEntityType.METRIC,
                                                 codec.encode(MetricEntityType.METRIC, "data.out")));
  }
}

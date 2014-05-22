package com.continuuity.examples.incrementbench;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 *
 */
public class IncrementConsumer extends AbstractFlowlet {
  private static final byte[] COL = Bytes.toBytes("c");

  @UseDataSet("incrementTable")
  private Table table;

  @ProcessInput
  public void process(byte[] row) {
    table.increment(row, COL, 1);
  }
}

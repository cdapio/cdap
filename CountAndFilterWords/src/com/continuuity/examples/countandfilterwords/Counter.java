package com.continuuity.examples.countandfilterwords;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.annotation.Process;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

public class Counter extends AbstractFlowlet {

  private static Logger LOG = LoggerFactory.getLogger(Counter.class);

  @UseDataSet(CountAndFilterWords.tableName)
  KeyValueTable counters;

  @Process("counts")
  public void process(String counter) throws OperationException {
    LOG.debug("Incrementing counter " + counter);
    this.counters.increment(Bytes.toBytes(counter), 1L);
  }
}

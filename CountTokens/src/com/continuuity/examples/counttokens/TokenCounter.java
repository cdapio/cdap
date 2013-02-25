package com.continuuity.examples.counttokens;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

public class TokenCounter extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(TokenCounter.class);

  @UseDataSet(CountTokens.tableName)
  private KeyValueTable counters;

  public void process(String token) throws OperationException {
    LOG.debug(this.getContext().getName() + ": Received and " +
        "incrementing count for token " + token);
    this.counters.increment(Bytes.toBytes(token), 1);
  }

}
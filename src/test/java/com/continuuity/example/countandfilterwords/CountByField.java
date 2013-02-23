package com.continuuity.example.countandfilterwords;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

import java.util.Map;

public class CountByField extends AbstractFlowlet {

  @UseDataSet(Common.counterTableName)
  KeyValueTable counters;

  public CountByField() {
    super("countByField");
  }

  public void process(Map<String, String> tupleIn) {
    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tupleIn);
    }

    String token = tupleIn.get("word");
    if (token == null) return;
    String field = tupleIn.get("field");
    if (field != null) token = field + ":" + token;

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Incrementing for " + token);
    }
    try {
      this.counters.increment(token.getBytes(), 1L);
    } catch (OperationException e) {
      throw new RuntimeException(e);
    }
  }
}

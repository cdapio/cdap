package com.continuuity.examples.counttokens;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class UpperCaser extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(UpperCaser.class);

  @Output("upperOut")
  private OutputEmitter<String> upperOut;

  public UpperCaser() {
    super("upper");
  }

  public void process(Map<String, String> tupleIn) {
    LOG.debug(this.getContext().getName() + ": Received tuple " + tupleIn);

    String word = tupleIn.get("word");
    if (word == null) return;
    String upper = word.toUpperCase();

    LOG.debug(this.getContext().getName() + ": Emitting word " + upper);

    upperOut.emit(upper);
  }
}

package com.continuuity.examples.countrandom;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

public class NumberSplitter extends AbstractFlowlet {
  private OutputEmitter<Integer> output;

  public NumberSplitter() {
    super("NumberSplitter");
  }

  public void process(Integer number)  {
    output.emit(new Integer(number % 10000));
    output.emit(new Integer(number % 1000));
    output.emit(new Integer(number % 100));
    output.emit(new Integer(number % 10));
  }
}

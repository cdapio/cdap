package com.continuuity.examples.countoddandeven;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

/**
 * Based on the whether number is odd or even it puts the number on
 * different streams.
 */
public class OddOrEven extends AbstractFlowlet {

  @Output("evenNumbers")
  private OutputEmitter<Integer> evenOutput;

  @Output("oddNumbers")
  private OutputEmitter<Integer> oddOutput;

  public void process(Integer number) {
    if(number.intValue() % 2 == 0) {
      evenOutput.emit(number);
    } else {
      oddOutput.emit(number);
    }
  }
}

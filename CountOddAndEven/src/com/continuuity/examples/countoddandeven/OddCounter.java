package com.continuuity.examples.countoddandeven;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * Counts number of Odd tuples.
 */
public class OddCounter extends AbstractFlowlet {

  int count = 0;

  @ProcessInput("oddNumbers")
  public void process(Integer number) {
    count++;
  }

}

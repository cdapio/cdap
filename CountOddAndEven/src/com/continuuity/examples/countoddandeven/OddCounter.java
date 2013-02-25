package com.continuuity.examples.countoddandeven;

import com.continuuity.api.annotation.Process;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * Counts number of Odd tuples.
 */
public class OddCounter extends AbstractFlowlet {

  int count = 0;

  @Process("oddNumbers")
  public void process() {
    count++;
  }

}

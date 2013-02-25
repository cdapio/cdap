package com.continuuity.examples.countoddandeven;

import com.continuuity.api.annotation.Process;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * Counts number of even tuples.
 */
public class EvenCounter extends AbstractFlowlet {

  int count = 0;

  @Process("evenNumbers")
  public void process() {
    count++;
  }
}

package com.continuuity.data.operation.ttqueue.randomized;

/**
*
*/
public interface SelectionFunction {
  int select(int value);
  boolean isProbable(float probability);
}

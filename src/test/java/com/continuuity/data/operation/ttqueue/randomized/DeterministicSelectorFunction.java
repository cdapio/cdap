package com.continuuity.data.operation.ttqueue.randomized;

/**
*
*/
@SuppressWarnings("UnusedDeclaration")
public class DeterministicSelectorFunction implements SelectionFunction {
  @Override
  public int select(int value) {
    return value;
  }

  @Override
  public boolean isProbable(float probability) {
    return false;
  }
}

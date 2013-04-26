package com.continuuity.data.operation.ttqueue.randomized;

import java.util.Random;

/**
*
*/
public class RandomSelectionFunction implements SelectionFunction {
  private final Random random = new Random(System.currentTimeMillis());

  @Override
  public int select(int value) {
    return random.nextInt(value);
  }

  @Override
  public boolean isProbable(float probability) {
    return random.nextFloat() < probability;
  }
}

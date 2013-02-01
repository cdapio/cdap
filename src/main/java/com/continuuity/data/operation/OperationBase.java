package com.continuuity.data.operation;

import java.util.Random;

/**
 *
 */
public class OperationBase {
  public static Random random = new Random();

  public static long getId() {
    return random.nextLong();
  }
}

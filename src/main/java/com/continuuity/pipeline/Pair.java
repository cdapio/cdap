package com.continuuity.pipeline;

/**
 *
 */
public final class Pair<X,Y> {
  private final X x;
  private final Y y;
  public Pair(final X x, final Y y) {
    this.x = x;
    this.y = y;
  }
  public X getFirst() {
    return x;
  }

  public Y getSecond() {
    return y;
  }
}

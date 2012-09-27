package com.continuuity.metrics2.common;

/**
 *
 */
public interface Datnum<T> {
  public double getValue();
  public <T> T getKey();
  public Long getTimestamp();
}

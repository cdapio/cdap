package com.continuuity.common.conflake;

/**
 * Conflake
 */
public interface Conflake {
  public boolean start();
  public long next();
  public void stop();
}

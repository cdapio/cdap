package com.continuuity.data.engine;

public class SimpleReadPointer implements ReadPointer {

  private final long maxVersion;

  /**
   * 
   * @param maxVersion inclusive
   */
  public SimpleReadPointer(long maxVersion) {
    this.maxVersion = maxVersion;
  }

  @Override
  public boolean isVisible(long txid) {
    return txid <= maxVersion;
  }

  @Override
  public long getMaximum() {
    return maxVersion;
  }

}

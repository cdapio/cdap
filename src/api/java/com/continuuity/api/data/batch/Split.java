package com.continuuity.api.data.batch;

public abstract class Split {
  public long getLength() throws InterruptedException {
    // by default assume that the size of each split is roughly the same
    return 0;
  }

  public String[] getLocations() throws InterruptedException {
    // by default we don't care about the data locality
    return new String[0];
  }
}

package com.continuuity.api.data;

import java.util.List;

public class ReadKeys implements ReadOperation<List<byte[]>> {

  private final int offset;
  private final int limit;

  private List<byte[]> result;

  public ReadKeys(int offset, int limit) {
    this.offset = offset;
    this.limit = limit;
  }

  @Override
  public void setResult(List<byte[]> result) {
    this.result = result;
  }

  @Override
  public List<byte[]> getResult() {
    return this.result;
  }

  public int getOffset() {
    return this.offset;
  }

  public int getLimit() {
    return this.limit;
  }

}

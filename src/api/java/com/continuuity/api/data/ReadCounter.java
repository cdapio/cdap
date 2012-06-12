package com.continuuity.api.data;


public class ReadCounter implements ReadOperation<Long> {

  private byte [] key;
 
  public ReadCounter(final byte [] key) {
    this.key = key;
  }

  public byte [] getKey() {
    return this.key;
  }

  @Override
  public Long getResult() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setResult(Long t) {
    // TODO Auto-generated method stub
    
  }
}

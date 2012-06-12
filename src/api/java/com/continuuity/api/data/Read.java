package com.continuuity.api.data;


public class Read implements ReadOperation<byte[]> {

  private final byte [] key;

  public Read(final byte [] key) {
    this.key = key;
  }

  public byte [] getKey() {
    return this.key;
  }

  @Override
  public byte [] getResult() {
    return null;
  }

  @Override
  public void setResult(byte[] t) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Read{key=");
    sb.append(new String(key));
    sb.append("}");
    return sb.toString();
  }
}

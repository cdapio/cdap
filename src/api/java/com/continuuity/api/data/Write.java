package com.continuuity.api.data;


public class Write implements WriteOperation {

  private byte [] key;
  private byte [] value;
 
  public Write(final byte [] key, final byte [] value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public byte [] getKey() {
    return this.key;
  }
 
  public byte [] getValue() {
    return this.value;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Write{key=");
    sb.append(new String(key));
    sb.append(", value=");
    sb.append(new String(value));
    sb.append("}");
    return sb.toString();
  }

  @Override
  public int getPriority() {
    return 1;
  }
}

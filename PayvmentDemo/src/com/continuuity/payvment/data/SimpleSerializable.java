package com.continuuity.payvment.data;

public interface SimpleSerializable {

  public byte [] toBytes();

  public SimpleSerializable fromBytes(byte [] bytes);

}

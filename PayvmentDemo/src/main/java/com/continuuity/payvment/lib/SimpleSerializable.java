package com.continuuity.payvment.lib;

public interface SimpleSerializable {

  public byte [] toBytes();

  public SimpleSerializable fromBytes(byte [] bytes);

}

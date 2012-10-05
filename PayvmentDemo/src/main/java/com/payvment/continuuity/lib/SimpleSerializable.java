package com.payvment.continuuity.lib;

public interface SimpleSerializable {

  public byte [] toBytes();

  public SimpleSerializable fromBytes(byte [] bytes);

}

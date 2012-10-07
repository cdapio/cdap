package com.continuuity.api.data.lib;

public interface SimpleSerializable {

  public byte [] toBytes();

  public SimpleSerializable fromBytes(byte [] bytes);

}

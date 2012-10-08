package com.continuuity.metrics2.common;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: nmotgi
 * Date: 10/7/12
 * Time: 5:45 PM
 * To change this template use File | Settings | File Templates.
 */
public interface KVStore {
  public void put(byte[] key, byte[] value) throws Exception;
  public byte[] get(byte[] key) throws Exception;
}

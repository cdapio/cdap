package com.continuuity.metrics2.temporaldb.internal;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: nmotgi
 * Date: 10/7/12
 * Time: 5:45 PM
 * To change this template use File | Settings | File Templates.
 */
class Bytes {

  public static long getLong(byte[] bytes) {
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    return bb.getLong();
  }

  public static byte[] fromLong(long value) {
    ByteBuffer bb = ByteBuffer.allocate(8);
    bb.putLong(value);
    return bb.array();
  }

  public static int getInt(byte[] bytes) {
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    return bb.getInt();
  }

  public static byte[] fromInt(int value) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(value);
    return bb.array();
  }

  public static byte[] fromDouble(double value) {
    ByteBuffer bb = ByteBuffer.allocate(8);
    bb.putDouble(value);
    return bb.array();
  }

  public static double getDouble(byte[] data) {
    ByteBuffer bb = ByteBuffer.wrap(data);
    return bb.getDouble();
  }

  public static String toHex(byte[] bytes){
    BigInteger bi = new BigInteger(1, bytes);
    return String.format("%0" + (bytes.length << 1) + "X", bi);
  }
}
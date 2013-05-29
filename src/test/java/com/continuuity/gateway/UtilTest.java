package com.continuuity.gateway;

import com.continuuity.gateway.util.Util;
import org.junit.Assert;
import org.junit.Test;

public class UtilTest {

  static final byte ff = (byte)0xff;
  static final byte x7f = (byte)0x7f;
  static final byte x80 = (byte)0x80;

  @Test
  public void testLongToBytes() {
    Assert.assertArrayEquals(new byte[]
        { 0,0,0,0,0,0,0,0 }, Util.longToBytes(0L));
    Assert.assertArrayEquals(new byte[]
        { 0,0,0,0,0,0,0,1 }, Util.longToBytes(1L));
    Assert.assertArrayEquals(new byte[]
        { x7f,ff,ff,ff,ff,ff,ff,ff }, Util.longToBytes(Long.MAX_VALUE));
    Assert.assertArrayEquals(new byte[]
        { ff,ff,ff,ff,ff,ff,ff,ff }, Util.longToBytes(-1L));
    Assert.assertArrayEquals(new byte[]
        { x80,0,0,0,0,0,0,0 }, Util.longToBytes(Long.MIN_VALUE));
  }

  @Test
  public void testBytesToLong() {
    Assert.assertEquals(
        0, Util.bytesToLong(new byte[] { }));
    Assert.assertEquals(
        0, Util.bytesToLong(new byte[] { 0,0,0}));
    Assert.assertEquals(
        0, Util.bytesToLong(new byte[] { 0,0,0,0,0,0,0,0 }));
    Assert.assertEquals(
        0, Util.bytesToLong(new byte[] { 1,0,0,0,0,0,0,0,0 }));
    Assert.assertEquals(
        0, Util.bytesToLong(new byte[] { ff,ff,0,0,0,0,0,0,0,0 }));
    Assert.assertEquals(
        1, Util.bytesToLong(new byte[] { 0,0,0,0,0,0,0,1 }));
    Assert.assertEquals(
        1, Util.bytesToLong(new byte[] { 0,0,0,0,0,0,0,0,0,0,1 }));
    Assert.assertEquals(
        1, Util.bytesToLong(new byte[] { 0,0,1 }));
    Assert.assertEquals(
        1, Util.bytesToLong(new byte[] { ff,1,ff,0,0,0,0,0,0,0,1 }));
    Assert.assertEquals(
        Long.MAX_VALUE,
        Util.bytesToLong(new byte[] { x7f,ff,ff,ff,ff,ff,ff,ff }));
    Assert.assertEquals(
        Long.MIN_VALUE,
        Util.bytesToLong(new byte[] { x80,0,0,0,0,0,0,0 }));
    Assert.assertEquals(
        Long.MAX_VALUE,
        Util.bytesToLong(new byte[] { 1,1,1,x7f,ff,ff,ff,ff,ff,ff,ff }));
    Assert.assertEquals(
        Long.MIN_VALUE,
        Util.bytesToLong(new byte[] { 0,0,0,x80,0,0,0,0,0,0,0 }));
  }

}

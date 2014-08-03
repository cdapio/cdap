/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway;

import co.cask.cdap.gateway.util.Util;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class UtilTest {

  static final byte FF = (byte) 0xff;
  static final byte X7F = (byte) 0x7f;
  static final byte X80 = (byte) 0x80;

  @Test
  public void testLongToBytes() {
    Assert.assertArrayEquals(new byte[]
        { 0, 0, 0, 0, 0, 0, 0, 0 }, Util.longToBytes(0L));
    Assert.assertArrayEquals(new byte[]
        { 0, 0, 0, 0, 0, 0, 0, 1 }, Util.longToBytes(1L));
    Assert.assertArrayEquals(new byte[]
        { X7F, FF, FF, FF, FF, FF, FF, FF}, Util.longToBytes(Long.MAX_VALUE));
    Assert.assertArrayEquals(new byte[]
        { FF, FF, FF, FF, FF, FF, FF, FF}, Util.longToBytes(-1L));
    Assert.assertArrayEquals(new byte[]
        { X80, 0, 0, 0, 0, 0, 0, 0 }, Util.longToBytes(Long.MIN_VALUE));
  }

  @Test
  public void testBytesToLong() {
    Assert.assertEquals(
        0, Util.bytesToLong(new byte[] { }));
    Assert.assertEquals(
        0, Util.bytesToLong(new byte[] { 0, 0, 0}));
    Assert.assertEquals(
        0, Util.bytesToLong(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }));
    Assert.assertEquals(
        0, Util.bytesToLong(new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0 }));
    Assert.assertEquals(
        0, Util.bytesToLong(new byte[] { FF, FF, 0, 0, 0, 0, 0, 0, 0, 0 }));
    Assert.assertEquals(
        1, Util.bytesToLong(new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }));
    Assert.assertEquals(
        1, Util.bytesToLong(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }));
    Assert.assertEquals(
        1, Util.bytesToLong(new byte[] { 0, 0, 1 }));
    Assert.assertEquals(
        1, Util.bytesToLong(new byte[] { FF, 1, FF, 0, 0, 0, 0, 0, 0, 0, 1 }));
    Assert.assertEquals(
        Long.MAX_VALUE,
        Util.bytesToLong(new byte[] { X7F, FF, FF, FF, FF, FF, FF, FF}));
    Assert.assertEquals(
        Long.MIN_VALUE,
        Util.bytesToLong(new byte[] { X80, 0, 0, 0, 0, 0, 0, 0 }));
    Assert.assertEquals(
        Long.MAX_VALUE,
        Util.bytesToLong(new byte[] { 1, 1, 1, X7F, FF, FF, FF, FF, FF, FF, FF}));
    Assert.assertEquals(
        Long.MIN_VALUE,
        Util.bytesToLong(new byte[] { 0, 0, 0, X80, 0, 0, 0, 0, 0, 0, 0 }));
  }

}

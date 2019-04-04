/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.api.common;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Unit tests for {@link Bytes} class.
 */
public class BytesTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAdd() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] b = {5, 6, 7, 8};
    final byte[] empty = {};

    Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, Bytes.add(a, b));
    Assert.assertArrayEquals(a, Bytes.add(a, empty));
    Assert.assertArrayEquals(empty, Bytes.add(empty, empty));
  }

  @Test
  public void testCompareTo() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] b = {5, 6, 7, 8};
    final byte[] empty = {};

    Assert.assertEquals(0, Bytes.compareTo(empty, empty));
    Assert.assertTrue(Bytes.compareTo(a, empty) > 0);
    Assert.assertTrue(Bytes.compareTo(empty, b) < 0);

    Assert.assertTrue(Bytes.compareTo(a, b) < 0);
    Assert.assertTrue(Bytes.compareTo(b, a) > 0);

    Assert.assertTrue(Bytes.compareTo(b, 1, 2, a, 2, 2) > 0);
    Assert.assertTrue(Bytes.compareTo(b, 0, 3, a, 2, 1) > 0);
  }

  @Test
  public void testConcat() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] b = {5, 6, 7, 8};
    final byte[] empty = {};

    Assert.assertArrayEquals(empty, Bytes.concat(empty));
    Assert.assertArrayEquals(a, Bytes.concat(a));

    Assert.assertArrayEquals(a, Bytes.concat(a, empty));
    Assert.assertArrayEquals(b, Bytes.concat(empty, b));

    Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, Bytes.concat(a, b));
  }

  @Test
  public void testEquals() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] b = {5, 6, 7, 8};
    final byte[] c = {7, 8, 1, 2};
    final byte[] empty = {};

    Assert.assertTrue(Bytes.equals(empty, empty));
    Assert.assertTrue(Bytes.equals(a, a));
    Assert.assertTrue(Bytes.equals(null, null));

    Assert.assertFalse(Bytes.equals(a, empty));
    Assert.assertFalse(Bytes.equals(null, empty));
    Assert.assertFalse(Bytes.equals(a, b));
    Assert.assertFalse(Bytes.equals(a, null));

    Assert.assertTrue(Bytes.equals(a, 0, 2, a, 0, 2));
    Assert.assertTrue(Bytes.equals(a, 0, 2, c, 2, 2));
    Assert.assertTrue(Bytes.equals(a, 1, 0, empty, 0, 0));

    Assert.assertFalse(Bytes.equals(a, 2, 2, c, 2, 2));
    Assert.assertFalse(Bytes.equals(a, 0, 0, b, 2, 2));
    Assert.assertFalse(Bytes.equals(a, 0, 3, b, 2, 2));
  }

  @Test
  public void testFromHexString() {
    Assert.assertArrayEquals(new byte[]{}, Bytes.fromHexString(""));
    Assert.assertArrayEquals(new byte[]{0x16}, Bytes.fromHexString("16"));
    Assert.assertArrayEquals(new byte[]{0x16, 0x0A}, Bytes.fromHexString("160A"));

    thrown.expect(IllegalArgumentException.class);
    Bytes.fromHexString("g");
  }

  @Test
  public void testHashBytes() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] b = {5, 6, 7, 8};
    final byte[] empty = {};

    Assert.assertEquals(1, Bytes.hashBytes(empty, 0));
    Assert.assertEquals(955331, Bytes.hashBytes(a, 4));
    Assert.assertEquals(1078467, Bytes.hashBytes(b, 4));

    Assert.assertEquals(1058, Bytes.hashBytes(a, 2, 2));
    Assert.assertEquals(35782, Bytes.hashBytes(b, 1, 3));
  }

  @Test
  public void testHashCode() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] b = {5, 6, 7, 8};
    final byte[] empty = {};

    Assert.assertEquals(1, Bytes.hashCode(empty, 0));
    Assert.assertEquals(955331, Bytes.hashCode(a, 4));
    Assert.assertEquals(1078467, Bytes.hashCode(b, 4));

    Assert.assertEquals(1058, Bytes.hashCode(a, 2, 2));
    Assert.assertEquals(35782, Bytes.hashCode(b, 1, 3));
  }

  @Test
  public void testMapKey() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] b = {5, 6, 7, 8};
    final byte[] empty = {};

    Assert.assertEquals(new Integer(1), Bytes.mapKey(empty));
    Assert.assertEquals(new Integer(955331), Bytes.mapKey(a));
    Assert.assertEquals(new Integer(1078467), Bytes.mapKey(b));

    Assert.assertEquals(new Integer(1), Bytes.mapKey(empty, 0));
    Assert.assertEquals(new Integer(955331), Bytes.mapKey(a, 4));
    Assert.assertEquals(new Integer(1122), Bytes.mapKey(b, 2));
  }

  @Test
  public void testHead() {
    final byte[] a = {1};
    final byte[] b = {5, 6, 7, 8};
    final byte[] empty = {};

    Assert.assertNull(Bytes.head(a, 2));

    Assert.assertArrayEquals(new byte[]{}, Bytes.head(empty, 0));
    Assert.assertArrayEquals(new byte[]{1}, Bytes.head(a, 1));
    Assert.assertArrayEquals(new byte[]{5, 6, 7, 8}, Bytes.head(b, 4));
  }

  @Test
  public void testIncrementBytes() {
    final byte[] a = {1, 2, 3, 4, 5, 6, 7, 8};
    Assert.assertArrayEquals(
        new byte[]{1, 2, 3, 4, 5, 6, 7, 18},
        Bytes.incrementBytes(a, 10));

    final byte[] b = {-10, 2, 3, 4, 5, 6, 7, 8};
    Assert.assertArrayEquals(
        new byte[]{-10, 2, 3, 4, 5, 6, 7, 18},
        Bytes.incrementBytes(b, 10));

    final byte[] c = {-10, 2, 3, 4};
    Assert.assertArrayEquals(
        new byte[]{-1, -1, -1, -1, -10, 2, 3, 14},
        Bytes.incrementBytes(c, 10));

    final byte[] d = {10, 2, 3, 4};
    Assert.assertArrayEquals(
        new byte[]{0, 0, 0, 0, 10, 2, 3, 14},
        Bytes.incrementBytes(d, 10));

    final byte[] e = {10, 2, 3, 4};
    Assert.assertArrayEquals(
        new byte[]{0, 0, 0, 0, 10, 2, 3, 4},
        Bytes.incrementBytes(e, 0));
  }

  @Test
  public void testPadHead() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] empty = {};

    Assert.assertArrayEquals(new byte[]{}, Bytes.padHead(empty, 0));
    Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, Bytes.padHead(a, 0));

    Assert.assertArrayEquals(new byte[]{0, 0, 0}, Bytes.padHead(empty, 3));
    Assert.assertArrayEquals(new byte[]{0, 1, 2, 3, 4}, Bytes.padHead(a, 1));

    thrown.expect(NegativeArraySizeException.class);
    Bytes.padHead(a, -10);
  }

  @Test
  public void testPadTail() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] empty = {};

    Assert.assertArrayEquals(new byte[]{}, Bytes.padTail(empty, 0));
    Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, Bytes.padTail(a, 0));

    Assert.assertArrayEquals(new byte[]{0, 0, 0}, Bytes.padTail(empty, 3));
    Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 0, 0, 0}, Bytes.padTail(a, 3));

    thrown.expect(NegativeArraySizeException.class);
    Bytes.padHead(a, -10);
  }

  @Test
  public void testPutByte() {
    byte[] bytes = new byte[4];
    Assert.assertEquals(3, Bytes.putByte(bytes, 2, (byte) 12));
    Assert.assertArrayEquals(new byte[]{0, 0, 12, 0}, bytes);
  }

  @Test
  public void testPutBytes() {
    final byte[] a = {1, 2, 3, 4};

    byte[] bytes = new byte[4];

    Assert.assertEquals(4, Bytes.putBytes(bytes, 0, a, 0, 4));
    Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, bytes);

    bytes = new byte[4];
    Assert.assertEquals(3, Bytes.putBytes(bytes, 1, a, 2, 2));
    Assert.assertArrayEquals(new byte[]{0, 3, 4, 0}, bytes);
  }

  @Test
  public void testPutDouble() {
    byte[] bytes = new byte[8];

    Assert.assertEquals(8, Bytes.putDouble(bytes, 0, 0d));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}, bytes);

    bytes = new byte[8];
    Assert.assertEquals(8, Bytes.putDouble(bytes, 0, 0.55d));
    Assert.assertArrayEquals(new byte[]{63, -31, -103, -103, -103, -103, -103, -102}, bytes);

    bytes = new byte[10];
    Assert.assertEquals(9, Bytes.putDouble(bytes, 1, 0.55d));
    Assert.assertArrayEquals(new byte[]{0, 63, -31, -103, -103, -103, -103, -103, -102, 0}, bytes);
  }

  @Test
  public void testPutFloat() {
    byte[] bytes = new byte[4];

    Assert.assertEquals(4, Bytes.putFloat(bytes, 0, 0f));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0}, bytes);

    bytes = new byte[4];
    Assert.assertEquals(4, Bytes.putFloat(bytes, 0, 0.55f));
    Assert.assertArrayEquals(new byte[]{63, 12, -52, -51}, bytes);

    bytes = new byte[6];
    Assert.assertEquals(5, Bytes.putFloat(bytes, 1, 0.55f));
    Assert.assertArrayEquals(new byte[]{0, 63, 12, -52, -51, 0}, bytes);
  }

  @Test
  public void testPutInt() {
    byte[] bytes = new byte[4];

    Assert.assertEquals(4, Bytes.putInt(bytes, 0, 0));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0}, bytes);

    bytes = new byte[4];
    Assert.assertEquals(4, Bytes.putInt(bytes, 0, 231));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, -25}, bytes);

    bytes = new byte[6];
    Assert.assertEquals(5, Bytes.putInt(bytes, 1, 123));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 123, 0}, bytes);
  }

  @Test
  public void testPutLong() {
    byte[] bytes = new byte[8];

    Assert.assertEquals(8, Bytes.putLong(bytes, 0, 0));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}, bytes);

    bytes = new byte[8];
    Assert.assertEquals(8, Bytes.putLong(bytes, 0, 555));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 2, 43}, bytes);

    bytes = new byte[10];
    Assert.assertEquals(9, Bytes.putLong(bytes, 1, 123));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 123, 0}, bytes);
  }

  @Test
  public void testPutShort() {
    byte[] bytes = {7, 7};

    Assert.assertEquals(2, Bytes.putShort(bytes, 0, (short) 0));
    Assert.assertArrayEquals(new byte[]{0, 0}, bytes);

    bytes = new byte[]{7, 7};
    Assert.assertEquals(2, Bytes.putShort(bytes, 0, (short) 555));
    Assert.assertArrayEquals(new byte[]{2, 43}, bytes);

    bytes = new byte[]{7, 7, 7, 7};
    Assert.assertEquals(3, Bytes.putShort(bytes, 1, (short) 123));
    Assert.assertArrayEquals(new byte[]{7, 0, 123, 7}, bytes);
  }

  @Test
  public void testSplit() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] b = {5, 6, 7, 8};

    Assert.assertArrayEquals(new byte[][]{
            {1, 2, 3, 4},
            {3, 4, 5, 6},
            {5, 6, 7, 8}
        },
        Bytes.split(a, b, 1));
  }

  @Test
  public void testStartsWith() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] b = {5, 6, 7, 8};
    final byte[] empty = {};

    Assert.assertTrue(Bytes.startsWith(a, empty));
    Assert.assertTrue(Bytes.startsWith(a, new byte[]{1}));
    Assert.assertTrue(Bytes.startsWith(b, new byte[]{5, 6}));

    Assert.assertFalse(Bytes.startsWith(b, new byte[]{4, 5, 6, 7, 8}));
    Assert.assertFalse(Bytes.startsWith(a, b));
    Assert.assertFalse(Bytes.startsWith(empty, new byte[]{1, 2}));
  }

  @Test
  public void testStopKeyForPrefix() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] b = {5, 6, 7, 8};
    final byte[] empty = {};

    Assert.assertNull(Bytes.stopKeyForPrefix(empty));

    Assert.assertArrayEquals(new byte[]{1, 2, 3, 5}, Bytes.stopKeyForPrefix(a));
    Assert.assertArrayEquals(new byte[]{5, 6, 7, 9}, Bytes.stopKeyForPrefix(b));
  }

  @Test
  public void testTail() {
    final byte[] a = {1, 2, 3, 4};
    final byte[] b = {5, 6, 7, 8};
    final byte[] empty = {};

    Assert.assertNull(Bytes.tail(empty, 2));

    Assert.assertArrayEquals(empty, Bytes.tail(a, 0));
    Assert.assertArrayEquals(new byte[]{3, 4}, Bytes.tail(a, 2));
    Assert.assertArrayEquals(new byte[]{6, 7, 8}, Bytes.tail(b, 3));
  }

  @Test
  public void testToBinaryFromHex() {
    Assert.assertEquals(0xA, Bytes.toBinaryFromHex((byte) 'A'));
    Assert.assertEquals(0x2, Bytes.toBinaryFromHex((byte) '2'));
  }

  @Test
  public void testToBoolean() {
    Assert.assertFalse(Bytes.toBoolean(new byte[]{0}));

    Assert.assertTrue(Bytes.toBoolean(new byte[]{1}));
    Assert.assertTrue(Bytes.toBoolean(new byte[]{-1}));
    Assert.assertTrue(Bytes.toBoolean(new byte[]{20}));

    thrown.expect(IllegalArgumentException.class);
    Bytes.toBoolean(new byte[]{1, 2});
  }

  @Test
  public void testToByteArrays() {
    Assert.assertArrayEquals(
        new byte[][]{{1, 2, 3, 4}},
        Bytes.toByteArrays(new byte[]{1, 2, 3, 4}));

    Assert.assertArrayEquals(
        new byte[][]{{116, 101, 115, 116}},
        Bytes.toByteArrays("test"));

    Assert.assertArrayEquals(
        new byte[][]{
            {97, 98, 99, 100},
            {101, 102, 103, 104}
        },
        Bytes.toByteArrays(new String[]{"abcd", "efgh"}));
  }

  @Test
  public void testToBytesBinary() {
    Assert.assertArrayEquals(new byte[]{}, Bytes.toBytesBinary(""));
    Assert.assertArrayEquals(new byte[]{97, 98, 99, 100}, Bytes.toBytesBinary("abcd"));
    Assert.assertArrayEquals(new byte[]{0x0A, 0x2B}, Bytes.toBytesBinary("\\x0A\\x2B"));
  }

  @Test
  public void testBooleanToBytes() {
    Assert.assertArrayEquals(new byte[]{0}, Bytes.toBytes(false));
    Assert.assertArrayEquals(new byte[]{-1}, Bytes.toBytes(true));
  }

  @Test
  public void testStringToBytes() {
    Assert.assertArrayEquals(new byte[]{}, Bytes.toBytes(""));
    Assert.assertArrayEquals(new byte[]{}, Bytes.toBytes(""));
  }

  @Test
  public void testShortToBytes() {
    Assert.assertArrayEquals(new byte[]{0, 0}, Bytes.toBytes((short) 0));
    Assert.assertArrayEquals(new byte[]{1, 0}, Bytes.toBytes((short) 256));
    Assert.assertArrayEquals(new byte[]{-1, 0}, Bytes.toBytes((short) -256));
    Assert.assertArrayEquals(new byte[]{48, 57}, Bytes.toBytes((short) 12345));
    Assert.assertArrayEquals(new byte[]{-49, -57}, Bytes.toBytes((short) -12345));
  }

  @Test
  public void testToShort() {
    Assert.assertEquals((short) 0, Bytes.toShort(new byte[]{0, 0}));
    Assert.assertEquals((short) 256, Bytes.toShort(new byte[]{1, 0}));
    Assert.assertEquals((short) -256, Bytes.toShort(new byte[]{-1, 0}));
    Assert.assertEquals((short) 12345, Bytes.toShort(new byte[]{48, 57}));
    Assert.assertEquals((short) -12345, Bytes.toShort(new byte[]{-49, -57}));
  }

  @Test
  public void testIntToBytes() {
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0}, Bytes.toBytes(0));
    Assert.assertArrayEquals(new byte[]{0, 0, 1, 0}, Bytes.toBytes(256));
    Assert.assertArrayEquals(new byte[]{-1, -1, -1, 0}, Bytes.toBytes(-256));
    Assert.assertArrayEquals(new byte[]{0, 0, 48, 57}, Bytes.toBytes(12345));
    Assert.assertArrayEquals(new byte[]{-1, -1, -49, -57}, Bytes.toBytes(-12345));
  }

  @Test
  public void testToInt() {
    Assert.assertEquals(0, Bytes.toInt(new byte[]{0, 0, 0, 0}));
    Assert.assertEquals(256, Bytes.toInt(new byte[]{0, 0, 1, 0}));
    Assert.assertEquals(-256, Bytes.toInt(new byte[]{-1, -1, -1, 0}));
    Assert.assertEquals(12345, Bytes.toInt(new byte[]{0, 0, 48, 57}));
    Assert.assertEquals(-12345, Bytes.toInt(new byte[]{-1, -1, -49, -57}));
  }


  @Test
  public void testFloatToBytes() {
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0}, Bytes.toBytes(0F));
    Assert.assertArrayEquals(new byte[]{67, -128, 0, 0}, Bytes.toBytes(256F));
    Assert.assertArrayEquals(new byte[]{-61, -128, 0, 0}, Bytes.toBytes(-256F));
    Assert.assertArrayEquals(new byte[]{66, -10, -26, 102}, Bytes.toBytes(123.45F));
    Assert.assertArrayEquals(new byte[]{-62, -10, -26, 102}, Bytes.toBytes(-123.45F));
  }

  @Test
  public void testToFloat() {
    Assert.assertEquals(0F, Bytes.toFloat(new byte[]{0, 0, 0, 0}), 0);
    Assert.assertEquals(256F, Bytes.toFloat(new byte[]{67, -128, 0, 0}), 0);
    Assert.assertEquals(-256F, Bytes.toFloat(new byte[]{-61, -128, 0, 0}), 0);
    Assert.assertEquals(123.45F, Bytes.toFloat(new byte[]{66, -10, -26, 102}), 0);
    Assert.assertEquals(-123.45F, Bytes.toFloat(new byte[]{-62, -10, -26, 102}), 0);
  }

  @Test
  public void testDoubleToBytes() {
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}, Bytes.toBytes(0D));
    Assert.assertArrayEquals(new byte[]{64, 112, 0, 0, 0, 0, 0, 0}, Bytes.toBytes(256D));
    Assert.assertArrayEquals(new byte[]{-64, 112, 0, 0, 0, 0, 0, 0}, Bytes.toBytes(-256D));
    Assert.assertArrayEquals(new byte[]{64, 94, -36, -52, -52, -52, -52, -51}, Bytes.toBytes(123.45D));
    Assert.assertArrayEquals(new byte[]{-64, 94, -36, -52, -52, -52, -52, -51}, Bytes.toBytes(-123.45D));
  }

  @Test
  public void testToDouble() {
    Assert.assertEquals(0D,
        Bytes.toDouble(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}), 0);
    Assert.assertEquals(256D,
        Bytes.toDouble(new byte[]{64, 112, 0, 0, 0, 0, 0, 0}), 0);
    Assert.assertEquals(-256D,
        Bytes.toDouble(new byte[]{-64, 112, 0, 0, 0, 0, 0, 0}), 0);
    Assert.assertEquals(123.45D,
        Bytes.toDouble(new byte[]{64, 94, -36, -52, -52, -52, -52, -51}), 0);
    Assert.assertEquals(-123.45D,
        Bytes.toDouble(new byte[]{-64, 94, -36, -52, -52, -52, -52, -51}), 0);
  }

  @Test
  public void testLongToBytes() {
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}, Bytes.toBytes(0L));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 1, 0}, Bytes.toBytes(256L));
    Assert.assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, 0}, Bytes.toBytes(-256L));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 48, 57}, Bytes.toBytes(12345L));
    Assert.assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -49, -57}, Bytes.toBytes(-12345L));
  }

  @Test
  public void testToLong() {
    Assert.assertEquals(0L,
        Bytes.toLong(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}));
    Assert.assertEquals(256L,
        Bytes.toLong(new byte[]{0, 0, 0, 0, 0, 0, 1, 0}));
    Assert.assertEquals(-256L,
        Bytes.toLong(new byte[]{-1, -1, -1, -1, -1, -1, -1, 0}));
    Assert.assertEquals(12345L,
        Bytes.toLong(new byte[]{0, 0, 0, 0, 0, 0, 48, 57}));
    Assert.assertEquals(-12345L,
        Bytes.toLong(new byte[]{-1, -1, -1, -1, -1, -1, -49, -57}));
  }

  @Test
  public void testUUIDToBytes() {
    Assert.assertArrayEquals(new byte[]{
            0, 0, 0, 0,
            0, 0, 0, 123,
            0, 0, 0, 0,
            0, 0, 1, -56
        },
        Bytes.toBytes(new UUID(123L, 456L)));
    Assert.assertArrayEquals(new byte[]{
            0, 0, 0, 0,
            0, 0, 3, 21,
            0, 0, 0, 0,
            0, 0, 0, 123
        },
        Bytes.toBytes(new UUID(789L, 123L)));
  }

  @Test
  public void testBigDecimalToBytes() {
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0}, Bytes.toBytes(new BigDecimal(0)));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 1}, Bytes.toBytes(new BigDecimal(1)));
    Assert.assertArrayEquals(new byte[]{0, 0, 0, 5, 73, -107, 41, -39}, Bytes.toBytes(new BigDecimal("12345.12345")));
  }


  @Test
  public void testToStringBinary() {
    Assert.assertEquals("null", Bytes.toStringBinary((byte[]) null));
    Assert.assertEquals("", Bytes.toStringBinary(new byte[]{}));
    Assert.assertEquals("\\x01\\x02\\x03\\x04", Bytes.toStringBinary(new byte[]{1, 2, 3, 4}));
    Assert.assertEquals("AbCd", Bytes.toStringBinary(new byte[]{'A', 'b', 'C', 'd'}));
  }

  @Test
  public void testByteBufferToStringBinary() {
    ByteBuffer bb = ByteBuffer.wrap(new byte[]{'A', 'b', 'C', 'd'});
    Assert.assertEquals("AbCd", Bytes.toStringBinary(bb));
  }

  @Test
  public void testBytesToString() {
    final byte[] input1 = new byte[]{'A', 'b', 'C', 'd'};
    final byte[] input2 = new byte[]{'D', 'e', 'F', 'g'};

    Assert.assertEquals("", Bytes.toString(input1, 0, 0));

    Assert.assertEquals("AbCd", Bytes.toString(input1));
    Assert.assertEquals("Ab", Bytes.toString(input1, 0, 2));
    Assert.assertEquals("Cd", Bytes.toString(input1, 2, 2));

    Assert.assertEquals("AbCdDeFg", Bytes.toString(input1, "", input2));
    Assert.assertEquals("AbCd<>DeFg", Bytes.toString(input1, "<>", input2));
  }

  @Test
  public void testByteBufferToString() {
    Assert.assertNull(Bytes.toString((ByteBuffer) null));

    ByteBuffer bb = ByteBuffer.wrap(new byte[]{'A', 'b', 'C', 'd'});
    Assert.assertEquals("AbCd", Bytes.toString(bb));
    Assert.assertEquals("AbCd", Bytes.toString(bb));
  }

  @Test
  public void testHexString() {
    byte[] bytes = new byte[]{1, 2, 0, 127, -128, 63, -1};

    String hexString = Bytes.toHexString(bytes);
    Assert.assertEquals("0102007f803fff", hexString);

    Assert.assertArrayEquals(bytes, Bytes.fromHexString(hexString));
    Assert.assertArrayEquals(bytes, Bytes.fromHexString(hexString.toUpperCase()));
  }

  @Test
  public void testUnsafeComparer() {
    testComparer(Bytes.LexicographicalComparerHolder.UnsafeComparer.INSTANCE);
  }

  @Test
  public void testPureJavaComparer() {
    testComparer(Bytes.LexicographicalComparerHolder.PureJavaComparer.INSTANCE);
  }

  private void testComparer(Bytes.Comparer<byte[]> comparer) {
    byte[] left = "aaabbbcccdddeeefffggghhh".getBytes(StandardCharsets.US_ASCII);
    byte[] right = "aaabbbcccdddeeefffggghhh".getBytes(StandardCharsets.US_ASCII);

    // Simple comparison
    int cmp = comparer.compareTo(left, 0, left.length, right, 0, right.length);
    Assert.assertEquals(0, cmp);

    // Compare with offset
    cmp = comparer.compareTo(left, 2, left.length - 2, right, 0, right.length);
    Assert.assertTrue(cmp > 0);

    // Different in length
    cmp = comparer.compareTo(left, 0, left.length - 1, right, 0, right.length);
    Assert.assertTrue(cmp < 0);

    // Empty array
    cmp = comparer.compareTo(Bytes.EMPTY_BYTE_ARRAY, 0, 0, Bytes.EMPTY_BYTE_ARRAY, 0, 0);
    Assert.assertEquals(0, cmp);

    // Empty array is always smallest
    cmp = comparer.compareTo(left, 0, left.length, Bytes.EMPTY_BYTE_ARRAY, 0, 0);
    Assert.assertTrue(cmp > 0);
  }
}

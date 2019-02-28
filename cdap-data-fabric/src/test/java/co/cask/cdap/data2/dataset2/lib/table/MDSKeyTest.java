
/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class MDSKeyTest {

  @Test
  public void simpleStringKeySplit() {
    // Tests key: [ "part1", "part2", "part3" ]
    List<String> originalKeyParts = ImmutableList.of("part1", "part2", "part3");
    MDSKey.Builder builder = new MDSKey.Builder();
    for (String part : originalKeyParts) {
      builder.add(part);
    }
    MDSKey mdsKey = builder.build();

    MDSKey.Splitter splitter = mdsKey.split();
    for (String originalKeyPart : originalKeyParts) {
      Assert.assertEquals(originalKeyPart, splitter.getString());
    }
  }

  @Test
  public void testComplexKeySplit() {
    // Tests key: [ "part1", "part2", "", 4l, byte[] { 0x5 } ]
    List<String> firstParts = ImmutableList.of("part1", "part2", "");
    long fourthPart = 4L;
    byte[] fifthPart = new byte[] { 0x5 };

    MDSKey.Builder builder = new MDSKey.Builder();
    // intentionally testing the MDSKey.Builder#add(String... parts) method.
    builder.add(firstParts.get(0), firstParts.get(1), firstParts.get(2));

    builder.add(fourthPart);
    builder.add(fifthPart);
    MDSKey mdsKey = builder.build();

    MDSKey.Splitter splitter = mdsKey.split();
    for (String part : firstParts) {
      Assert.assertEquals(part, splitter.getString());
    }
    Assert.assertEquals(fourthPart, splitter.getLong());
    Assert.assertTrue(Bytes.equals(fifthPart, splitter.getBytes()));
  }

  @Test
  public void testSkipStringAndBytes() {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add("part1");
    builder.add("part2");
    builder.add("part3");

    byte[] bytesToSkip = new byte[] { 0x1 };
    byte[] bytesToCheck = new byte[] { 0x2 };
    builder.add(bytesToSkip);
    builder.add(bytesToCheck);

    MDSKey mdsKey = builder.build();
    MDSKey.Splitter splitter = mdsKey.split();

    Assert.assertEquals("part1", splitter.getString());
    splitter.skipString();
    Assert.assertEquals("part3", splitter.getString());

    splitter.skipBytes();
    Assert.assertTrue(splitter.hasRemaining());
    Assert.assertTrue(Bytes.equals(bytesToCheck, splitter.getBytes()));
    Assert.assertFalse(splitter.hasRemaining());
  }

  @Test
  public void testSkipLongAndInt() {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add("part1");
    builder.add(2L);
    builder.add(3L);
    builder.add(4);
    builder.add(5);

    MDSKey mdsKey = builder.build();
    MDSKey.Splitter splitter = mdsKey.split();

    Assert.assertEquals("part1", splitter.getString());
    splitter.skipLong();
    Assert.assertEquals(3L, splitter.getLong());

    splitter.skipInt();
    Assert.assertEquals(5, splitter.getInt());
  }

  @Test
  public void testGetBytesOverflow() {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(2000);
    builder.add(2000);

    MDSKey mdsKey = builder.build();
    MDSKey.Splitter splitter = mdsKey.split();

    // splitter.getBytes and splitter.getString() will fail due to the key being composed of two large int parts
    try {
      splitter.getBytes();
      Assert.fail();
    } catch (BufferUnderflowException expected) {
    }

    try {
      splitter.getString();
      Assert.fail();
    } catch (BufferUnderflowException expected) {
    }
  }

  @Test
  public void getGetIntOverflow() {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(1);
    builder.add(2);
    builder.add(3);

    MDSKey mdsKey = builder.build();
    MDSKey.Splitter splitter = mdsKey.split();

    Assert.assertEquals(1, splitter.getInt());
    Assert.assertEquals(2, splitter.getInt());
    Assert.assertEquals(3, splitter.getInt());

    // splitter.getInt will fail due to there only being 3 parts in the key
    try {
      splitter.getInt();
      Assert.fail();
    } catch (BufferUnderflowException expected) {
      Assert.assertFalse(splitter.hasRemaining());
    }
  }

  @Test
  public void testAppend() {
    MDSKey mdsKey1 = new MDSKey.Builder().add("ab").add(3L).add(new byte[]{'x', 'y'}).build();
    MDSKey mdsKey2 = new MDSKey.Builder().add("bd").add(5).append(mdsKey1).add(new byte[]{'z', 'z'}).build();
    MDSKey mdsKey3 = new MDSKey.Builder().add(2).add(new byte[]{'w'}).append(mdsKey2).add(8L).build();

    // Assert
    MDSKey.Splitter splitter = mdsKey3.split();
    Assert.assertEquals(2, splitter.getInt());
    Assert.assertArrayEquals(new byte[]{'w'}, splitter.getBytes());
    Assert.assertEquals("bd", splitter.getString());
    Assert.assertEquals(5, splitter.getInt());
    Assert.assertEquals("ab", splitter.getString());
    Assert.assertEquals(3L, splitter.getLong());
    Assert.assertArrayEquals(new byte[]{'x', 'y'}, splitter.getBytes());
    Assert.assertArrayEquals(new byte[]{'z', 'z'}, splitter.getBytes());
    Assert.assertEquals(8L, splitter.getLong());
  }

  @SuppressWarnings("SimplifiableJUnitAssertion")
  @Test
  public void testSort() {
    List<MDSKey> expectedSorted = new ArrayList<>();
    List<MDSKey> expectedReverse = new ArrayList<>();
    // Add keys in sorted order
    int max = 3;
    List<String> sortedStrings = Arrays.asList("first-", "long-string-", "str-");
    for (int i = 0; i < max; ++i) {
      for (int s = 0; s < max; ++s) {
        for (long l = 0; l < max; ++l) {
          expectedSorted.add(new MDSKey.Builder().add(i).add(sortedStrings.get(s) + i).add(l).build());
          int ri = max - i - 1;
          int rs = max - s - 1;
          long rl = max - l - 1;
          expectedReverse.add(new MDSKey.Builder().add(ri).add(sortedStrings.get(rs) + ri).add(rl).build());
        }
      }
    }

    // Now shuffle, and sort the list
    List<MDSKey> actualSorted = new ArrayList<>(expectedSorted);
    Random random = new Random(System.nanoTime());
    Collections.shuffle(actualSorted, random);
    Collections.sort(actualSorted);
    Assert.assertTrue(createSortErrorMessage(expectedSorted, actualSorted), expectedSorted.equals(actualSorted));

    // Test reverse sorting
    ArrayList<MDSKey> actualReverse = new ArrayList<>(expectedReverse);
    Collections.shuffle(actualReverse, random);
    Collections.sort(actualReverse, Collections.reverseOrder());
    Collections.reverse(actualReverse);
    Assert.assertTrue(createSortErrorMessage(expectedReverse, actualReverse), expectedReverse.equals(actualReverse));
  }

  private String createSortErrorMessage(List<MDSKey> expected, List<MDSKey> actual) {
    StringBuilder stringBuilder = new StringBuilder("\n\nExpected :");
    writeSortKeyList(stringBuilder, expected);
    stringBuilder.append("\nActual   :");
    writeSortKeyList(stringBuilder, actual);
    stringBuilder.append("\n");
    return stringBuilder.toString();
  }
  private void writeSortKeyList(StringBuilder stringBuilder, List<MDSKey> keys) {
    for (MDSKey key : keys) {
      MDSKey.Splitter splitter = key.split();
      stringBuilder.append("MDSKey{")
        .append("int=").append(splitter.getInt()).append(", ")
        .append("str=").append(splitter.getString()).append(", ")
        .append("long=").append(splitter.getLong())
        .append("}, ");
    }
  }
}

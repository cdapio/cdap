
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

import java.util.List;

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
    Assert.assertTrue(Bytes.equals(bytesToCheck, splitter.getBytes()));
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
    } catch (IllegalStateException expected) {
    }

    try {
      splitter.getString();
      Assert.fail();
    } catch (IllegalStateException expected) {
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
    } catch (Exception expected) {
    }
  }
}

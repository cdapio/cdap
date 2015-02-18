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
import com.google.common.collect.Lists;
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

    List<String> splitKeyParts = Lists.newArrayList();
    List<byte[]> splittedBytes = mdsKey.split();
    for (byte[] bytes : splittedBytes) {
      splitKeyParts.add(Bytes.toString(bytes));
    }
    Assert.assertEquals(originalKeyParts, splitKeyParts);
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

    List<byte[]> splittedBytes = mdsKey.split();
    List<String> splitKeyParts = Lists.newArrayList();

    int i = 0;
    for (; i < firstParts.size(); i++) {
      splitKeyParts.add(Bytes.toString(splittedBytes.get(i)));
    }
    Assert.assertEquals(firstParts, splitKeyParts);

    Assert.assertEquals(fourthPart, Bytes.toLong(splittedBytes.get(3)));
    Assert.assertTrue(Bytes.equals(fifthPart, splittedBytes.get(4)));
  }
}

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
  public void testKeyBuildSplit() {
    List<String> originalKeyParts = ImmutableList.of("part1", "part2", "part3");
    MDSKey.Builder builder = new MDSKey.Builder();
    for (String part : originalKeyParts) {
      builder.add(part);
    }
    MDSKey MDSKey = builder.build();

    List<String> splitKeyParts = Lists.newArrayList();
    List<byte[]> splittedBytes = MDSKey.split();
    for (byte[] bytes : splittedBytes) {
      splitKeyParts.add(Bytes.toString(bytes));
    }
    Assert.assertEquals(originalKeyParts, splitKeyParts);
  }
}

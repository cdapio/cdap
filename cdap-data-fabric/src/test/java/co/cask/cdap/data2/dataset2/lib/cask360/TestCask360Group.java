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
package co.cask.cdap.data2.dataset2.lib.cask360;

import co.cask.cdap.api.dataset.lib.cask360.Cask360Group.Cask360GroupMeta;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Group.Cask360GroupType;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link TestCask360Group}.
 */
public class TestCask360Group {

  @Test
  public void testSerialization() throws Exception {

    byte [] oneName = Bytes.toBytes("one");
    short oneNumber = 1;
    Cask360GroupType oneType = Cask360GroupType.MAP;
    Cask360GroupMeta one = new Cask360GroupMeta(oneName, oneNumber, oneType);

    byte [] twoName = Bytes.toBytes("two");
    short twoNumber = 2;
    Cask360GroupType twoType = Cask360GroupType.TIME;
    Cask360GroupMeta two = new Cask360GroupMeta(twoName, twoNumber, twoType);

    byte [] oneBytes = one.toBytes();
    Cask360GroupMeta oneAgain = Cask360GroupMeta.fromBytes(oneBytes);
    Assert.assertTrue("Serialized and Re-Deserialized does not match",
        oneAgain.equals(one));

    byte [] twoBytes = two.toBytes();
    Cask360GroupMeta twoAgain = Cask360GroupMeta.fromBytes(twoBytes);
    Assert.assertTrue("Serialized and Re-Deserialized does not match",
        twoAgain.equals(two));
  }

}

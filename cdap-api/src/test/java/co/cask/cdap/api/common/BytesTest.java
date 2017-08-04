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

package co.cask.cdap.api.common;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Bytes} class.
 */
public class BytesTest {
  @Test
  public void testHexString() {
    byte[] bytes = new byte[] {1, 2, 0, 127, -128, 63, -1};

    String hexString = Bytes.toHexString(bytes);
    Assert.assertEquals("0102007f803fff", hexString);

    Assert.assertArrayEquals(bytes, Bytes.fromHexString(hexString));
    Assert.assertArrayEquals(bytes, Bytes.fromHexString(hexString.toUpperCase()));
  }

}

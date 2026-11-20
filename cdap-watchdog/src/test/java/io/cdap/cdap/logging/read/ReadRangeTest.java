/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.read;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ReadRange}.
 */
public class ReadRangeTest {

  @Test
  public void testToStringAndFactoryMethods() {
    ReadRange range = new ReadRange(100L, 200L, 55L);
    Assert.assertEquals(100L, range.getFromMillis());
    Assert.assertEquals(200L, range.getToMillis());
    Assert.assertEquals(55L, range.getKafkaOffset());
    Assert.assertEquals("ReadRange{fromMillis=100, toMillis=200, kafkaOffset=55}",
        range.toString());

    Assert.assertSame(ReadRange.LATEST, ReadRange.createFromRange(LogOffset.LATEST_OFFSET));
    Assert.assertSame(ReadRange.LATEST, ReadRange.createToRange(LogOffset.LATEST_OFFSET));

    LogOffset offset = new LogOffset(7L, 5000L);
    Assert.assertEquals(
        "ReadRange{fromMillis=5000, toMillis=" + Long.MAX_VALUE + ", kafkaOffset=7}",
        ReadRange.createFromRange(offset).toString());
    Assert.assertEquals("ReadRange{fromMillis=-1, toMillis=5000, kafkaOffset=7}",
        ReadRange.createToRange(offset).toString());
  }
}

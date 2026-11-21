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
 * Unit tests for {@link LogOffset}.
 */
public class LogOffsetTest {

  @Test
  public void testToStringAndGetters() {
    LogOffset offset = new LogOffset(123L, 456789L);
    Assert.assertEquals(123L, offset.getKafkaOffset());
    Assert.assertEquals(456789L, offset.getTime());
    Assert.assertEquals("LogOffset{kafkaOffset=123, time=456789}", offset.toString());
    Assert.assertEquals("LogOffset{kafkaOffset=-1, time=-1}", LogOffset.LATEST_OFFSET.toString());
  }
}

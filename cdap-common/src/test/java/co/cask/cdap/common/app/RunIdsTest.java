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

package co.cask.cdap.common.app;

import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RunIdsTest {
  @Test
  public void testTimeBasedRunId() throws Exception {
    long time = System.currentTimeMillis();

    // Generate UUID based on time, and extract time from it.
    UUID uuid1 = RunIds.generateUUIDForTime(time);
    Assert.assertEquals(time, RunIds.getTime(RunIds.fromString(uuid1.toString()), TimeUnit.MILLISECONDS));

    // Generate another UUID for the same time, the new UUID should be different from the previous one.
    UUID uuid2 = RunIds.generateUUIDForTime(time);
    Assert.assertEquals(time, RunIds.getTime(RunIds.fromString(uuid2.toString()), TimeUnit.MILLISECONDS));
    Assert.assertNotEquals(uuid1.toString(), uuid2.toString());

    // Generate UUID for a different time
    long newTime = time + 1;
    UUID uuid3 = RunIds.generateUUIDForTime(newTime);
    Assert.assertEquals(newTime, RunIds.getTime(RunIds.fromString(uuid3.toString()), TimeUnit.MILLISECONDS));
    Assert.assertNotEquals(uuid1.toString(), uuid3.toString());
    Assert.assertNotEquals(uuid2.toString(), uuid3.toString());

    // Time from a random UUID should be -1
    Assert.assertEquals(-1, RunIds.getTime(RunIds.fromString(UUID.randomUUID().toString()), TimeUnit.MILLISECONDS));
  }
}

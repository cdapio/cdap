/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.queue;

import io.cdap.cdap.proto.id.ScheduleId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link JobKey}.
 */
public class JobKeyTest {

  @Test
  public void testToStringAndEquality() {
    ScheduleId scheduleId = new ScheduleId("default", "MyApp", "Sched1");
    JobKey key1 = new JobKey(scheduleId, 7);
    JobKey key2 = new JobKey(new ScheduleId("default", "MyApp", "Sched1"), 7);
    JobKey differentGen = new JobKey(scheduleId, 8);

    Assert.assertEquals(scheduleId, key1.getScheduleId());
    Assert.assertEquals(7, key1.getGenerationId());
    Assert.assertEquals(key1, key2);
    Assert.assertEquals(key1.hashCode(), key2.hashCode());
    Assert.assertNotEquals(key1, differentGen);
    Assert.assertEquals(
        "JobKey{scheduleId=" + scheduleId + ", generationId=7}",
        key1.toString());
  }
}

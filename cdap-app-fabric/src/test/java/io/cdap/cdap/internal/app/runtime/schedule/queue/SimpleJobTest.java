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

import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.WorkflowId;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SimpleJob}.
 */
public class SimpleJobTest {

  @Test
  public void testToStringAndDeletionState() {
    WorkflowId workflowId = NamespaceId.DEFAULT.app("MyApp").workflow("MyWorkflow");
    ProgramSchedule schedule = new ProgramSchedule(
        "Hourly", "desc", workflowId, Collections.emptyMap(),
        new TimeTrigger("0 * * * *"), Collections.emptyList());

    SimpleJob job = new SimpleJob(
        schedule, 2, 12345L, Collections.emptyList(), Job.State.PENDING_TRIGGER, 10000L);
    Assert.assertFalse(job.isToBeDeleted());
    Assert.assertNull(job.getDeleteTimeMillis());
    Assert.assertEquals(
        "SimpleJob{schedule=" + schedule
            + ", creationTime=12345"
            + ", jobKey=" + job.getJobKey()
            + ", notifications=[]"
            + ", state=PENDING_TRIGGER"
            + ", scheduleLastUpdatedTime=10000"
            + ", deleteTimeMillis=null}",
        job.toString());

    job.setToBeDeleted(99999L);
    Assert.assertTrue(job.isToBeDeleted());
    Assert.assertEquals(Long.valueOf(99999L), job.getDeleteTimeMillis());
    Assert.assertEquals(
        "SimpleJob{schedule=" + schedule
            + ", creationTime=12345"
            + ", jobKey=" + job.getJobKey()
            + ", notifications=[]"
            + ", state=PENDING_TRIGGER"
            + ", scheduleLastUpdatedTime=10000"
            + ", deleteTimeMillis=99999}",
        job.toString());
  }
}

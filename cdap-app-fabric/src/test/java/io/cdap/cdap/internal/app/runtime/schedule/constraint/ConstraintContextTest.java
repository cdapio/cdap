/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule.constraint;

import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.WorkflowId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ConstraintContext#getWorkflowToken(ProgramRunId)}.
 */
public class ConstraintContextTest {

  @Test
  public void testGetWorkflowTokenForWorkflowRun() {
    Store store = Mockito.mock(Store.class);
    Job job = Mockito.mock(Job.class);
    WorkflowId workflowId = NamespaceId.DEFAULT.app("app1").workflow("wf1");
    ProgramRunId runId = workflowId.run("run-1");
    WorkflowToken expectedToken = new BasicWorkflowToken(1);
    Mockito.when(store.getWorkflowToken(workflowId, "run-1")).thenReturn(expectedToken);

    ConstraintContext context = new ConstraintContext(job, 1000L, store);
    Assert.assertSame(expectedToken, context.getWorkflowToken(runId));
  }

  @Test
  public void testGetWorkflowTokenForNonWorkflowRunReturnsNull() {
    Store store = Mockito.mock(Store.class);
    Job job = Mockito.mock(Job.class);
    ProgramRunId sparkRunId =
        NamespaceId.DEFAULT.app("app1").program(ProgramType.SPARK, "spark1").run("run-1");

    ConstraintContext context = new ConstraintContext(job, 1000L, store);
    Assert.assertNull(context.getWorkflowToken(sparkRunId));
    Mockito.verifyNoInteractions(store);
  }
}

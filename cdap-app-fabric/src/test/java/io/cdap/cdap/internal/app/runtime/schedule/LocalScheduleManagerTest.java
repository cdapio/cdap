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

package io.cdap.cdap.internal.app.runtime.schedule;

import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LocalScheduleManager}.
 */
public class LocalScheduleManagerTest {

  private static final WorkflowId WORKFLOW_ID = NamespaceId.DEFAULT.app("app1").workflow("wf1");
  private static final ProgramSchedule WORKFLOW_SCHEDULE = new ProgramSchedule(
      "sched1", "desc", WORKFLOW_ID, Collections.emptyMap(),
      new TimeTrigger("0 * * * *"), Collections.emptyList());

  @Test(expected = BadRequestException.class)
  public void testAddSchedulesRejectsNonWorkflow() throws Exception {
    ProgramId mrId = NamespaceId.DEFAULT.app("app1").program(ProgramType.MAPREDUCE, "mr1");
    ProgramSchedule invalidSchedule = new ProgramSchedule(
        "schedMr", "desc", mrId, Collections.emptyMap(),
        new TimeTrigger("0 * * * *"), Collections.emptyList());

    LocalScheduleManager manager = createManager(runnable -> {
      try {
        runnable.run(Mockito.mock(StructuredTableContext.class));
      } catch (Exception e) {
        throw new TransactionException("tx", e);
      }
    });
    manager.addSchedules(Collections.singletonList(invalidSchedule));
  }

  @Test
  public void testAddSchedulesPropagatesDeclaredExceptionsDirectly() throws Exception {
    NotFoundException notFound = new NotFoundException(WORKFLOW_ID);
    LocalScheduleManager notFoundManager = createManager(runnable -> {
      throw new TransactionException("tx", notFound);
    });
    try {
      notFoundManager.addSchedules(Collections.singletonList(WORKFLOW_SCHEDULE));
      Assert.fail("Expected NotFoundException");
    } catch (NotFoundException thrown) {
      Assert.assertSame(notFound, thrown);
    }

    AlreadyExistsException alreadyExists = new AlreadyExistsException(WORKFLOW_SCHEDULE.getScheduleId());
    LocalScheduleManager existsManager = createManager(runnable -> {
      throw new TransactionException("tx", alreadyExists);
    });
    try {
      existsManager.addSchedules(Collections.singletonList(WORKFLOW_SCHEDULE));
      Assert.fail("Expected AlreadyExistsException");
    } catch (AlreadyExistsException thrown) {
      Assert.assertSame(alreadyExists, thrown);
    }

    ProfileConflictException profileConflict =
        new ProfileConflictException("disabled", new ProfileId("default", "p1"));
    LocalScheduleManager conflictManager = createManager(runnable -> {
      throw new TransactionException("tx", profileConflict);
    });
    try {
      conflictManager.addSchedules(Collections.singletonList(WORKFLOW_SCHEDULE));
      Assert.fail("Expected ProfileConflictException");
    } catch (ProfileConflictException thrown) {
      Assert.assertSame(profileConflict, thrown);
    }
  }

  @Test
  public void testAddSchedulesPropagatesUncheckedExceptionUnwrapped() throws Exception {
    IllegalStateException expected = new IllegalStateException("scheduler failure");
    LocalScheduleManager manager = createManager(runnable -> {
      throw new TransactionException("tx failed", expected);
    });

    try {
      manager.addSchedules(Collections.singletonList(WORKFLOW_SCHEDULE));
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(expected, thrown);
    }
  }

  @Test
  public void testAddSchedulesWrapsCheckedTransactionException() throws Exception {
    TransactionException txException = new TransactionException("storage unavailable", null);
    LocalScheduleManager manager = createManager(runnable -> {
      throw txException;
    });

    try {
      manager.addSchedules(Collections.singletonList(WORKFLOW_SCHEDULE));
      Assert.fail("Expected RuntimeException wrapping TransactionException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(txException, thrown.getCause());
    }
  }

  private LocalScheduleManager createManager(TransactionRunner txRunner) {
    return new LocalScheduleManager(
        txRunner,
        Mockito.mock(MessagingService.class),
        CConfiguration.create(),
        Mockito.mock(TimeSchedulerService.class));
  }
}

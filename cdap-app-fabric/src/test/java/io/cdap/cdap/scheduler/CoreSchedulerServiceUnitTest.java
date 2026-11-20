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

package io.cdap.cdap.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.SchedulerException;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.PropertiesResolver;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link CoreSchedulerService} startup ordering and exception propagation.
 */
public class CoreSchedulerServiceUnitTest {

  private static final class RecordingTimeSchedulerService
      extends AbstractIdleService implements TimeSchedulerService {

    private final List<String> events;

    RecordingTimeSchedulerService(List<String> events) {
      this.events = events;
    }

    @Override
    protected void startUp() {
      events.add("start:time");
    }

    @Override
    protected void shutDown() {
      events.add("stop:time");
    }

    @Override
    public void addProgramSchedule(ProgramSchedule schedule)
        throws AlreadyExistsException, SchedulerException {
    }

    @Override
    public void deleteProgramSchedule(ProgramSchedule schedule) throws SchedulerException {
    }

    @Override
    public void suspendProgramSchedule(ProgramSchedule schedule)
        throws NotFoundException, SchedulerException {
    }

    @Override
    public void resumeProgramSchedule(ProgramSchedule schedule)
        throws NotFoundException, SchedulerException {
    }

    @Override
    public List<ScheduledRuntime> previousScheduledRuntime(ProgramId program)
        throws SchedulerException {
      return Collections.emptyList();
    }

    @Override
    public List<ScheduledRuntime> nextScheduledRuntime(ProgramId program)
        throws SchedulerException {
      return Collections.emptyList();
    }

    @Override
    public List<ScheduledRuntime> getAllScheduledRunTimes(
        ProgramId program,
        SchedulableProgramType programType,
        long startTimeSecs,
        long endTimeSecs) throws SchedulerException {
      return Collections.emptyList();
    }
  }

  private static final class RecordingConstraintCheckerService extends ConstraintCheckerService {
    private final List<String> events;

    RecordingConstraintCheckerService(
        CConfiguration cConf, Store store, TransactionRunner txRunner, List<String> events) {
      super(
          store,
          Mockito.mock(ProgramLifecycleService.class),
          Mockito.mock(PropertiesResolver.class),
          Mockito.mock(NamespaceQueryAdmin.class),
          cConf,
          txRunner,
          new NoOpMetricsCollectionService());
      this.events = events;
    }

    @Override
    protected void startUp() {
      events.add("start:constraint");
    }

    @Override
    protected void shutDown() {
      events.add("stop:constraint");
    }
  }

  private static final class RecordingSubscriberService
      extends ScheduleNotificationSubscriberService {

    private final List<String> events;

    RecordingSubscriberService(
        CConfiguration cConf,
        MessagingService messagingService,
        TransactionRunner txRunner,
        List<String> events) {
      super(cConf, messagingService, new NoOpMetricsCollectionService(), txRunner);
      this.events = events;
    }

    @Override
    protected void startUp() {
      events.add("start:subscriber");
    }

    @Override
    protected void shutDown() {
      events.add("stop:subscriber");
    }
  }

  @Test
  public void testStartupAndShutdownOrderAndUnstartedChecks() throws Exception {
    List<String> events = Collections.synchronizedList(new ArrayList<>());
    CConfiguration cConf = CConfiguration.create();
    Store store = Mockito.mock(Store.class);
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    Impersonator impersonator = Mockito.mock(Impersonator.class);
    TransactionRunner txRunner = runnable -> {
      throw new TransactionException("skip startup cleanup", null);
    };

    TimeSchedulerService timeScheduler = new RecordingTimeSchedulerService(events);
    ConstraintCheckerService constraintChecker =
        new RecordingConstraintCheckerService(cConf, store, txRunner, events);
    ScheduleNotificationSubscriberService subscriberService =
        new RecordingSubscriberService(cConf, messagingService, txRunner, events);

    CoreSchedulerService service = new CoreSchedulerService(
        timeScheduler,
        subscriberService,
        constraintChecker,
        messagingService,
        cConf,
        store,
        impersonator,
        txRunner);

    try {
      service.waitUntilFunctional(1, TimeUnit.SECONDS);
      Assert.fail("Expected IllegalStateException before service starts");
    } catch (IllegalStateException expected) {
      // Expected.
    }

    try {
      service.listSchedules(NamespaceId.DEFAULT.app("app1"));
      Assert.fail("Expected ServiceUnavailableException before service starts");
    } catch (ServiceUnavailableException expected) {
      // Expected.
    }

    service.startAndWait();
    service.waitUntilFunctional(5, TimeUnit.SECONDS);
    Assert.assertEquals(Service.State.RUNNING, service.state());
    Assert.assertEquals(
        ImmutableList.of("start:time", "start:constraint", "start:subscriber"),
        events);

    service.stopAndWait();
    Assert.assertEquals(Service.State.TERMINATED, service.state());
    Assert.assertEquals(
        ImmutableList.of(
            "start:time",
            "start:constraint",
            "start:subscriber",
            "stop:subscriber",
            "stop:constraint",
            "stop:time"),
        events);
  }

  @Test
  public void testScheduleOperationsPropagateExceptionsWithoutDoubleWrapping() throws Exception {
    List<String> events = Collections.synchronizedList(new ArrayList<>());
    CConfiguration cConf = CConfiguration.create();
    Store store = Mockito.mock(Store.class);
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    Impersonator impersonator = Mockito.mock(Impersonator.class);
    TransactionRunner txRunner = Mockito.mock(TransactionRunner.class);

    AtomicReference<Exception> txExceptionRef = new AtomicReference<>(
        new TransactionException("skip startup cleanup", null));
    Mockito.doAnswer(invocation -> {
      Exception toThrow = txExceptionRef.get();
      if (toThrow != null) {
        throw toThrow;
      }
      return null;
    }).when(txRunner).run(Mockito.any(TxRunnable.class));

    TimeSchedulerService timeScheduler = new RecordingTimeSchedulerService(events);
    ConstraintCheckerService constraintChecker =
        new RecordingConstraintCheckerService(cConf, store, txRunner, events);
    ScheduleNotificationSubscriberService subscriberService =
        new RecordingSubscriberService(cConf, messagingService, txRunner, events);

    CoreSchedulerService service = new CoreSchedulerService(
        timeScheduler,
        subscriberService,
        constraintChecker,
        messagingService,
        cConf,
        store,
        impersonator,
        txRunner);

    service.startAndWait();
    service.waitUntilFunctional(5, TimeUnit.SECONDS);
    try {
      // Non-workflow program type throws BadRequestException directly.
      ProgramId mapReduceId = NamespaceId.DEFAULT.app("app1").mr("mr1");
      ProgramSchedule invalidSchedule = new ProgramSchedule(
          "sched-mr", "desc", mapReduceId, Collections.emptyMap(),
          new TimeTrigger("0 * * * *"), Collections.emptyList());
      try {
        service.addSchedule(invalidSchedule);
        Assert.fail("Expected BadRequestException for non-workflow schedule");
      } catch (BadRequestException expected) {
        // Expected.
      }

      ProgramId workflowId = NamespaceId.DEFAULT.app("app1").workflow("wf1");
      ProgramSchedule validSchedule = new ProgramSchedule(
          "sched-wf", "desc", workflowId, Collections.emptyMap(),
          new TimeTrigger("0 * * * *"), Collections.emptyList());
      ScheduleId scheduleId = validSchedule.getScheduleId();

      // Unchecked RuntimeException inside execute(...) must NOT be double-wrapped.
      IllegalArgumentException rawUnchecked = new IllegalArgumentException("bad argument");
      txExceptionRef.set(rawUnchecked);

      try {
        service.addSchedule(validSchedule);
        Assert.fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException caught) {
        Assert.assertSame(rawUnchecked, caught);
      }

      try {
        service.enableSchedule(scheduleId);
        Assert.fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException caught) {
        Assert.assertSame(rawUnchecked, caught);
      }

      try {
        service.disableSchedule(scheduleId);
        Assert.fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException caught) {
        Assert.assertSame(rawUnchecked, caught);
      }

      try {
        service.reEnableSchedules(NamespaceId.DEFAULT, 0L, 100L);
        Assert.fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException caught) {
        Assert.assertSame(rawUnchecked, caught);
      }

      // Checked domain exceptions (NotFoundException / ConflictException) rethrow unwrapped.
      NotFoundException notFound =
          new NotFoundException(scheduleId);
      txExceptionRef.set(new TransactionException("tx not found", notFound));
      try {
        service.enableSchedule(scheduleId);
        Assert.fail("Expected NotFoundException");
      } catch (NotFoundException caught) {
        Assert.assertSame(notFound, caught);
      }

      ConflictException conflict = new ConflictException("already enabled");
      txExceptionRef.set(new TransactionException("tx conflict", conflict));
      try {
        service.reEnableSchedules(NamespaceId.DEFAULT, 0L, 100L);
        Assert.fail("Expected ConflictException");
      } catch (ConflictException caught) {
        Assert.assertSame(conflict, caught);
      }

      // Checked TransactionException without cause is wrapped once in RuntimeException.
      TransactionException txFailure = new TransactionException("tx failed", null);
      txExceptionRef.set(txFailure);
      try {
        service.addSchedule(validSchedule);
        Assert.fail("Expected RuntimeException wrapping TransactionException");
      } catch (RuntimeException caught) {
        Assert.assertSame(txFailure, caught.getCause());
      }
    } finally {
      txExceptionRef.set(null);
      service.stopAndWait();
    }
  }
}

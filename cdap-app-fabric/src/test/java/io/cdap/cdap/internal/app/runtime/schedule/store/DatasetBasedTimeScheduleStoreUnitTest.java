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

package io.cdap.cdap.internal.app.runtime.schedule.store;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.scheduler.LogPrintingJob;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;

/**
 * Fast unit tests for {@link DatasetBasedTimeScheduleStore} exception propagation.
 */
public class DatasetBasedTimeScheduleStoreUnitTest {

  @Test
  public void testUncheckedExceptionPropagatedUnwrappedAcrossStoreOperations() throws Exception {
    IllegalStateException expected = new IllegalStateException("storage failure");
    TransactionRunner failingRunner = runnable -> {
      throw new TransactionException("tx failure", expected);
    };
    DatasetBasedTimeScheduleStore store =
        new DatasetBasedTimeScheduleStore(failingRunner, CConfiguration.create());

    CascadingClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();
    SchedulerSignaler signaler = Mockito.mock(SchedulerSignaler.class);

    assertThrowsSame(expected, () -> store.initialize(loadHelper, signaler));

    JobDetail job = JobBuilder.newJob(LogPrintingJob.class)
        .withIdentity("job1", "group1")
        .storeDurably()
        .build();
    assertThrowsSame(expected, () -> store.storeJob(job, true));

    OperableTrigger trigger = (OperableTrigger) TriggerBuilder.newTrigger()
        .withIdentity("trig1", "group1")
        .forJob(job)
        .withSchedule(CronScheduleBuilder.cronSchedule("0 0/5 * * * ?"))
        .build();
    assertThrowsSame(expected, () -> store.storeTrigger(trigger, true));

    TriggerKey triggerKey = trigger.getKey();
    assertThrowsSame(expected, () -> store.pauseTrigger(triggerKey));
    assertThrowsSame(expected, () -> store.resumeTrigger(triggerKey));
    assertThrowsSame(expected, () -> store.removeTrigger(triggerKey));
    assertThrowsSame(expected, () -> store.removeJob(new JobKey("job1", "group1")));

    try {
      store.pauseTrigger(null);
      Assert.fail("Expected NullPointerException");
    } catch (NullPointerException expectedNpe) {
      Assert.assertNull(expectedNpe.getCause());
    }
  }

  @Test
  public void testCheckedTransactionExceptionWrappedOnce() throws Exception {
    TransactionException txException = new TransactionException("checked storage error", null);
    TransactionRunner failingRunner = runnable -> {
      throw txException;
    };
    DatasetBasedTimeScheduleStore store =
        new DatasetBasedTimeScheduleStore(failingRunner, CConfiguration.create());

    CascadingClassLoadHelper loadHelper = new CascadingClassLoadHelper();
    loadHelper.initialize();
    SchedulerSignaler signaler = Mockito.mock(SchedulerSignaler.class);

    assertWrappedCause(txException, () -> store.initialize(loadHelper, signaler));
    assertWrappedCause(txException, () -> store.removeTrigger(new TriggerKey("t1", "g1")));
    assertWrappedCause(txException, () -> store.removeJob(new JobKey("j1", "g1")));
    assertWrappedCause(txException, () -> store.pauseTrigger(new TriggerKey("t1", "g1")));
  }

  private void assertThrowsSame(RuntimeException expected, ThrowingAction action) throws Exception {
    try {
      action.run();
      Assert.fail("Expected exception " + expected);
    } catch (RuntimeException thrown) {
      Assert.assertSame(expected, thrown);
    }
  }

  private void assertWrappedCause(Throwable expectedCause, ThrowingAction action) throws Exception {
    try {
      action.run();
      Assert.fail("Expected RuntimeException wrapping " + expectedCause);
    } catch (RuntimeException thrown) {
      Assert.assertSame(expectedCause, thrown.getCause());
    }
  }

  private interface ThrowingAction {
    void run() throws Exception;
  }
}

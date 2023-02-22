/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.AndTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.OrTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This tests the indexing of the schedule store. Adding, retrieving. listing, deleting schedules is tested
 * in {@link io.cdap.cdap.scheduler.CoreSchedulerServiceTest}, which has equivalent methods that execute
 * in a transaction.
 */
public abstract class ProgramScheduleStoreDatasetTest extends AppFabricTestBase {

  private static final NamespaceId NS1_ID = new NamespaceId("schedtest");
  private static final NamespaceId NS2_ID = new NamespaceId("schedtestNs2");
  private static final ApplicationId APP1_ID = NS1_ID.app("app1", "1");
  private static final ApplicationId APP2_ID = NS1_ID.app("app2");
  private static final ApplicationId APP3_ID = NS2_ID.app("app3", "1");
  private static final ApplicationReference APP1_REFERENCE = APP1_ID.getAppReference();
  private static final ApplicationReference APP2_REFERENCE = APP2_ID.getAppReference();
  private static final ApplicationReference APP3_REFERENCE = APP3_ID.getAppReference();
  private static final WorkflowId PROG1_ID = APP1_ID.workflow("wf1");
  private static final WorkflowId PROG2_ID = APP2_ID.workflow("wf2");
  private static final WorkflowId PROG3_ID = APP2_ID.workflow("wf3");
  private static final WorkflowId PROG4_ID = APP3_ID.workflow("wf4");
  private static final WorkflowId PROG5_ID = APP3_ID.workflow("wf5");
  private static final ProgramReference PROG1_REFERENCE = PROG1_ID.getProgramReference();
  private static final ProgramReference PROG2_REFERENCE = PROG2_ID.getProgramReference();
  private static final ProgramReference PROG3_REFERENCE = PROG3_ID.getProgramReference();
  private static final ProgramReference PROG4_REFERENCE = PROG4_ID.getProgramReference();
  private static final ProgramReference PROG5_REFERENCE = PROG5_ID.getProgramReference();
  private static final DatasetId DS1_ID = NS1_ID.dataset("pfs1");
  private static final DatasetId DS2_ID = NS1_ID.dataset("pfs2");

  protected abstract TransactionRunner getTransactionRunner();
  
  @Before
  public void beforeTest() {
    TransactionRunner transactionRunner = getTransactionRunner();
    // Delete all data in the tables
    TransactionRunners.run(
      transactionRunner,
      context -> {
        context.getTable(StoreDefinition.ProgramScheduleStore.PROGRAM_SCHEDULE_TABLE).deleteAll(Range.all());
        context.getTable(StoreDefinition.ProgramScheduleStore.PROGRAM_TRIGGER_TABLE).deleteAll(Range.all());
      }
    );
  }

  @Test
  public void testListSchedules() {
    TransactionRunner transactionRunner = getTransactionRunner();

    final ProgramSchedule sched1 = new ProgramSchedule("sched1", "one partition schedule",
                                                       PROG1_REFERENCE, Collections.emptyMap(),
                                                       new PartitionTrigger(DS1_ID, 1),
                                                       Collections.emptyList());
    final ProgramSchedule sched2 = new ProgramSchedule("sched2", "time schedule", PROG2_REFERENCE,
                                                       Collections.emptyMap(), new TimeTrigger("* * * 1 1"),
                                                       Collections.emptyList());
    final ProgramSchedule sched3 = new ProgramSchedule("sched3", "two partitions schedule", PROG4_REFERENCE,
                                                       Collections.emptyMap(), new PartitionTrigger(DS1_ID, 2),
                                                       Collections.emptyList());
    final ProgramSchedule sched4 = new ProgramSchedule("sched4", "time schedule", PROG5_REFERENCE,
                                                       Collections.emptyMap(), new TimeTrigger("* * * 2 1"),
                                                       Collections.emptyList());

    // assert no schedules exists before adding schedules
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        Assert.assertTrue(store.listSchedules(NS1_ID, schedule -> true).isEmpty());
        Assert.assertTrue(store.listSchedules(NS2_ID, schedule -> true).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(APP1_REFERENCE).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(APP2_REFERENCE).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(APP3_REFERENCE).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(PROG1_REFERENCE).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(PROG2_REFERENCE).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(PROG3_REFERENCE).isEmpty());
        Assert.assertTrue(store.listScheduleRecords(PROG4_REFERENCE).isEmpty());
      }
    );

    // add schedules to the store
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.addSchedules(ImmutableList.of(sched1, sched2, sched3, sched4));
      }
    );

    // list schedules by namespace
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        Assert.assertEquals(ImmutableSet.of(sched1, sched2),
                            new HashSet<>(store.listSchedules(NS1_ID, schedule -> true)));
        Assert.assertEquals(ImmutableSet.of(sched3, sched4),
                            new HashSet<>(store.listSchedules(NS2_ID, schedule -> true)));
        // list schedules by app
        Assert.assertEquals(ImmutableSet.of(sched1),
                            toScheduleSet(store.listScheduleRecords(APP1_REFERENCE)));
        Assert.assertEquals(ImmutableSet.of(sched2),
                            toScheduleSet(store.listScheduleRecords(APP2_REFERENCE)));
        Assert.assertEquals(ImmutableSet.of(sched3, sched4),
                            toScheduleSet(store.listScheduleRecords(APP3_REFERENCE)));
        // list schedules by program
        Assert.assertEquals(ImmutableSet.of(sched1),
                            toScheduleSet(store.listScheduleRecords(PROG1_REFERENCE)));
        Assert.assertEquals(ImmutableSet.of(sched2),
                            toScheduleSet(store.listScheduleRecords(PROG2_REFERENCE)));
        Assert.assertEquals(ImmutableSet.of(sched3),
                            toScheduleSet(store.listScheduleRecords(PROG4_REFERENCE)));
        Assert.assertEquals(ImmutableSet.of(sched4),
                            toScheduleSet(store.listScheduleRecords(PROG5_REFERENCE)));
      }
    );

    // disable a schedule in NS_1
    long startTime = System.currentTimeMillis();
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.updateScheduleStatus(sched1.getScheduleId(), ProgramScheduleStatus.SUSPENDED);
      }
    );
    // add one since end is exclusive
    long endTime = System.currentTimeMillis() + 1;

    // test list schedule disabled
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        Assert.assertEquals(ImmutableList.of(sched1), store.listSchedulesSuspended(NS1_ID, startTime, endTime));
      }
    );

    // test deletion
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.deleteSchedules(APP1_REFERENCE, System.currentTimeMillis());
        Assert.assertEquals(ImmutableSet.of(),
                            toScheduleSet(store.listScheduleRecords(APP1_REFERENCE)));
        store.deleteSchedules(PROG2_REFERENCE, System.currentTimeMillis());
        Assert.assertEquals(ImmutableSet.of(),
                            toScheduleSet(store.listScheduleRecords(PROG2_REFERENCE)));
        store.deleteSchedules(APP3_REFERENCE, System.currentTimeMillis());
        Assert.assertEquals(ImmutableSet.of(),
                            toScheduleSet(store.listScheduleRecords(APP3_REFERENCE)));
      }
    );
  }

  @Test
  public void testFindSchedulesByEventAndUpdateSchedule() {
    TransactionRunner transactionRunner = getTransactionRunner();

    final ProgramSchedule sched11 = new ProgramSchedule("sched11", "one partition schedule",
                                                        PROG1_REFERENCE, ImmutableMap.of("prop3", "abc"),
                                                        new PartitionTrigger(DS1_ID, 1),
                                                        ImmutableList.<Constraint>of());
    final ProgramSchedule sched12 = new ProgramSchedule("sched12", "two partition schedule",
                                                        PROG1_REFERENCE, ImmutableMap.of("propper", "popper"),
                                                        new PartitionTrigger(DS2_ID, 2),
                                                        ImmutableList.<Constraint>of());
    final ProgramSchedule sched22 = new ProgramSchedule("sched22", "twentytwo partition schedule",
                                                        PROG2_REFERENCE, ImmutableMap.of("nn", "4"),
                                                        new PartitionTrigger(DS2_ID, 22),
                                                        ImmutableList.<Constraint>of());
    final ProgramSchedule sched31 = new ProgramSchedule("sched31", "a program status trigger",
                                                        PROG3_REFERENCE, ImmutableMap.of("propper", "popper"),
                                                        new ProgramStatusTrigger(PROG1_REFERENCE,
                                                                                 ProgramStatus.COMPLETED,
                                                                                 ProgramStatus.FAILED,
                                                                                 ProgramStatus.KILLED),
                                                        ImmutableList.<Constraint>of());

    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // event for DS1 or DS2 should trigger nothing. validate it returns an empty collection
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForPartition(DS1_ID)).isEmpty());
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForPartition(DS2_ID)).isEmpty());
        // event for PROG1 should trigger nothing. it should also return an empty collection
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE,
                                                                                    ProgramStatus.COMPLETED))
                            .isEmpty());
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE,
                                                                                    ProgramStatus.FAILED))
                            .isEmpty());
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE,
                                                                                    ProgramStatus.KILLED))
                            .isEmpty());
      }
    );
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.addSchedules(ImmutableList.of(sched11, sched12, sched22, sched31));
      }
    );
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // event for ProgramStatus should trigger only sched31
        Assert.assertEquals(ImmutableSet.of(sched31),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE, ProgramStatus.COMPLETED))));

        Assert.assertEquals(ImmutableSet.of(sched31),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE, ProgramStatus.FAILED))));

        Assert.assertEquals(ImmutableSet.of(sched31),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE, ProgramStatus.KILLED))));

        // event for DS1 should trigger only sched11
        Assert.assertEquals(ImmutableSet.of(sched11),
                            toScheduleSet(store.findSchedules(Schedulers.triggerKeyForPartition(DS1_ID))));
        // event for DS2 triggers only sched12 and sched22
        Assert.assertEquals(ImmutableSet.of(sched12, sched22),
                            toScheduleSet(store.findSchedules(Schedulers.triggerKeyForPartition(DS2_ID))));
      }
    );

    final ProgramSchedule sched11New = new ProgramSchedule(sched11.getName(), "time schedule",
                                                           PROG1_REFERENCE, ImmutableMap.of("timeprop", "time"),
                                                           new TimeTrigger("* * * * *"),
                                                           ImmutableList.<Constraint>of());
    final ProgramSchedule sched12New = new ProgramSchedule(sched12.getName(), "one partition schedule",
                                                           PROG1_REFERENCE, ImmutableMap.of("pp", "p"),
                                                           new PartitionTrigger(DS1_ID, 2),
                                                           ImmutableList.<Constraint>of());
    final ProgramSchedule sched22New = new ProgramSchedule(sched22.getName(), "program3 failed schedule",
                                                           PROG2_REFERENCE, ImmutableMap.of("ss", "s"),
                                                           new ProgramStatusTrigger(PROG3_REFERENCE,
                                                                                    ProgramStatus.FAILED),
                                                           ImmutableList.<Constraint>of());
    final ProgramSchedule sched31New = new ProgramSchedule(sched31.getName(), "program1 failed schedule",
                                                           PROG3_REFERENCE, ImmutableMap.of("abcd", "efgh"),
                                                           new ProgramStatusTrigger(PROG1_REFERENCE,
                                                                                    ProgramStatus.FAILED),
                                                           ImmutableList.<Constraint>of());

    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.updateSchedule(sched11New);
        store.updateSchedule(sched12New);
        store.updateSchedule(sched22New);
        store.updateSchedule(sched31New);
      }
    );
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // event for DS1 should trigger only sched12New after update
        Assert.assertEquals(ImmutableSet.of(sched12New),
                            toScheduleSet(store.findSchedules(Schedulers.triggerKeyForPartition(DS1_ID))));
        // event for DS2 triggers no schedule after update
        Assert.assertEquals(ImmutableSet.<ProgramSchedule>of(),
                            toScheduleSet(store.findSchedules(Schedulers.triggerKeyForPartition(DS2_ID))));

        // event for PS triggers only for failed program statuses, not completed nor killed
        Assert.assertEquals(ImmutableSet.of(sched31New),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE, ProgramStatus.FAILED))));
        Assert.assertEquals(ImmutableSet.of(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE, ProgramStatus.COMPLETED))));
        Assert.assertEquals(ImmutableSet.of(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE, ProgramStatus.KILLED))));
      }
    );
  }

  private Set<ProgramSchedule> toScheduleSet(Collection<ProgramScheduleRecord> records) {
    return records.stream().map(ProgramScheduleRecord::getSchedule).collect(Collectors.toSet());
  }

  @Test
  public void testDeleteScheduleByTriggeringProgram() {
    TransactionRunner transactionRunner = getTransactionRunner();

    SatisfiableTrigger prog1Trigger = new ProgramStatusTrigger(PROG1_REFERENCE, ProgramStatus.COMPLETED,
                                                               ProgramStatus.FAILED, ProgramStatus.KILLED);
    SatisfiableTrigger prog2Trigger = new ProgramStatusTrigger(PROG2_REFERENCE, ProgramStatus.COMPLETED,
                                                               ProgramStatus.FAILED, ProgramStatus.KILLED);
    final ProgramSchedule sched1 = new ProgramSchedule("sched1", "a program status trigger",
                                                       PROG3_REFERENCE, ImmutableMap.of("propper", "popper"),
                                                       prog1Trigger, ImmutableList.<Constraint>of());
    final ProgramSchedule sched2 = new ProgramSchedule("sched2", "a program status trigger",
                                                       PROG3_REFERENCE, ImmutableMap.of("propper", "popper"),
                                                       prog2Trigger, ImmutableList.<Constraint>of());

    final ProgramSchedule schedOr = new ProgramSchedule("schedOr", "an OR trigger", PROG3_REFERENCE,
                                                        ImmutableMap.of("propper", "popper"),
                                                        new OrTrigger(new PartitionTrigger(DS1_ID, 1),
                                                                      prog1Trigger,
                                                                      new AndTrigger(new OrTrigger(prog1Trigger,
                                                                                                   prog2Trigger),
                                                                                     new PartitionTrigger(DS2_ID, 1)),
                                                                      new OrTrigger(prog2Trigger)),
                                                        ImmutableList.<Constraint>of());

    final ProgramSchedule schedAnd = new ProgramSchedule("schedAnd", "an AND trigger", PROG3_REFERENCE,
                                                        ImmutableMap.of("propper", "popper"),
                                                        new AndTrigger(new PartitionTrigger(DS1_ID, 1),
                                                                       prog2Trigger,
                                                                       new AndTrigger(prog1Trigger,
                                                                                      new PartitionTrigger(DS2_ID, 1))),
                                                         ImmutableList.<Constraint>of());

    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.addSchedules(ImmutableList.of(sched1, sched2, schedOr, schedAnd));
      }
    );
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // ProgramStatus event for PROG1_ID should trigger only sched1, schedOr, schedAnd
        Assert.assertEquals(ImmutableSet.of(sched1, schedOr, schedAnd),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE, ProgramStatus.COMPLETED))));
        // ProgramStatus event for PROG2_ID should trigger only sched2, schedOr, schedAnd
        Assert.assertEquals(ImmutableSet.of(sched2, schedOr, schedAnd),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG2_REFERENCE, ProgramStatus.FAILED))));
      }
    );
    // update or delete all schedules triggered by PROG1_ID
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.modifySchedulesTriggeredByDeletedProgram(PROG1_REFERENCE);
      }
    );
    final ProgramSchedule schedOrNew = new ProgramSchedule("schedOr", "an OR trigger", PROG3_REFERENCE,
                                                           ImmutableMap.of("propper", "popper"),
                                                           new OrTrigger(new PartitionTrigger(DS1_ID, 1),
                                                                         new AndTrigger(prog2Trigger,
                                                                                        new PartitionTrigger(DS2_ID,
                                                                                                             1)),
                                                                         prog2Trigger),
                                                           ImmutableList.of());
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // ProgramStatus event for PROG1_ID should trigger no schedules after modifying schedules triggered by it
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE, ProgramStatus.COMPLETED))));
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE, ProgramStatus.FAILED))));
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG1_REFERENCE, ProgramStatus.KILLED))));
        // ProgramStatus event for PROG2_ID should trigger only sched2 and schedOrNew
        Assert.assertEquals(ImmutableSet.of(sched2, schedOrNew),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG2_REFERENCE, ProgramStatus.FAILED))));
      }
    );
    // update or delete all schedules triggered by PROG2_ID
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        store.modifySchedulesTriggeredByDeletedProgram(PROG2_REFERENCE);
      }
    );
    final ProgramSchedule schedOrNew1 = new ProgramSchedule("schedOr", "an OR trigger", PROG3_REFERENCE,
                                                           ImmutableMap.of("propper", "popper"),
                                                           new PartitionTrigger(DS1_ID, 1),
                                                           ImmutableList.of());
    final Set<ProgramSchedule> ds1Schedules = new HashSet<>();
    TransactionRunners.run(
      transactionRunner,
      context -> {
        ProgramScheduleStoreDataset store = Schedulers.getScheduleStore(context);
        // ProgramStatus event for PROG2_ID should trigger no schedules after modifying schedules triggered by it
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG2_REFERENCE, ProgramStatus.COMPLETED))));
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG2_REFERENCE, ProgramStatus.FAILED))));
        Assert.assertEquals(Collections.emptySet(),
                            toScheduleSet(store.findSchedules(
                              Schedulers.triggerKeyForProgramStatus(PROG2_REFERENCE, ProgramStatus.KILLED))));
        // event for DS1 should trigger only schedOrNew1 since all other schedules are deleted
        ds1Schedules.addAll(toScheduleSet(store.findSchedules(Schedulers.triggerKeyForPartition(DS1_ID))));
      }
    );
    Assert.assertEquals(ImmutableSet.of(schedOrNew1), ds1Schedules);
  }
}

/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.store;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.data.runtime.DynamicTransactionExecutorFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.cdap.scheduler.CoreSchedulerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionSystemClient;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

/**
 * This tests the indexing of the schedule store. Adding, retrieving. listing, deleting schedules is tested
 * in {@link co.cask.cdap.scheduler.CoreSchedulerServiceTest}, which has equivalent methods that execute
 * in a transaction.
 */
public class ProgramScheduleStoreDatasetTest extends AppFabricTestBase {

  private static final NamespaceId NS_ID = new NamespaceId("schedtest");
  private static final ApplicationId APP1_ID = NS_ID.app("app1");
  private static final ApplicationId APP2_ID = NS_ID.app("app2");
  private static final WorkflowId PROG1_ID = APP1_ID.workflow("wf1");
  private static final WorkflowId PROG2_ID = APP2_ID.workflow("wf2");
  private static final DatasetId DS1_ID = NS_ID.dataset("pfs1");
  private static final DatasetId DS2_ID = NS_ID.dataset("pfs2");

  @Test
  public void checkDatasetType() throws DatasetManagementException {
    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);
    Assert.assertTrue(dsFramework.hasType(NamespaceId.SYSTEM.datasetType(ProgramScheduleStoreDataset.class.getName())));
  }

  @Test
  public void testFindSchedulesByEvent() throws Exception {

    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);
    TransactionSystemClient txClient = getInjector().getInstance(TransactionSystemClient.class);
    TransactionExecutorFactory txExecutorFactory = new DynamicTransactionExecutorFactory(txClient);
    final ProgramScheduleStoreDataset store = dsFramework.getDataset(Schedulers.STORE_DATASET_ID,
                                                                     new HashMap<String, String>(), null);
    Assert.assertNotNull(store);
    TransactionExecutor txExecutor = txExecutorFactory.createExecutor(Collections.singleton((TransactionAware) store));

    final ProgramSchedule sched11 = new ProgramSchedule("sched11", "one partition schedule", PROG1_ID,
                                                        ImmutableMap.of("prop3", "abc"),
                                                        new PartitionTrigger(DS1_ID, 1),
                                                        ImmutableList.<Constraint>of());
    final ProgramSchedule sched12 = new ProgramSchedule("sched12", "two partition schedule", PROG1_ID,
                                                        ImmutableMap.of("propper", "popper"),
                                                        new PartitionTrigger(DS2_ID, 2),
                                                        ImmutableList.<Constraint>of());
    final ProgramSchedule sched22 = new ProgramSchedule("sched22", "twentytwo partition schedule", PROG2_ID,
                                                        ImmutableMap.of("nn", "4"),
                                                        new PartitionTrigger(DS2_ID, 22),
                                                        ImmutableList.<Constraint>of());

    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // event for DS1 or DS2 should trigger nothing. validate it returns an empty collection
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForPartition(DS1_ID)).isEmpty());
        Assert.assertTrue(store.findSchedules(Schedulers.triggerKeyForPartition(DS2_ID)).isEmpty());
      }
    });
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        store.addSchedules(ImmutableList.of(sched11, sched12, sched22));
      }
    });
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // event for DS1 should trigger only sched11
        Assert.assertEquals(ImmutableSet.of(sched11),
                            ImmutableSet.copyOf(store.findSchedules(Schedulers.triggerKeyForPartition(DS1_ID))));
        // event for DS2 triggers only sched12 and sched22
        Assert.assertEquals(ImmutableSet.of(sched12, sched22),
                            ImmutableSet.copyOf(store.findSchedules(Schedulers.triggerKeyForPartition(DS2_ID))));
      }
    });
  }


}

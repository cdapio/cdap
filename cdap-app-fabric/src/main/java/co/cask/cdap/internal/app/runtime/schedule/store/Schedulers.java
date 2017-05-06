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

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.constraint.ConcurrencyConstraint;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Common utility methods for scheduling.
 */
public class Schedulers {
  public static final String STORE_TYPE_NAME = ProgramScheduleStoreDataset.class.getName();
  public static final DatasetId STORE_DATASET_ID = NamespaceId.SYSTEM.dataset("schedule.store");
  public static final DatasetId JOB_QUEUE_DATASET_ID = NamespaceId.SYSTEM.dataset("job.queue");

  public static String triggerKeyForPartition(DatasetId datasetId) {
    return "partition:" + datasetId.getNamespace() + '.' + datasetId.getDataset();
  }

  public static JobQueueDataset getJobQueue(DatasetContext context, DatasetFramework dsFramework) {
    try {
      return DatasetsUtil.getOrCreateDataset(context, dsFramework, Schedulers.JOB_QUEUE_DATASET_ID,
                                             JobQueueDataset.class.getName(), DatasetProperties.EMPTY);
    } catch (DatasetManagementException | IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static ProgramSchedule toProgramSchedule(TimeSchedule timeSchedule, ProgramId programId,
                                                  Map<String, String> properties) {
    TimeTrigger trigger = new TimeTrigger(timeSchedule.getCronEntry());
    Integer maxConcurrentRuns = timeSchedule.getRunConstraints().getMaxConcurrentRuns();
    List<Constraint> constraints = maxConcurrentRuns == null ? ImmutableList.<Constraint>of() :
      ImmutableList.<Constraint>of(new ConcurrencyConstraint(maxConcurrentRuns));
    return new ProgramSchedule(timeSchedule.getName(), timeSchedule.getDescription(), programId,
                               properties, trigger, constraints);
  }
}

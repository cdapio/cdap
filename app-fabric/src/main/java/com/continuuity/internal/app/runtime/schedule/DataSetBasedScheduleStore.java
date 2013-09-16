package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.commons.lang.SerializationUtils;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.simpl.RAMJobStore;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * ScheduleStore extends from RAMJobStore and persists the trigger and schedule information into datasets.
 */
public class DataSetBasedScheduleStore extends RAMJobStore {

  private static final Logger LOG = LoggerFactory.getLogger(DataSetBasedScheduleStore.class);

  //TODO: Use the namespacing feature from datasetmanager once it is in develop
  private final String SCHEDULE_STORE_DATASET_NAME = "system:schedulestore";

  private static final byte[] JOB_KEY = Bytes.toBytes("jobs");
  private static final byte[] TRIGGER_KEY = Bytes.toBytes("trigger");

  private final DataSetAccessor dataSetAccessor;

  @Inject
  public DataSetBasedScheduleStore(DataSetAccessor dataSetAccessor)
                       throws UnsupportedTypeException, OperationException, JobPersistenceException {
    this.dataSetAccessor = dataSetAccessor;
  }

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler schedSignaler) {
    super.initialize(loadHelper, schedSignaler);
    try {
      createDatasetIfNotExists();
      readSchedulesFromPersistentStore();
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }

  private void createDatasetIfNotExists() throws Exception {
    DataSetManager dataSetManager = dataSetAccessor.getDataSetManager(OrderedColumnarTable.class);
    Preconditions.checkNotNull(dataSetManager, "Dataset Manager cannot be null");
 
    if (!dataSetManager.exists(SCHEDULE_STORE_DATASET_NAME)) {
      dataSetManager.create(SCHEDULE_STORE_DATASET_NAME);
    }
  }

  @Override
  public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
                                 throws JobPersistenceException {
    super.storeJob(newJob, true);
    super.storeTrigger(newTrigger, true);

    try {
      LOG.debug("Schedule: storing job with key {}", newJob.getKey());
      OrderedColumnarTable table = dataSetAccessor.getDataSetClient(SCHEDULE_STORE_DATASET_NAME,
                                                                    OrderedColumnarTable.class);
      Preconditions.checkNotNull(table, String.format("Could not get dataset client for data set: %s",
                                                      SCHEDULE_STORE_DATASET_NAME));
      persistJob(table, newJob);

      LOG.debug("Schedule: storing trigger with key {}", newTrigger.getKey());
      persistTrigger(table, newTrigger);
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }

  // Persist the job information to dataset
  private void persistJob(OrderedColumnarTable table, JobDetail job) throws Exception {
    byte[][] cols = new byte[1][];
    byte[][] values = new byte[1][];

    cols[0] = Bytes.toBytes(job.getKey().toString());
    values[0] = SerializationUtils.serialize(job);

    table.put(JOB_KEY, cols, values);
  }

  // Persist the trigger information to dataset
  private void persistTrigger(OrderedColumnarTable table, OperableTrigger trigger) throws Exception {

    byte[][] cols = new byte[1][];
    byte[][] values = new byte[1][];

    cols[0] = Bytes.toBytes(trigger.getKey().toString());
    values[0] = SerializationUtils.serialize(trigger);

    table.put(TRIGGER_KEY, cols, values);
  }

  // Get schedule information from persistent store
  private void readSchedulesFromPersistentStore() throws Exception {

    OrderedColumnarTable table = dataSetAccessor.getDataSetClient(SCHEDULE_STORE_DATASET_NAME,
                                                                  OrderedColumnarTable.class);
    Preconditions.checkNotNull(table, String.format("Could not get dataset client for data set: %s",
                                                    SCHEDULE_STORE_DATASET_NAME));

    try {
      OperationResult<Map<byte[], byte[]>> result = table.get(JOB_KEY, null, null, -1);
      if (!result.isEmpty()) {
        for (byte[] bytes : result.getValue().values()){
          JobDetail jobDetail = (JobDetail) SerializationUtils.deserialize(bytes);
          LOG.debug("Schedule: Job with key {} found", jobDetail.getKey());
          super.storeJob(jobDetail, true);
        }
      } else {
        LOG.debug("Schedule: No Jobs found in Job store");
      }

      result = table.get(TRIGGER_KEY, null, null, -1);
      if (!result.isEmpty()) {
        for (byte[] bytes : result.getValue().values()){
          OperableTrigger trigger = (OperableTrigger) SerializationUtils.deserialize(bytes);
          super.storeTrigger(trigger, true);
          LOG.debug("Schedule: trigger with key {} found", trigger.getKey());
        }
      } else {
        LOG.debug("Schedule: No triggers found in job store");
      }
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }
  }
}

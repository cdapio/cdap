package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.queue.StreamAdmin;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * admin for streams in hbase.
 */
@Singleton
public class HBaseStreamAdmin extends HBaseQueueAdmin implements StreamAdmin {
  private static final Class[] COPROCESSORS = new Class[]{DEQUEUE_CP};

  @Inject
  public HBaseStreamAdmin(@Named("HBaseOVCTableHandleHConfig") Configuration hConf,
                          @Named("HBaseOVCTableHandleCConfig") CConfiguration cConf,
                          DataSetAccessor dataSetAccessor,
                          LocationFactory locationFactory) throws IOException {
    super(hConf, cConf, QueueConstants.QueueType.STREAM, dataSetAccessor, locationFactory);
  }

  @Override
  public String getActualTableName(QueueName queueName) {
    if (queueName.isStream()) {
      // <reactor namespace>.system.stream.<stream name>
      return getTableNamePrefix() + "." + queueName.getFirstComponent();
    } else {
      throw new IllegalArgumentException("'" + queueName + "' is not a valid name for a stream.");
    }
  }

  @Override
  public boolean doDropTable(QueueName queueName) {
    // separate table for each stream, ok to drop
    return true;
  }

  @Override
  public boolean doTruncateTable(QueueName queueName) {
    // separate table for each stream, ok to truncate
    return true;
  }

  @Override
  protected Class[] getCoprocessors() {
    // we don't want eviction CP here, hence overriding
    return COPROCESSORS;
  }
}

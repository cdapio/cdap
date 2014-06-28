package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.List;

/**
 * admin for streams in hbase.
 */
@Singleton
public class HBaseStreamAdmin extends HBaseQueueAdmin implements StreamAdmin {

  @Inject
  public HBaseStreamAdmin(Configuration hConf,
                          CConfiguration cConf,
                          DataSetAccessor dataSetAccessor,
                          LocationFactory locationFactory,
                          HBaseTableUtil tableUtil) throws IOException {
    super(hConf, cConf, QueueConstants.QueueType.STREAM, dataSetAccessor, locationFactory, tableUtil);
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
  protected List<? extends Class<? extends Coprocessor>> getCoprocessors() {
    // we don't want eviction CP here, hence overriding
    return ImmutableList.of(tableUtil.getDequeueScanObserverClassForVersion());
  }

  @Override
  public StreamConfig getConfig(String streamName) throws IOException {
    return null;
  }

  @Override
  public void updateConfig(StreamConfig config) throws IOException {

  }

}

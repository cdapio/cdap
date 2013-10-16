package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
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

  @Inject
  public HBaseStreamAdmin(@Named("HBaseOVCTableHandleHConfig") Configuration hConf,
                          @Named("HBaseOVCTableHandleCConfig") CConfiguration cConf,
                          DataSetAccessor dataSetAccessor,
                          LocationFactory locationFactory) throws IOException {
    super(hConf, cConf, "stream", dataSetAccessor, locationFactory);
  }
}

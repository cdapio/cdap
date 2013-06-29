/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.hbase.HBaseOVCTable;
import com.continuuity.data.engine.hbase.HBaseOVCTableHandle;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 *
 */
public class HBaseFilterableOVCTableHandle extends HBaseOVCTableHandle {

  @Inject
  public HBaseFilterableOVCTableHandle(CConfiguration conf, Configuration hConf) throws IOException {
    super(conf, hConf);
  }

  @Override
  public String getName() {
    return "hbase_filterable";
  }

  @Override
  protected HBaseOVCTable createOVCTable(byte[] tableName) throws OperationException {
    return new HBaseFilterableOVCTable(conf, hConf, tableName, FAMILY, new HBaseIOExceptionHandler());
  }
}

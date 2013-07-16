/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.engine.hbase;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * This class implements the table handle for an "improved" HBase that has extensions to handle transactions.
 */
public class HBaseNativeOVCTableHandle extends HBaseOVCTableHandle {

  @Inject
  public HBaseNativeOVCTableHandle(@Named("HBaseOVCTableHandleCConfig")CConfiguration conf,
                                   @Named("HBaseOVCTableHandleHConfig")Configuration hConf) throws IOException {
    super(conf, hConf);
  }

  @Override
  public String getName() {
    return "native";
  }

  @Override
  protected HBaseOVCTable createOVCTable(byte[] tableName) throws OperationException {
    return new HBaseNativeOVCTable(conf, hConf, tableName, FAMILY, new HBaseIOExceptionHandler());
  }

}

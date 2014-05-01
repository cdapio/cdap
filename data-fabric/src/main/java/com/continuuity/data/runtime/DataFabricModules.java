/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.runtime.RuntimeModule;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * DataFabricModules defines all of the bindings for the different data
 * fabric modes.
 */
public class DataFabricModules extends RuntimeModule {
  private final CConfiguration cConf;
  private final Configuration hbaseConf;

  public DataFabricModules() {
    this(CConfiguration.create(), HBaseConfiguration.create());
  }

  public DataFabricModules(CConfiguration cConf) {
    this(cConf, HBaseConfiguration.create());
  }

  public DataFabricModules(CConfiguration cConf, Configuration hbaseConf) {
    this.cConf = cConf;
    this.hbaseConf = hbaseConf;
  }

  @Override
  public Module getInMemoryModules() {
    return new DataFabricInMemoryModule(cConf);
  }

  @Override
  public Module getSingleNodeModules() {
    return new DataFabricLocalModule(cConf);
  }

  @Override
  public Module getDistributedModules() {
    return new DataFabricDistributedModule(cConf, hbaseConf);
  }

} // end of DataFabricModules

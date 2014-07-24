/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.metadata;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.hbase.HBaseTestFactory;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data.runtime.TransactionMetricsModule;
import com.continuuity.tephra.TransactionSystemClient;
import com.continuuity.tephra.inmemory.InMemoryTxSystemClient;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * HBase meta data store tests.
 */
public abstract class HBaseMetaDataStoreTest extends MetaDataTableTest {

  protected static Injector injector;
  private static HBaseTestBase testHBase;

  @BeforeClass
  public static void setupDataFabric() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Zookeeper.QUORUM, testHBase.getZkConnectionString());
    // tests should interact with HDFS as the current user
    conf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    DataFabricDistributedModule dfModule = new DataFabricDistributedModule();
    Module module = Modules.override(dfModule).with(
      new AbstractModule() {
        @Override
        protected void configure() {
          // prevent going through network for transactions
          bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class);
        }
      });
    injector = Guice.createInjector(module,
                                    new ConfigModule(conf, testHBase.getConfiguration()),
                                    new ZKClientModule(),
                                    new DiscoveryRuntimeModule().getDistributedModules(),
                                    new TransactionMetricsModule(),
                                    new LocationRuntimeModule().getDistributedModules());
  }

  @AfterClass
  public static void stopHBase()
    throws Exception {
    testHBase.stopHBase();
  }
}

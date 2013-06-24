package com.continuuity.performance.opex;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

/**
 * OpexProvider for benchmarks that use HBase.
 */
public class HBaseOpexProvider extends OpexProvider {

  String zkQuorum = null;

  @Override
  public void configure(CConfiguration config) {
    zkQuorum = config.get("zk");
  }

  @Override
  public OperationExecutor create() {
    Configuration hbaseConf = HBaseConfiguration.create();
    if (zkQuorum != null) {
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
    }
    hbaseConf.set("hbase.defaults.for.version.skip", "true");
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(DataFabricDistributedModule.CONF_ENABLE_NATIVE_QUEUES, false);
    Module module = new DataFabricDistributedModule(hbaseConf, conf);
    Injector injector = Guice.createInjector(module);
    return injector.getInstance(Key.get(OperationExecutor.class,
        Names.named("DataFabricOperationExecutor")));
  }

}

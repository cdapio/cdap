package com.continuuity.data.operation.executor.benchmark;

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

import java.util.ArrayList;

public class HBaseOpexProvider extends OpexProvider {

  String zkQuorum = null;

  @Override
  public String[] configure(String[] args) throws BenchmarkException {
    ArrayList<String> remaining = new ArrayList<String>();
    for (int i = 0; i < args.length; i++) {
      if ("--zk".equals(args[i])) {
        if (i + 1 < args.length) {
          zkQuorum = args[++i];
        } else {
          throw new BenchmarkException("--zk must have an argument. ");
        }
      } else {
        remaining.add(args[i]);
      }
    }
    return remaining.toArray(new String[remaining.size()]);
  }

  @Override
  public OperationExecutor create() {
    Configuration hbaseConf = HBaseConfiguration.create();
    if (zkQuorum != null)
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
    hbaseConf.set("hbase.defaults.for.version.skip", "true");
    Module module = new DataFabricDistributedModule(hbaseConf);
    Injector injector = Guice.createInjector(module);
    return injector.getInstance(Key.get(OperationExecutor.class,
        Names.named("DataFabricOperationExecutor")));
  }

}

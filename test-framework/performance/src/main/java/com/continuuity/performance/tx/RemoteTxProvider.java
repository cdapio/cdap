package com.continuuity.performance.tx;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TxConstants;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * TxProvider for benchmarks running against a remote data-fabric.
 */
public class RemoteTxProvider extends TxProvider {

  String zkQuorum = null;
  String host = null;
  int port = -1;
  int timeout = -1;

  @Override
  public void configure(CConfiguration config) {
    zkQuorum = config.get("zk");
    host = config.get("host");
    port = config.getInt("port", -1);
    timeout = config.getInt("timeout", -1);
  }

  @Override
  TransactionSystemClient create() {

    CConfiguration cConf = CConfiguration.create();

    if (zkQuorum != null) {
      cConf.set(Constants.Zookeeper.QUORUM, zkQuorum);
    }
    if (host != null) {
      cConf.set(TxConstants.Service.CFG_DATA_TX_BIND_ADDRESS, host);
      // don't use zookeeper-based service discovery if tx service host is given
      cConf.unset(Constants.Zookeeper.QUORUM);
    }
    if (port != -1) {
      cConf.setInt(TxConstants.Service.CFG_DATA_TX_BIND_ADDRESS, port);
    }
    if (timeout != -1) {
      cConf.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_TIMEOUT, timeout);
    }

    Injector injector = Guice.createInjector(new ConfigModule(cConf),
                                             new DataFabricModules().getDistributedModules(),
                                             new LocationRuntimeModule().getDistributedModules());
    return injector.getInstance(TransactionSystemClient.class);
  }
}

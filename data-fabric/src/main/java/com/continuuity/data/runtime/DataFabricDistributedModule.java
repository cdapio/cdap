package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.DistributedDataSetAccessor;
import com.continuuity.data.engine.hbase.HBaseOVCTableHandle;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.operation.executor.remote.RemoteOperationExecutor;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.NoopPersistor;
import com.continuuity.data2.transaction.inmemory.StatePersistor;
import com.continuuity.data2.transaction.inmemory.ZooKeeperPersistor;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueAdmin;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueClientFactory;
import com.continuuity.data2.transaction.server.TalkingToOpexTxSystemClient;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines guice bindings for distributed modules.
 */
public class DataFabricDistributedModule extends AbstractModule {

  private static final Logger Log =
      LoggerFactory.getLogger(DataFabricDistributedModule.class);

  private final CConfiguration conf;

  private final Configuration hbaseConf;

  /**
   * Create a module with default configuration for HBase and Continuuity.
   */
  public DataFabricDistributedModule() {
    this.conf = loadConfiguration();
    this.hbaseConf = HBaseConfiguration.create();
  }

  /**
   * Create a module with custom configuration for HBase,
   * and defaults for Continuuity.
   */
  public DataFabricDistributedModule(Configuration conf) {
    this.hbaseConf = new Configuration(conf);
    this.conf = loadConfiguration();
  }

  /**
   * Create a module with custom configuration, which will
   * be used both for HBase and for Continuuity.
   */
  public DataFabricDistributedModule(CConfiguration conf) {
    this.hbaseConf = new Configuration();
    this.conf = conf;
  }

  private CConfiguration loadConfiguration() {
    @SuppressWarnings("UnnecessaryLocalVariable") CConfiguration conf = CConfiguration.create();

    // this expects the port and number of threads for the opex service
    // - data.opex.server.port <int>
    // - data.opex.server.threads <int>
    // this expects the zookeeper quorum for continuuity and for hbase
    // - zookeeper.quorum host:port,...
    // - hbase.zookeeper.quorum host:port,...
    return conf;
  }

  @Override
  public void configure() {

    Class<? extends OVCTableHandle> ovcTableHandle = HBaseOVCTableHandle.class;
    Log.info("Table Handle is " + ovcTableHandle.getName());

    // Bind our implementations

    // Bind remote operation executor
    bind(OperationExecutor.class).to(RemoteOperationExecutor.class).in(Singleton.class);

    // For data fabric, bind to Omid and HBase
    bind(OperationExecutor.class).annotatedWith(Names.named("DataFabricOperationExecutor"))
        .to(OmidTransactionalOperationExecutor.class).in(Singleton.class);
    bind(OVCTableHandle.class).to(ovcTableHandle);

    // For now, just bind to in-memory omid oracles
    bind(TimestampOracle.class).to(MemoryStrictlyMonotonicTimeOracle.class).in(Singleton.class);
    bind(TransactionOracle.class).to(MemoryOracle.class).in(Singleton.class);

    // Bind HBase configuration into ovctable
    bind(Configuration.class).annotatedWith(Names.named("HBaseOVCTableHandleHConfig")).toInstance(hbaseConf);

    // Bind Continuuity configuration into ovctable
    bind(CConfiguration.class).annotatedWith(Names.named("HBaseOVCTableHandleCConfig")).toInstance(conf);

    // Bind our configurations
    bind(CConfiguration.class).annotatedWith(Names.named("RemoteOperationExecutorConfig")).toInstance(conf);
    bind(CConfiguration.class).annotatedWith(Names.named("DataFabricOperationExecutorConfig")).toInstance(conf);

    // Bind TxDs2 stuff
    if (conf.getBoolean(StatePersistor.CFG_DO_PERSIST, true)) {
      bind(StatePersistor.class).to(ZooKeeperPersistor.class).in(Singleton.class);
    } else {
      bind(StatePersistor.class).to(NoopPersistor.class).in(Singleton.class);
    }
    bind(DataSetAccessor.class).to(DistributedDataSetAccessor.class).in(Singleton.class);
    bind(InMemoryTransactionManager.class).in(Singleton.class);
    bind(TransactionSystemClient.class).to(TalkingToOpexTxSystemClient.class).in(Singleton.class);
    bind(QueueClientFactory.class).to(HBaseQueueClientFactory.class).in(Singleton.class);
    bind(QueueAdmin.class).to(HBaseQueueAdmin.class).in(Singleton.class);
  }

  public CConfiguration getConfiguration() {
    return this.conf;
  }

}

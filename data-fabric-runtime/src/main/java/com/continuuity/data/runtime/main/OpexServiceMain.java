package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.service.CommandPortService;
import com.continuuity.common.service.RUOKHandler;
import com.continuuity.common.twill.TwillRunnerMain;
import com.continuuity.data.runtime.DataFabricOpexModule;
import com.continuuity.data.security.HBaseSecureStoreUpdater;
import com.continuuity.data.security.HBaseTokenUtils;
import com.continuuity.data2.util.hbase.ConfigurationTable;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.Credentials;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.yarn.YarnSecureStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * Driver class to start (and stop?) tx in distributed mode using Twill.
 */
public class OpexServiceMain extends TwillRunnerMain {

  private static final Logger LOG = LoggerFactory.getLogger(OpexServiceMain.class);

  private static final int START = 1;
  private static final int STOP = 2;

  private static CommandPortService service;

  static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = OpexServiceMain.class.getSimpleName();
    out.println("Usage: ");
    out.println("  " + name + " ( start | stop ) ");
  }

  public OpexServiceMain(CConfiguration cConf, Configuration hConf) {
    super(cConf, hConf);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      usage(true);
      return;
    }
    if ("--help".equals(args[0])) {
      usage(false);
      return;
    }

    int command;

    if ("start".equals(args[0])) {
      command = START;
    } else if ("stop".equals(args[0])) {
      command = STOP;
    } else {
      usage(true);
      return;
    }

    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create(new HdfsConfiguration());

    DataFabricOpexModule module = new DataFabricOpexModule(cConf, hConf);

    Injector injector = Guice.createInjector(new ConfigModule(cConf, hConf),
                                             new IOModule(),
                                             new ZKClientModule(),
                                             new KafkaClientModule(),
                                             new LocationRuntimeModule().getDistributedModules(),
                                             new DiscoveryRuntimeModule().getDistributedModules(),
                                             new MetricsClientRuntimeModule().getDistributedModules(),
                                             module);

    if (START == command) {
      LOG.info("Starting OpexService Main...");
      service = startHealthCheckService(cConf);
      // populate the current configuration into an HBase table, for use by HBase components
      ConfigurationTable configTable = new ConfigurationTable(injector.getInstance(
        Key.get(Configuration.class, Names.named("HBaseOVCTableHandleHConfig"))));
      configTable.write(ConfigurationTable.Type.DEFAULT, cConf);
    }
    new OpexServiceMain(CConfiguration.create(), HBaseConfiguration.create()).doMain(args);
    service.stop();
    LOG.info("Stopping OpexService Main...");
  }

  private static CommandPortService startHealthCheckService(CConfiguration conf) {
    int port = conf.getInt(Constants.Transaction.Service.CFG_DATA_TX_COMMAND_PORT, 0);
    CommandPortService service = CommandPortService.builder("tx-status")
      .setPort(port)
      .addCommandHandler(RUOKHandler.COMMAND, RUOKHandler.DESCRIPTION, new RUOKHandler())
      .build();
    service.startAndWait();
    return service;
  }

  @Override
  protected TwillApplication createTwillApplication() {
    try {
      return new TransactionServiceTwillApplication(cConf, getSavedCConf(), getSavedHConf());
    } catch (Exception e) {
      throw  Throwables.propagate(e);
    }
  }

  @Override
  protected void scheduleSecureStoreUpdate(TwillRunner twillRunner) {
    if (User.isHBaseSecurityEnabled(hConf)) {
      HBaseSecureStoreUpdater updater = new HBaseSecureStoreUpdater(hConf);
      twillRunner.scheduleSecureStoreUpdate(updater, 30000L, updater.getUpdateInterval(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  protected TwillPreparer prepare(TwillPreparer preparer) {
    return preparer.withDependencies(new HBaseTableUtilFactory().get().getClass())
      .addSecureStore(YarnSecureStore.create(HBaseTokenUtils.obtainToken(hConf, new Credentials())));
  }
}

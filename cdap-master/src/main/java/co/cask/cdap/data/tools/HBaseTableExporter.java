/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
package co.cask.cdap.data.tools;

import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.RemoteUGIProvider;
import co.cask.cdap.security.impersonation.UGIProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.TransactionService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Tool to export the HBase table to HFiles. Tool accepts the HBase table name as input parameter and outputs
 * the HDFS path where the corresponding HFiles are exported.
 */
public class HBaseTableExporter {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableExporter.class);

  private final Configuration hConf;
  private final TransactionService txService;
  private final ZKClientService zkClientService;
  private final TransactionSystemClient txClient;
  private Path bulkloadDir = null;

  public HBaseTableExporter() throws Exception {
    this.hConf = HBaseConfiguration.create();
    Injector injector = createInjector(CConfiguration.create(), hConf);
    this.txClient = injector.getInstance(TransactionSystemClient.class);
    this.txService = injector.getInstance(TransactionService.class);
    this.zkClientService = injector.getInstance(ZKClientService.class);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          HBaseTableExporter.this.stop();
        } catch (Throwable e) {
          LOG.error("Failed to stop the tool.", e);
        }
      }
    });

  }

  @VisibleForTesting
  public static Injector createInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataFabricModules(HBaseTableExporter.class.getName()).getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new MessagingClientModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new ExploreClientModule(),
      new ViewAdminModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedServiceRuntimeModule().getDistributedModules(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new AuthenticationContextModules().getMasterModule(),
      new NamespaceClientRuntimeModule().getDistributedModules(),
      new SecureStoreModules().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(UGIProvider.class).to(RemoteUGIProvider.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      }
    );
  }

  /**
   * A mapper that just writes KeyValues.
   */
  static class KeyValueImporter extends TableMapper<ImmutableBytesWritable, KeyValue> {
    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
      InterruptedException {
      for (Cell kv : value.rawCells()) {
        context.write(row, KeyValueUtil.ensureKeyValue(kv));
      }
    }
  }

  /**
   * Sets up the actual MapReduce job.
   * @param tx The transaction which needs to be passed to the Scan instance. This transaction is be used by
   *           coprocessors to filter out the data corresonding to the invalid transactions .
   * @param tableName Name of the table which need to be exported as HFiles.
   * @return the configured job
   * @throws IOException
   */
  public Job createSubmittableJob(Transaction tx, String tableName) throws IOException {

    Job job = Job.getInstance(hConf, "HBaseTableExporter");

    job.setJarByClass(HBaseTableExporter.class);
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    // Set the transaction attribute for the scan.
    scan.setAttribute(TxConstants.TX_OPERATION_ATTRIBUTE_KEY, new TransactionCodec().encode(tx));
    job.setNumReduceTasks(0);

    TableMapReduceUtil.initTableMapperJob(tableName, scan, KeyValueImporter.class, null, null, job);

    FileSystem fs = FileSystem.get(hConf);
    Random rand = new Random();
    Path root = new Path(fs.getWorkingDirectory(), "hbasetableexporter");
    fs.mkdirs(root);
    while (true) {
      bulkloadDir = new Path(root, "" + rand.nextLong());
      if (!fs.exists(bulkloadDir)) {
        break;
      }
    }

    HFileOutputFormat2.setOutputPath(job, bulkloadDir);
    HTable hTable = new HTable(hConf, tableName);
    HFileOutputFormat2.configureIncrementalLoad(job, hTable);

    return job;
  }

  private void startUp() throws Exception {
    zkClientService.startAndWait();
    txService.startAndWait();
  }

  /**
   * Stops a guava {@link Service}. No exception will be thrown even stopping failed.
   */
  private void stopQuietly(Service service) {
    try {
      service.stopAndWait();
    } catch (Exception e) {
      LOG.warn("Exception when stopping service {}", service, e);
    }
  }


  private void stop() throws Exception {
    stopQuietly(txService);
    stopQuietly(zkClientService);
  }

  private void printHelp() {
    System.out.println();
    System.out.println("Usage: /opt/cdap/master/bin/svc-master " +
                         "run co.cask.cdap.data.tools.HBaseTableExporter <tablename>");
    System.out.println("Args:");
    System.out.println(" tablename    Name of the table to copy");
  }

  public void doMain(String[] args) throws Exception {
    if (args.length < 1) {
      printHelp();
      return;
    }

    String tableName = args[0];

    try {
      startUp();
      Transaction tx = txClient.startLong();
      Job job = createSubmittableJob(tx, tableName);
      if (!job.waitForCompletion(true)) {
        LOG.info("MapReduce job failed!");
        throw new RuntimeException("Failed to run the MapReduce job.");
      }

      // Always commit the transaction, since we are not doing any data update
      // operation in this tool.
      txClient.commit(tx);
      System.out.println("Export operation complete. HFiles are stored at location " + bulkloadDir.toString());
    } finally {
      stop();
    }
  }

  public static void main(String[] args) throws Exception {
    try {
      HBaseTableExporter hBaseTableExporter = new HBaseTableExporter();
      hBaseTableExporter.doMain(args);
    } catch (Throwable t) {
      LOG.error("Failed to export the HBase table.", t);
    }
  }
}

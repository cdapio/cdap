/*
 * Copyright © 2015-2022 Cask Data, Inc.
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
package io.cdap.cdap.data.tools;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.RemoteUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.Import;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
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
  private Path bulkloadDir;

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
      RemoteAuthenticatorModules.getDefaultModule(),
      new IOModule(),
      new ZKClientModule(),
      new ZKDiscoveryModule(),
      new KafkaClientModule(),
      new DFSLocationModule(),
      new DataFabricModules(HBaseTableExporter.class.getName()).getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new SystemDatasetRuntimeModule().getDistributedModules(),
      new MessagingClientModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new KafkaLogAppenderModule(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getStandaloneModules(),
      new AuthenticationContextModules().getMasterModule(),
      new NamespaceQueryAdminModule(),
      new SecureStoreServerModule(),
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

    TableMapReduceUtil.initTableMapperJob(tableName, scan, Import.KeyValueImporter.class, null, null, job);

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

    TableName hbaseTableName = TableName.valueOf(tableName);
    HFileOutputFormat2.setOutputPath(job, bulkloadDir);
    try (Connection connection = ConnectionFactory.createConnection(hConf);
         Table table = connection.getTable(hbaseTableName);
         RegionLocator locator = connection.getRegionLocator(hbaseTableName)) {
      HFileOutputFormat2.configureIncrementalLoad(job, table, locator);
      return job;
    }
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
                         "run io.cdap.cdap.data.tools.HBaseTableExporter <tablename>");
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
      txClient.commitOrThrow(tx);
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

/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.TwillModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.metrics.NoOpMetricsSystemClient;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.common.zookeeper.coordination.DiscoverableCodec;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.InMemoryDatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.LineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpLineageWriter;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.data2.util.hbase.CoprocessorManager;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.logging.guice.KafkaLogAppenderModule;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.messaging.store.hbase.HBaseTableFactory;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.distributed.TransactionService;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * Command line tool for the Upgrade tool
 */
public class UpgradeTool {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeTool.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Discoverable.class, new DiscoverableCodec())
    .create();

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final TransactionService txService;
  private final ZKClientService zkClientService;
  private final DatasetFramework dsFramework;
  private final DatasetUpgrader dsUpgrade;
  private final HBaseTableFactory tmsTableFactory;
  private final CoprocessorManager coprocessorManager;

  /**
   * Set of Action available in this tool.
   */
  private enum Action {
    UPGRADE("Upgrades CDAP to " + ProjectInfo.getVersion() + "\n" +
              "  The upgrade tool upgrades the following: \n" +
              "  1. User and System Datasets (upgrades the coprocessor jars)\n" +
              "  2. Stream State Store\n" +
              "  Note: Once you run the upgrade tool you cannot rollback to the previous version."),
    UPGRADE_HBASE("After an HBase upgrade, updates the coprocessor jars of all user and \n" +
                    "system HBase tables to a version that is compatible with the new HBase \n" +
                    "version. All tables must be disabled prior to this step."),
    HELP("Show this help.");

    private final String description;

    Action(String description) {
      this.description = description;
    }

    private String getDescription() {
      return description;
    }
  }

  UpgradeTool() throws Exception {
    this.cConf = CConfiguration.create();
    if (this.cConf.getBoolean(Constants.Security.Authorization.ENABLED)) {
      LOG.info("Disabling authorization for {}.", getClass().getSimpleName());
      this.cConf.setBoolean(Constants.Security.Authorization.ENABLED, false);
    }
    // Note: login has to happen before any objects that need Kerberos credentials are instantiated.
    SecurityUtil.loginForMasterService(cConf);

    this.hConf = HBaseConfiguration.create();
    Injector injector = createInjector();
    this.txService = injector.getInstance(TransactionService.class);
    this.zkClientService = injector.getInstance(ZKClientService.class);
    this.dsFramework = injector.getInstance(DatasetFramework.class);
    this.dsUpgrade = injector.getInstance(DatasetUpgrader.class);
    this.tmsTableFactory = injector.getInstance(HBaseTableFactory.class);
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    HBaseTableUtil tableUtil = injector.getInstance(HBaseTableUtil.class);
    this.coprocessorManager = new CoprocessorManager(cConf, locationFactory, tableUtil);


    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        UpgradeTool.this.stop();
      } catch (Throwable e) {
        LOG.error("Failed to upgrade", e);
      }
    }));
  }

  @VisibleForTesting
  Injector createInjector() {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new DFSLocationModule(),
      new ZKClientModule(),
      new ZKDiscoveryModule(),
      new MessagingClientModule(),
      Modules.override(new DataSetsModules().getDistributedModules()).with(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(DatasetFramework.class).to(InMemoryDatasetFramework.class).in(Scopes.SINGLETON);
            // the DataSetsModules().getDistributedModules() binds to RemoteDatasetFramework so override that to
            // the same InMemoryDatasetFramework
            bind(DatasetFramework.class)
              .annotatedWith(Names.named(DataSetsModules.BASE_DATASET_FRAMEWORK))
              .to(DatasetFramework.class);
            bind(DatasetDefinitionRegistryFactory.class)
              .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);
            // CDAP-5954 Upgrade tool does not need to record lineage and metadata changes for now.
            bind(LineageWriter.class).to(NoOpLineageWriter.class);
            bind(FieldLineageWriter.class).to(NoOpLineageWriter.class);
          }
        }
      ),
      new TwillModule(),
      new ExploreClientModule(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new SystemDatasetRuntimeModule().getDistributedModules(),
      new KafkaClientModule(),
      new AuthenticationContextModules().getMasterModule(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new SecureStoreServerModule(),
      new DataFabricModules(UpgradeTool.class.getName()).getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new KafkaLogAppenderModule(),
      // the DataFabricDistributedModule needs MetricsCollectionService binding
      new AbstractModule() {
        @Override
        protected void configure() {
          // Since Upgrade tool does not do anything with Metrics we just bind it to no-op implementations
          bind(MetricsCollectionService.class).toInstance(new NoOpMetricsCollectionService());
          bind(MetricsSystemClient.class).toInstance(new NoOpMetricsSystemClient());
        }

        @Provides
        @Singleton
        @Named("datasetInstanceManager")
        @SuppressWarnings("unused")
        public DatasetInstanceManager getDatasetInstanceManager(TransactionRunner transactionRunner) {
          return new DatasetInstanceManager(transactionRunner);
        }

        // This is needed because the LocalApplicationManager
        // expects a dsframework injection named datasetMDS
        @Provides
        @Singleton
        @Named("datasetMDS")
        @SuppressWarnings("unused")
        public DatasetFramework getInDsFramework(DatasetFramework dsFramework) {
          return dsFramework;
        }

      },
      new AbstractModule() {
        @Override
        protected void configure() {
          // TODO (CDAP-14677): find a better way to inject metadata publisher
          bind(MetadataServiceClient.class).to(NoOpMetadataServiceClient.class);
        }
      }
    );
  }

  /**
   * Do the start up work
   * Note: includeNewDatasets boolean is required because upgrade tool has two mode: 1. Normal CDAP upgrade and
   * 2. Upgrading co processor for tables after hbase upgrade. This parameter specifies whether new system dataset
   * which were added in the current release needs to be added in the dataset framework or not.
   * During Normal CDAP upgrade (1) we don't need these datasets to be added in the ds framework as they will get
   * created during upgrade rather than when cdap starts after upgrade which is what we want.
   * Whereas during Hbase upgrade (2) we want these new tables to be added so that the co processor of these tables
   * can be upgraded when the user runs CDAP's Hbase Upgrade after upgrading to a newer version of Hbase.
   * @param includeNewDatasets boolean which specifies whether to add new datasets in ds framework or not
   */
  private void startUp(boolean includeNewDatasets) throws Exception {
    // Start all the services.
    LOG.info("Starting Zookeeper Client...");
    Services.startAndWait(zkClientService, cConf.getLong(Constants.Zookeeper.CLIENT_STARTUP_TIMEOUT_MILLIS),
                          TimeUnit.MILLISECONDS,
                          String.format("Connection timed out while trying to start ZooKeeper client. Please " +
                                          "verify that the ZooKeeper quorum settings are correct in cdap-site.xml. " +
                                          "Currently configured as: %s", cConf.get(Constants.Zookeeper.QUORUM)));
    LOG.info("Starting Transaction Service...");
    txService.startAndWait();
    LOG.info("Initializing Dataset Framework...");
    initializeDSFramework(dsFramework, includeNewDatasets);
    LOG.info("Building and uploading new HBase coprocessors...");
    coprocessorManager.ensureCoprocessorExists();
  }

  /**
   * Stop services and
   */
  private void stop() {
    try {
      txService.stopAndWait();
      zkClientService.stopAndWait();
    } catch (Throwable e) {
      LOG.error("Exception while trying to stop upgrade process", e);
      Runtime.getRuntime().halt(1);
    }
  }

  private void doMain(String[] args) throws Exception {
    System.out.println(String.format("%s - version %s.", getClass().getSimpleName(), ProjectInfo.getVersion()));
    System.out.println();

    if (args.length < 1) {
      printHelp();
      return;
    }

    Action action = parseAction(args[0]);
    if (action == null) {
      System.out.println(String.format("Unsupported action : %s", args[0]));
      printHelp(true);
      return;
    }
    // Set to interactive mode to true by default
    boolean interactive = true;

    if ((args.length >= 2) && (args[1]).equals("force")) {
      interactive = false;
      System.out.println("Starting upgrade in non interactive mode.");
    }

    try {
      switch (action) {
        case UPGRADE: {
          System.out.println(String.format("%s - %s", action.name().toLowerCase(), action.getDescription()));
          String response = getResponse(interactive);
          if (response.equalsIgnoreCase("y") || response.equalsIgnoreCase("yes")) {
            System.out.println("Starting upgrade ...");
            try {
              startUp(false);
              ensureCDAPMasterStopped();
              performUpgrade();
              System.out.println("\nUpgrade completed successfully.\n");
            } finally {
              stop();
            }
          } else {
            System.out.println("Upgrade cancelled.");
          }
          break;
        }
        case UPGRADE_HBASE: {
          System.out.println(String.format("%s - %s", action.name().toLowerCase(), action.getDescription()));
          String response = getResponse(interactive);
          if (response.equalsIgnoreCase("y") || response.equalsIgnoreCase("yes")) {
            System.out.println("Starting upgrade ...");
            try {
              startUp(true);
              performHBaseUpgrade();
              System.out.println("\nUpgrade completed successfully.\n");
            } finally {
              stop();
            }
          } else {
            System.out.println("Upgrade cancelled.");
          }
          break;
        }
        case HELP:
          printHelp();
          break;
      }
    } catch (Exception e) {
      System.out.println(String.format("Failed to perform action '%s'. Reason: '%s'.", action, e.getMessage()));
      throw e;
    }
  }

  /**
   * Checks for appfabric service path on zookeeper, if they exist, CDAP master is still running, so throw
   * exception message with information on where its running.
   * @throws Exception if at least one master is running
   */
  private void ensureCDAPMasterStopped() throws Exception {
    String appFabricPath = String.format("/discoverable/%s", Constants.Service.APP_FABRIC_HTTP);
    NodeChildren nodeChildren = zkClientService.getChildren(appFabricPath).get();
    List<String> runningNodes = new ArrayList<>();
    // if no children nodes at appfabric path, all master nodes are stopped
    if (!nodeChildren.getChildren().isEmpty()) {
      for (String runId : nodeChildren.getChildren()) {
        // only one children would be present, as only the active master will be registered at this path
        NodeData nodeData = zkClientService.getData(String.format("%s/%s", appFabricPath, runId)).get();
        Discoverable discoverable = GSON.fromJson(Bytes.toString(nodeData.getData()), Discoverable.class);
        runningNodes.add(discoverable.getSocketAddress().getHostName());
      }
      String exceptionMessage =
        String.format("CDAP Master is still running on %s, please stop it before running upgrade.",
                      com.google.common.base.Joiner.on(",").join(runningNodes));
      throw new Exception(exceptionMessage);
    }
    // CDAP-11733 As a future improvement, the upgrade tool can register as a CDAP master to become the leader
    // and prevent other masters from starting.
  }

  private String getResponse(boolean interactive) {
    if (interactive) {
      Scanner scan = new Scanner(System.in);
      System.out.println("Do you want to continue (y/n)");
      return scan.next();
    }
    return "y";
  }

  private void printHelp() {
    printHelp(false);
  }

  private void printHelp(boolean beginNewLine) {
    if (beginNewLine) {
      System.out.println();
    }
    System.out.println("Available actions: ");
    System.out.println();

    for (Action action : Action.values()) {
      System.out.println(String.format("%s - %s", action.name().toLowerCase(), action.getDescription()));
    }
  }

  private Action parseAction(String action) {
    try {
      return Action.valueOf(action.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private void performUpgrade() throws Exception {
    performCoprocessorUpgrade();
  }

  private void performHBaseUpgrade() throws Exception {
    System.setProperty(AbstractHBaseDataSetAdmin.SYSTEM_PROPERTY_FORCE_HBASE_UPGRADE, Boolean.TRUE.toString());
    performCoprocessorUpgrade();
  }

  private void performCoprocessorUpgrade() throws Exception {
    LOG.info("Disabling TMS Tables...");
    tmsTableFactory.disableMessageTable(cConf.get(Constants.MessagingSystem.MESSAGE_TABLE_NAME));
    tmsTableFactory.disablePayloadTable(cConf.get(Constants.MessagingSystem.PAYLOAD_TABLE_NAME));

    LOG.info("Upgrading User and System HBase Tables ...");
    dsUpgrade.upgrade();
  }

  public static void main(String[] args) {
    try {
      UpgradeTool upgradeTool = new UpgradeTool();
      upgradeTool.doMain(args);
    } catch (Throwable t) {
      LOG.error("Failed to upgrade ...", t);
      System.exit(1);
    }
  }

  /**
   * Sets up a {@link DatasetFramework} instance for standalone usage.  NOTE: should NOT be used by applications!!!
   * Note: includeNewDatasets boolean is required because upgrade tool has two mode: 1. Normal CDAP upgrade and
   * 2. Upgrading co processor for tables after hbase upgrade. This parameter specifies whether new system dataset
   * which were added in the current release needs to be added in the dataset framework or not.
   * During Normal CDAP upgrade (1) we don't need these datasets to be added in the ds framework as they will get
   * created during upgrade rather than when cdap starts after upgrade which is what we want.
   * Whereas during Hbase upgrade (2) we want these new tables to be added so that the co processor of these tables
   * can be upgraded when the user runs CDAP's Hbase Upgrade after upgrading to a newer version of Hbase.
   */
  private void initializeDSFramework(DatasetFramework datasetFramework, boolean includeNewDatasets)
    throws IOException, DatasetManagementException, UnauthorizedException {
    // Note: do no remove this block even if it's empty. Read the comment below and function doc above
    //noinspection StatementWithEmptyBody
    if (includeNewDatasets) {
      // Add all new system dataset introduced in the current release in this block. If no new dataset was introduced
      // then leave this block empty but do not remove block so that it can be used in next release if needed
    }

    DefaultStore.setupDatasets(datasetFramework);
    ProgramScheduleStoreDataset.setupDatasets(datasetFramework);
  }
}

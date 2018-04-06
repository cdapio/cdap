/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.app.guice.TwillModule;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsStoreModule;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.security.impersonation.SecurityUtil;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 *
 * Because the JobQueue is scanned over multiple transactions, it will be an inconsistent view.
 * The same Job will not be counted multiple times, but some Jobs may be missed if they were deleted or added during
 * the scan. The count of the Job State may also be inconsistent.
 *
 * The publish timestamp of the last message processed from the topics will also be inconsistent from the Jobs in the
 * JobQueue.
 */
public class StoreScanner extends AbstractIdleService {

  private static final Gson GSON = new GsonBuilder().create();

  private final CConfiguration cConf;
  private final ZKClientService zkClientService;
  private final Store store;

  @Inject
  public StoreScanner(CConfiguration cConf, ZKClientService zkClientService, Store store) {
    this.cConf = cConf;
    this.zkClientService = zkClientService;
    this.store = store;
  }

  public Store getStore() {
    return store;
  }

  @Override
  protected void startUp() throws Exception {
    zkClientService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    zkClientService.stopAndWait();
  }

  private static Injector createInjector() throws Exception {

    CConfiguration cConf = CConfiguration.create();
    if (cConf.getBoolean(Constants.Security.Authorization.ENABLED)) {
      System.out.println(String.format("Disabling authorization for %s.", StoreScanner.class.getSimpleName()));
      cConf.setBoolean(Constants.Security.Authorization.ENABLED, false);
    }
    // Note: login has to happen before any objects that need Kerberos credentials are instantiated.
    SecurityUtil.loginForMasterService(cConf);

    return Guice.createInjector(
      new ConfigModule(cConf, HBaseConfiguration.create()),
      new IOModule(),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new ViewAdminModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NotificationFeedClientModule(),
      new TwillModule(),
      new ExploreClientModule(),
      new DataFabricModules(StoreScanner.class.getName()).getDistributedModules(),
      new ServiceStoreModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new SystemDatasetRuntimeModule().getDistributedModules(),
      new NotificationServiceRuntimeModule().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new MetricsStoreModule(),
      new KafkaClientModule(),
      new NamespaceStoreModule().getDistributedModules(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new SecureStoreModules().getDistributedModules(),
      new MessagingClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);
          bind(Store.class).annotatedWith(Names.named("defaultStore")).to(DefaultStore.class).in(Singleton.class);

          // This is needed because the LocalApplicationManager
          // expects a dsframework injection named datasetMDS
          bind(DatasetFramework.class)
            .annotatedWith(Names.named("datasetMDS"))
            .to(DatasetFramework.class).in(Singleton.class);
        }
      });
  }

  @VisibleForTesting
  static StoreScanner createScanner() throws Exception {
    return createInjector().getInstance(StoreScanner.class);
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options()
      .addOption(new Option("h", "help", false, "Print this usage message."))
      .addOption(new Option("n", "number", true, "Number of run records to generate"))
      .addOption(new Option("d", "delete", false, "delete runs in the store"))
      .addOption(new Option("t", "trace", false, "Trace mode. Prints all of the jobs being debugged."));

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);
    String[] commandArgs = commandLine.getArgs();

    // if help is an option, or if there is a command, print usage and exit.
    if (commandLine.hasOption("h") || commandArgs.length != 0) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp(
        StoreScanner.class.getName(),
        "Scans the JobQueueDataset and prints statistics about the Jobs in it.",
        options, "");
      System.exit(0);
    }

    Integer number = 0;
    if (commandLine.hasOption("n")) {
      String n = commandLine.getOptionValue("n");
      number = Integer.valueOf(n);
    }

    StoreScanner scanner = createScanner();
    scanner.startAndWait();
    Store store = scanner.getStore();
    List<String> namespaces = ImmutableList.of("ns1", "ns2", "ns3", "ns4", "ns5", "ns6");

    if (commandLine.hasOption("d")) {
      for (String ns : namespaces) {
        store.removeAll(new NamespaceId(ns));
      }
    }

    int count = 2 * number;
    for (int i = 0; i < count; i++) {
      if (i == count / 10) {
        System.out.println("==> 10% done");
      } else if (i == count / 5) {
        System.out.println("====> 20% done");
      } else if (i == count / 2) {
        System.out.println("====> 50% done");
      } else if (i == count / 5 * 4) {
        System.out.println("====> 80% done");
      }

      String ns = namespaces.get(i % (2 * namespaces.size()) / 2);
      ProgramRunId runId = new ProgramRunId(ns, "app", ProgramType.WORKFLOW, "program", Integer.toString(i / 2));
      if (i % 2 == 0) {
        store.setProvisioning(runId, i, Collections.emptyMap(), Collections.emptyMap(), Bytes.toBytes(i),
                              new ArtifactId("art", new ArtifactVersion("v1"), ArtifactScope.USER));
      } else {
        store.setStop(runId, i, ProgramRunStatus.KILLED, Bytes.toBytes(i));
      }
    }
    Map<ProgramRunId, RunRecordMeta> runs =
      scanner.getStore().getHistoricalRuns(new HashSet<>(namespaces), number - 10, number, 100);
    System.out.println("Runs: " + runs.values());
    scanner.stopAndWait();
  }
}

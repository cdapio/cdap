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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.schedule.Trigger;
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
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import co.cask.cdap.internal.app.runtime.schedule.queue.Job;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueue;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsStoreModule;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.security.impersonation.SecurityUtil;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Debugging tool for {@link JobQueue}.
 *
 * Because the JobQueue is scanned over multiple transactions, it will be an inconsistent view.
 * The same Job will not be counted multiple times, but some Jobs may be missed if they were deleted or added during
 * the scan. The count of the Job State may also be inconsistent.
 *
 * The publish timestamp of the last message processed from the topics will also be inconsistent from the Jobs in the
 * JobQueue.
 */
public class JobQueueDebugger extends AbstractIdleService {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Trigger.class, new TriggerCodec())
    .registerTypeAdapter(Constraint.class, new ConstraintCodec())
    .create();

  private final ZKClientService zkClientService;
  private final CConfiguration cConf;
  private final Transactional transactional;
  private final MultiThreadDatasetCache multiThreadDatasetCache;
  private final DatasetFramework datasetFramework;

  private JobQueueScanner jobQueueScanner;

  @Inject
  public JobQueueDebugger(CConfiguration cConf, ZKClientService zkClientService,
                          DatasetFramework datasetFramework,
                          TransactionSystemClient txClient) {
    this.cConf = cConf;
    this.zkClientService = zkClientService;

    this.datasetFramework = datasetFramework;
    multiThreadDatasetCache = new MultiThreadDatasetCache(
      new SystemDatasetInstantiator(datasetFramework), txClient,
      NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null);
    transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(multiThreadDatasetCache),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  @Override
  protected void startUp() throws Exception {
    zkClientService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    zkClientService.stopAndWait();
  }

  private JobQueueScanner getJobQueueScanner() {
    if (jobQueueScanner == null) {
      jobQueueScanner = new JobQueueScanner(cConf, transactional,
                                            Schedulers.getJobQueue(multiThreadDatasetCache, datasetFramework));
    }
    return jobQueueScanner;
  }

  private void printTopicMessageIds() throws TransactionFailureException {
    getJobQueueScanner().printTopicMessageIds();
  }

  private void scanPartitions(boolean trace) throws Exception {
    getJobQueueScanner().scanPartitions(trace);
  }

  private JobStatistics scanPartition(int partition, boolean trace) throws TransactionFailureException {
    return getJobQueueScanner().scanPartition(partition, trace);
  }

  /**
   * Scans over the JobQueueDataset and collects statistics about the Jobs.
   */
  private static final class JobQueueScanner {
    private final CConfiguration cConf;
    private final Transactional transactional;
    private final JobQueueDataset jobQueue;
    private final int numPartitions;

    private Job lastJobConsumed;

    JobQueueScanner(CConfiguration cConf, Transactional transactional, JobQueueDataset jobQueue) {
      this.cConf = cConf;
      this.transactional = transactional;
      this.jobQueue = jobQueue;
      this.numPartitions = jobQueue.getNumPartitions();
    }

    private void printTopicMessageIds() throws TransactionFailureException {
      transactional.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          System.out.printf("Getting notification subscriber messageIds.\n");
          List<String> topics = ImmutableList.of(cConf.get(Constants.Scheduler.TIME_EVENT_TOPIC),
                                                 cConf.get(Constants.Dataset.DATA_EVENT_TOPIC));
          for (String topic : topics) {
            String messageIdString = jobQueue.retrieveSubscriberState(topic);
            String publishTimestampString = messageIdString == null ? "n/a" :
              Long.toString(new MessageId(Bytes.fromHexString(messageIdString)).getPublishTimestamp());
            System.out.println(String.format("Topic: %s, Publish Timestamp: %s", topic, publishTimestampString));
          }
        }
      });
    }

    private void scanPartitions(boolean trace) throws Exception {
      final JobStatistics totalStats = new JobStatistics();

      System.out.println("\nScanning JobQueue.");
      for (int partition = 0; partition < numPartitions; partition++) {
        JobStatistics jobStatistics = scanPartition(partition, trace);
        totalStats.aggregate(jobStatistics);
      }

      System.out.printf("\nTotal statistics:\n%s\n", totalStats.getReport());
    }

    private JobStatistics scanPartition(final int partition, boolean trace) throws TransactionFailureException {
      Preconditions.checkArgument(partition >= 0 && partition < numPartitions);
      System.out.printf("Scanning partition id %s.\n", partition);
      final JobStatistics jobStatistics = new JobStatistics(trace);
      boolean moreJobs = true;
      while (moreJobs) {
        moreJobs = Transactions.execute(transactional, new TxCallable<Boolean>() {
          @Override
          public Boolean call(DatasetContext context) throws Exception {
            return scanJobQueue(jobQueue, partition, jobStatistics);
          }
        });
      }
      if (0 == jobStatistics.getTotal()) {
        System.out.println("N/A - empty partition");
      } else {
        System.out.println(jobStatistics.getReport());
      }
      return jobStatistics;
    }

    // returns true if there are more Jobs in the partitions
    private boolean scanJobQueue(JobQueue jobQueue, int partition, JobStatistics jobStatistics) {
      try (CloseableIterator<Job> jobs = jobQueue.getJobs(partition, lastJobConsumed)) {
        Stopwatch stopwatch = new Stopwatch().start();
        while (stopwatch.elapsedMillis() < 1000) {
          if (!jobs.hasNext()) {
            lastJobConsumed = null;
            return false;
          }
          lastJobConsumed = jobs.next();
          jobStatistics.updateWithJob(lastJobConsumed);
        }
        return true;
      }
    }
  }

  /**
   * Statistics about Jobs in the JobQueue.
   */
  private static final class JobStatistics {

    private final boolean trace;

    @Nullable
    private Job oldestJob;
    @Nullable
    private Job newestJob;

    private int pendingTrigger = 0;
    private int pendingConstraint = 0;
    private int pendingLaunch = 0;

    JobStatistics() {
      this(false);
    }

    JobStatistics(boolean trace) {
      this.trace = trace;
    }

    void updateWithJob(Job job) {
      if (trace) {
        System.out.println("Job: " + GSON.toJson(job));
      }

      switch (job.getState()) {
        case PENDING_TRIGGER:
          pendingTrigger++;
          break;
        case PENDING_CONSTRAINT:
          pendingConstraint++;
          break;
        case PENDING_LAUNCH:
          pendingLaunch++;
          break;
      }

      updateOldestNewest(job);
    }

    private void updateOldestNewest(@Nullable Job job) {
      // can be null in the case of calling the aggregate method
      if (job == null) {
        return;
      }
      if (oldestJob == null) {
        oldestJob = job;
      } else {
        oldestJob = job.getCreationTime() > oldestJob.getCreationTime() ? oldestJob : job;
      }
      if (newestJob == null) {
        newestJob = job;
      } else {
        newestJob = job.getCreationTime() < newestJob.getCreationTime() ? newestJob : job;
      }
    }

    private int getTotal() {
      return pendingTrigger + pendingConstraint + pendingLaunch;
    }

    private String getReport() {
      return String.format("Number of Jobs by state:\n" +
                             "  Pending Trigger: %s\n" +
                             "  Pending Constraint: %s\n" +
                             "  Pending Launch: %s\n" +
                             "  Total: %s\n",
                           pendingTrigger, pendingConstraint, pendingLaunch,
                           getTotal());
    }

    // updates this JobQueueStatistics with the results of the JobQueueStatistics passed in
    private void aggregate(JobStatistics jobStatistics) {
      updateOldestNewest(jobStatistics.newestJob);
      updateOldestNewest(jobStatistics.oldestJob);
      pendingTrigger += jobStatistics.pendingTrigger;
      pendingConstraint += jobStatistics.pendingConstraint;
      pendingLaunch += jobStatistics.pendingLaunch;
    }

  }

  private static Injector createInjector() throws Exception {

    CConfiguration cConf = CConfiguration.create();
    if (cConf.getBoolean(Constants.Security.Authorization.ENABLED)) {
      System.out.println(String.format("Disabling authorization for %s.", JobQueueDebugger.class.getSimpleName()));
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
      new DataFabricModules().getDistributedModules(),
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
  static JobQueueDebugger createDebugger() throws Exception {
    return createInjector().getInstance(JobQueueDebugger.class);
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options()
      .addOption(new Option("h", "help", false, "Print this usage message."))
      .addOption(new Option("p", "partition", true, "JobQueue partition to debug. Defaults to all partitions."))
      .addOption(new Option("t", "trace", false, "Trace mode. Prints all of the jobs being debugged."));

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);
    String[] commandArgs = commandLine.getArgs();

    // if help is an option, or if there is a command, print usage and exit.
    if (commandLine.hasOption("h") || commandArgs.length != 0) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp(
        JobQueueDebugger.class.getName(),
        "Scans the JobQueueDataset and prints statistics about the Jobs in it.",
        options, "");
      System.exit(0);
    }

    Integer partition = null;
    if (commandLine.hasOption("p")) {
      String partitionString = commandLine.getOptionValue("p");
      partition = Integer.valueOf(partitionString);
    }

    boolean trace = false;
    if (commandLine.hasOption("t")) {
      trace = true;
    }

    JobQueueDebugger debugger = createDebugger();
    debugger.startAndWait();

    debugger.printTopicMessageIds();

    if (partition == null) {
      debugger.scanPartitions(trace);
    } else {
      debugger.scanPartition(partition, trace);
    }
    debugger.stopAndWait();
  }
}

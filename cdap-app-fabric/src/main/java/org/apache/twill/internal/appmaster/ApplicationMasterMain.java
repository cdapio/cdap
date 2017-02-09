/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal.appmaster;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.ServiceMain;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.logging.Loggings;
import org.apache.twill.internal.yarn.VersionDetectYarnAMClientFactory;
import org.apache.twill.internal.yarn.YarnAMClient;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Main class for launching {@link ApplicationMasterService}.
 * TODO: This is copied from Twill as a temporary workaround for Kafka issue in MapR.
 * Will be removed when TWILL-147 is fixed.
 */
public final class ApplicationMasterMain extends ServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterMain.class);
  private final String kafkaZKConnect;

  private ApplicationMasterMain(String kafkaZKConnect) {
    this.kafkaZKConnect = kafkaZKConnect;
  }

  /**
   * Starts the application master.
   */
  public static void main(String[] args) throws Exception {
    File twillSpec = new File(Constants.Files.TWILL_SPEC);
    TwillRuntimeSpecification twillRuntimeSpec = TwillRuntimeSpecificationAdapter.create().fromJson(twillSpec);
    String zkConnect = twillRuntimeSpec.getZkConnectStr();
    RunId runId = twillRuntimeSpec.getTwillAppRunId();

    ZKClientService zkClientService = createZKClient(zkConnect, twillRuntimeSpec.getTwillAppName());
    Configuration conf = new YarnConfiguration(new HdfsConfiguration(new Configuration()));
    setRMSchedulerAddress(conf, twillRuntimeSpec.getRmSchedulerAddr());

    final YarnAMClient amClient = new VersionDetectYarnAMClientFactory(conf).create();
    ApplicationMasterService service =
      new ApplicationMasterService(runId, zkClientService, twillRuntimeSpec, amClient,
                                   createAppLocation(conf, twillRuntimeSpec.getFsUser(),
                                                     twillRuntimeSpec.getTwillAppDir()));
    TrackerService trackerService = new TrackerService(service);

    List<Service> prerequisites = Lists.newArrayList(
      new YarnAMClientService(amClient, trackerService),
      zkClientService,
      new AppMasterTwillZKPathService(zkClientService, runId)
    );

    // TODO: Temp fix for Kakfa issue in MapR. Will be removed when fixing TWILL-147
    if (Boolean.parseBoolean(System.getProperty("twill.disable.kafka"))) {
      LOG.info("Log collection through kafka disabled");
    } else {
      prerequisites.add(new ApplicationKafkaService(zkClientService, runId));
    }

    new ApplicationMasterMain(String.format("%s/%s/kafka", zkConnect, runId.getId()))
      .doMain(
        service,
        prerequisites.toArray(new Service[prerequisites.size()])
      );
  }

  /**
   * Optionally sets the RM scheduler address based on the environment variable if it is not set in the cluster config.
   */
  private static void setRMSchedulerAddress(Configuration conf, String schedulerAddress) {
    if (schedulerAddress == null) {
      return;
    }

    // If the RM scheduler address is not in the config or it's from yarn-default.xml,
    // replace it with the one from the env, which is the same as the one client connected to.
    String[] sources = conf.getPropertySources(YarnConfiguration.RM_SCHEDULER_ADDRESS);
    if (sources == null || sources.length == 0 || "yarn-default.xml".equals(sources[sources.length - 1])) {
      conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, schedulerAddress);
    }
  }

  @Override
  protected String getHostname() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      return "unknown";
    }
  }

  @Override
  protected String getKafkaZKConnect() {
    return kafkaZKConnect;
  }

  @Override
  protected String getRunnableName() {
    return System.getenv(EnvKeys.TWILL_RUNNABLE_NAME);
  }

  /**
   * A service wrapper for starting/stopping {@link EmbeddedKafkaServer} and make sure the ZK path for
   * Kafka exists before starting the Kafka server.
   */
  private static final class ApplicationKafkaService extends AbstractIdleService {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationKafkaService.class);

    private final ZKClient zkClient;
    private final String kafkaZKPath;
    private final EmbeddedKafkaServer kafkaServer;

    private ApplicationKafkaService(ZKClient zkClient, RunId runId) {
      this.zkClient = zkClient;
      this.kafkaZKPath = "/" + runId.getId() + "/kafka";
      this.kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(zkClient.getConnectString() + kafkaZKPath));
    }

    @Override
    protected void startUp() throws Exception {
      ZKOperations.ignoreError(
        zkClient.create(kafkaZKPath, null, CreateMode.PERSISTENT),
        KeeperException.NodeExistsException.class, kafkaZKPath).get();
      kafkaServer.startAndWait();
    }

    @Override
    protected void shutDown() throws Exception {
      // Flush all logs before shutting down Kafka server
      Loggings.forceFlush();
      // Delay for 2 seconds to give clients chance to poll the last batch of log messages.
      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException e) {
        // Ignore
        LOG.info("Kafka shutdown delay interrupted", e);
      } finally {
        kafkaServer.stopAndWait();
      }
    }

    private Properties generateKafkaConfig(String kafkaZKConnect) {
      Properties prop = new Properties();
      prop.setProperty("log.dir", new File("kafka-logs").getAbsolutePath());
      prop.setProperty("broker.id", "1");
      prop.setProperty("socket.send.buffer.bytes", "1048576");
      prop.setProperty("socket.receive.buffer.bytes", "1048576");
      prop.setProperty("socket.request.max.bytes", "104857600");
      prop.setProperty("num.partitions", "1");
      prop.setProperty("log.retention.hours", "24");
      prop.setProperty("log.flush.interval.messages", "10000");
      prop.setProperty("log.flush.interval.ms", "1000");
      prop.setProperty("log.segment.bytes", "536870912");
      prop.setProperty("zookeeper.connect", kafkaZKConnect);
      // Set the connection timeout to relatively short time (3 seconds).
      // It is only used by the org.I0Itec.zkclient.ZKClient inside KafkaServer
      // to block and wait for ZK connection goes into SyncConnected state.
      // However, due to race condition described in TWILL-139 in the ZK client library used by Kafka,
      // when ZK authentication is enabled, the ZK client may hang until connection timeout.
      // Setting it to lower value allow the AM to retry multiple times if race happens.
      prop.setProperty("zookeeper.connection.timeout.ms", "3000");
      prop.setProperty("default.replication.factor", "1");
      return prop;
    }
  }


  /**
   * A Service wrapper that starts {@link TrackerService} and {@link YarnAMClient}. It is needed because
   * the tracker host and url needs to be provided to {@link YarnAMClient} before it starts {@link YarnAMClient}.
   */
  private static final class YarnAMClientService extends AbstractIdleService {

    private final YarnAMClient yarnAMClient;
    private final TrackerService trackerService;

    private YarnAMClientService(YarnAMClient yarnAMClient, TrackerService trackerService) {
      this.yarnAMClient = yarnAMClient;
      this.trackerService = trackerService;
    }

    @Override
    protected void startUp() throws Exception {
      trackerService.setHost(yarnAMClient.getHost());
      trackerService.startAndWait();

      yarnAMClient.setTracker(trackerService.getBindAddress(), trackerService.getUrl());
      try {
        yarnAMClient.startAndWait();
      } catch (Exception e) {
        trackerService.stopAndWait();
        throw e;
      }
    }

    @Override
    protected void shutDown() throws Exception {
      try {
        yarnAMClient.stopAndWait();
      } finally {
        trackerService.stopAndWait();
      }
    }
  }

  private static final class AppMasterTwillZKPathService extends TwillZKPathService {

    private static final Logger LOG = LoggerFactory.getLogger(AppMasterTwillZKPathService.class);
    private final ZKClient zkClient;

    AppMasterTwillZKPathService(ZKClient zkClient, RunId runId) {
      super(zkClient, runId);
      this.zkClient = zkClient;
    }

    @Override
    protected void shutDown() throws Exception {
      super.shutDown();

      // Deletes ZK nodes created for the application execution.
      // We don't have to worry about a race condition if another instance of the same app starts at the same time
      // as when removal is performed. This is because we always create nodes with "createParent == true",
      // which takes care of the parent node recreation if it is removed from here.

      // Try to delete the /instances path. It may throws NotEmptyException if there are other instances of the
      // same app running, which we can safely ignore and return.
      if (!delete(Constants.INSTANCES_PATH_PREFIX)) {
        return;
      }

      // Try to delete children under /discovery. It may fail with NotEmptyException if there are other instances
      // of the same app running that has discovery services running.
      List<String> children = zkClient.getChildren(Constants.DISCOVERY_PATH_PREFIX)
        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getChildren();
      List<OperationFuture<?>> deleteFutures = new ArrayList<>();
      for (String child : children) {
        String path = Constants.DISCOVERY_PATH_PREFIX + "/" + child;
        LOG.info("Removing ZK path: {}{}", zkClient.getConnectString(), path);
        deleteFutures.add(zkClient.delete(path));
      }
      Futures.successfulAsList(deleteFutures).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      for (OperationFuture<?> future : deleteFutures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          if (e.getCause() instanceof KeeperException.NotEmptyException) {
            return;
          }
          throw e;
        }
      }

      // Delete the /discovery. It may fail with NotEmptyException (due to race between apps),
      // which can safely ignore and return.
      if (!delete(Constants.DISCOVERY_PATH_PREFIX)) {
        return;
      }

      // Delete the ZK path for the app namespace.
      delete("/");
    }

    /**
     * Deletes the given ZK path.
     *
     * @param path path to delete
     * @return true if the path was deleted, false if failed to delete due to {@link KeeperException.NotEmptyException}.
     * @throws Exception if failed to delete the path
     */
    private boolean delete(String path) throws Exception {
      try {
        LOG.info("Removing ZK path: {}{}", zkClient.getConnectString(), path);
        zkClient.delete(path).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return true;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof KeeperException.NotEmptyException) {
          return false;
        }
        throw e;
      }
    }
  }
}

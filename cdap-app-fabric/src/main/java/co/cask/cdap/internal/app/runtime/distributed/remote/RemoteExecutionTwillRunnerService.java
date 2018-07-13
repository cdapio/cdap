/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed.remote;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitorClient;
import co.cask.cdap.internal.profile.ProfileMetricScheduledService;
import co.cask.cdap.internal.provision.LocationBasedSSHKeyPair;
import co.cask.cdap.logging.remote.RemoteExecutionLogProcessor;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.Node;
import co.cask.cdap.runtime.spi.ssh.SSHKeyPair;
import co.cask.cdap.security.tools.KeyStores;
import co.cask.common.http.HttpRequestConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.SingleRunnableApplication;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link TwillRunnerService} implementations that uses ssh and {@link RuntimeMonitor} to launch and monitor
 * {@link TwillApplication} with single {@link TwillRunnable}.
 */
public class RemoteExecutionTwillRunnerService implements TwillRunnerService {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionTwillRunnerService.class);
  // Max out the HTTPS certificate validity.
  // We use max int of seconds to compute number of days to avoid overflow.
  private static final int CERT_VALIDITY_DAYS = (int) TimeUnit.SECONDS.toDays(Integer.MAX_VALUE);
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final DatasetFramework datasetFramework;
  private final Map<ProgramRunId, RemoteExecutionTwillController> controllers;
  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;
  private final RemoteExecutionLogProcessor logProcessor;
  private final MetricsCollectionService metricsCollectionService;

  private LocationCache locationCache;
  private Path cachePath;
  private ScheduledExecutorService monitorScheduler;

  @Inject
  RemoteExecutionTwillRunnerService(CConfiguration cConf, Configuration hConf,
                                    LocationFactory locationFactory, MessagingService messagingService,
                                    DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                    RemoteExecutionLogProcessor logProcessor,
                                    MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, Collections.emptyMap(), null, null, messagingContext)),
      org.apache.tephra.RetryStrategies.retryOnConflict(20, 100)
    );

    this.controllers = new ConcurrentHashMap<>();
    this.logProcessor = logProcessor;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  public void start() {
    try {
      // Use local directory for caching generated jar files
      Path tempDir = Files.createDirectories(Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                       cConf.get(Constants.AppFabric.TEMP_DIR)).toAbsolutePath());
      cachePath = Files.createTempDirectory(tempDir, "runner.cache");
      locationCache = new BasicLocationCache(Locations.toLocation(cachePath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    monitorScheduler = Executors.newScheduledThreadPool(cConf.getInt(Constants.RuntimeMonitor.THREADS),
                                                        Threads.createDaemonThreadFactory("runtime-monitor-%d"));

    // TODO CDAP-13417: Scan AppMetaStore for active run records that were launched with RemoteExecutionTwillRunner
    // to initialize the controllers map.
  }

  @Override
  public void stop() {
    // Stops all the runtime monitor. This won't terminate the remotely running program.
    List<ListenableFuture<Service.State>> stopFutures = new ArrayList<>();
    for (RemoteExecutionTwillController controller : controllers.values()) {
      stopFutures.add(controller.getRuntimeMonitor().stop());
    }

    // Wait for all of them to stop
    try {
      Uninterruptibles.getUninterruptibly(Futures.successfulAsList(stopFutures));
    } catch (Exception e) {
      // This shouldn't happen
      LOG.warn("Exception raised when waiting for runtime monitors to stop.", e);
    }

    try {
      if (cachePath != null) {
        DirUtils.deleteDirectoryContents(cachePath.toFile());
      }
    } catch (IOException e) {
      LOG.warn("Exception raised during stop", e);
    } finally {
      if (monitorScheduler != null) {
        monitorScheduler.shutdownNow();
      }
    }
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return prepare(runnable, ResourceSpecification.BASIC);
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return prepare(new SingleRunnableApplication(runnable, resourceSpecification));
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    Configuration config = new Configuration(hConf);

    // Restrict the usage to launch user program only.
    if (!(application instanceof ProgramTwillApplication)) {
      throw new IllegalArgumentException("Only instance of ProgramTwillApplication is supported");
    }

    ProgramTwillApplication programTwillApp = (ProgramTwillApplication) application;
    ProgramRunId programRunId = programTwillApp.getProgramRunId();
    ProgramOptions programOptions = programTwillApp.getProgramOptions();

    // Get the SSH information provided by the provisioner.
    Arguments systemArgs = programOptions.getArguments();
    Cluster cluster = GSON.fromJson(systemArgs.getOption(ProgramOptionConstants.CLUSTER), Cluster.class);

    Node masterNode = cluster.getNodes().stream()
      .filter(node -> co.cask.cdap.runtime.spi.Constants.Node.MASTER_TYPE.equals(
        node.getProperties().get(co.cask.cdap.runtime.spi.Constants.Node.TYPE)))
      .findFirst().orElseThrow(
      () -> new IllegalArgumentException("Missing master node information for the cluster " + cluster.getName()));

    // Generate the KeyStores for HTTPS
    Location keysDir = locationFactory.create(
      GSON.fromJson(systemArgs.getOption(ProgramOptionConstants.SECURE_KEYS_DIR), URI.class));

    KeyStore serverKeyStore = KeyStores.generatedCertKeyStore(CERT_VALIDITY_DAYS, "");
    KeyStore clientKeyStore = KeyStores.generatedCertKeyStore(CERT_VALIDITY_DAYS, "");

    saveKeyStores(serverKeyStore, clientKeyStore, keysDir);

    // Loads the SSH keys
    SSHKeyPair sshKeyPair = createSSHKeyPair(keysDir, cluster);

    // Creates and return the twill preparer
    String remoteHost = Preconditions.checkNotNull(masterNode.getProperties().get("ip.external"));

    Optional<ProfileId> profile = SystemArguments.getProfileIdFromArgs(programRunId.getNamespaceId(),
                                                                       programOptions.getArguments().asMap());
    ProfileId profileId = profile.orElseThrow(() ->
      new IllegalStateException("Missing profile information for this program run %s " + programRunId));
    ProfileMetricScheduledService metricScheduledService = new ProfileMetricScheduledService(metricsCollectionService,
                                                                                             programRunId,
                                                                                             profileId,
                                                                                             cluster.getNodes().size(),
                                                                                             monitorScheduler);
    RunId runId = RunIds.fromString(programRunId.getRun());
    return new RemoteExecutionTwillPreparer(cConf, config, remoteHost, sshKeyPair,
                                            serverKeyStore, clientKeyStore,
                                            application.configure(), runId,
                                            null, locationCache, locationFactory,
                                            createControllerFactory(programRunId, runId, remoteHost,
                                                                    serverKeyStore, clientKeyStore,
                                                                    metricScheduledService));
  }

  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    return null;
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    return null;
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    return null;
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay,
                                               long delay, TimeUnit unit) {
    // This method is deprecated and not used in CDAP
    throw new UnsupportedOperationException("The scheduleSecureStoreUpdate method is deprecated, " +
                                              "use setSecureStoreRenewer instead");
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay,
                                           long delay, long retryDelay, TimeUnit unit) {
    return null;
  }

  /**
   * Creates a new instance of {@link RemoteExecutionTwillControllerFactory} for a given run.
   */
  private RemoteExecutionTwillControllerFactory createControllerFactory(ProgramRunId programRunId,
                                                                        RunId runId,
                                                                        String remoteHost,
                                                                        KeyStore serverKeyStore,
                                                                        KeyStore clientKeyStore,
                                                                        ProfileMetricScheduledService metricService) {
    return () -> {
      try {
        // Creates a runtime monitor and starts it
        RuntimeMonitorClient runtimeMonitorClient = new RuntimeMonitorClient(
          remoteHost,
          cConf.getInt(Constants.RuntimeMonitor.SERVER_PORT),
          HttpRequestConfig.DEFAULT, clientKeyStore, KeyStores.createTrustStore(serverKeyStore)
        );

        RuntimeMonitor runtimeMonitor = new RuntimeMonitor(programRunId, cConf, runtimeMonitorClient,
                                                           datasetFramework, transactional,
                                                           messagingContext, monitorScheduler, logProcessor,
                                                           metricService);
        RemoteExecutionTwillController controller = new RemoteExecutionTwillController(runId, runtimeMonitor);
        controllers.put(programRunId, controller);
        runtimeMonitor.start();

        // When the program completed, remove the controller from the map
        controller.onTerminated(() -> controllers.remove(programRunId, controller), Threads.SAME_THREAD_EXECUTOR);

        return controller;
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    };
  }

  /**
   * Creates a {@link SSHKeyPair} by loading keys from the given location and {@link Cluster} properties.
   */
  private SSHKeyPair createSSHKeyPair(Location keysDir, Cluster cluster) {
    String sshUser = cluster.getProperties().get(Constants.RuntimeMonitor.SSH_USER);
    if (sshUser == null) {
      throw new IllegalStateException("Missing SSH user");
    }
    try {
      return new LocationBasedSSHKeyPair(keysDir, sshUser);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create SSHKeyPair from location " + keysDir, e);
    }
  }

  /**
   * Saves the given {@link KeyStore} under the given directory.
   */
  private void saveKeyStores(KeyStore serverKeyStore, KeyStore clientKeyStore, Location keysDir) {
    Map<String, KeyStore> keyStores = ImmutableMap.of(
      Constants.RuntimeMonitor.SERVER_KEYSTORE, serverKeyStore,
      Constants.RuntimeMonitor.CLIENT_KEYSTORE, clientKeyStore
    );

    for (Map.Entry<String, KeyStore> entry : keyStores.entrySet()) {
      try (OutputStream os = keysDir.append(entry.getKey()).getOutputStream("600")) {
        entry.getValue().store(os, "".toCharArray());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }
}

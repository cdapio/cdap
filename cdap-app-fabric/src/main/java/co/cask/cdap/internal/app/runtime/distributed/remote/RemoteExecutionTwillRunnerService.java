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
import co.cask.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitorClient;
import co.cask.cdap.internal.provision.SecureKeyInfo;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.Node;
import co.cask.cdap.security.tools.KeyStores;
import co.cask.common.http.HttpRequestConfig;
import com.google.common.base.Throwables;
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
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.SingleRunnableApplication;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final DatasetFramework datasetFramework;
  private final Map<ProgramRunId, RemoteExecutionTwillController> controllers;
  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;
  private LocationCache locationCache;
  private Path cachePath;
  private ScheduledExecutorService monitorScheduler;

  @Inject
  RemoteExecutionTwillRunnerService(CConfiguration cConf, Configuration hConf,
                                    LocationFactory locationFactory, MessagingService messagingService,
                                    DatasetFramework datasetFramework, TransactionSystemClient txClient) {
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
      .filter(node -> "master".equals(node.getProperties().get("type")))
      .findFirst().orElseThrow(
      () -> new IllegalArgumentException("Missing master node information for the cluster " + cluster.getName()));

    if (!systemArgs.hasOption(ProgramOptionConstants.CLUSTER_KEY_INFO)) {
      throw new IllegalStateException("Missing ssh key information for the cluster " + cluster.getName());
    }
    SecureKeyInfo keyInfo = GSON.fromJson(systemArgs.getOption(ProgramOptionConstants.CLUSTER_KEY_INFO),
                                          SecureKeyInfo.class);

    RunId twillRunId = RunIds.generate();
    String remoteHost = masterNode.getProperties().get("ip.external");

    return new RemoteExecutionTwillPreparer(cConf, config, remoteHost, keyInfo,
                                            application.configure(), twillRunId,
                                            null, locationCache, locationFactory,
                                            createControllerFactory(programRunId, keyInfo, twillRunId, remoteHost));
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
                                                                        SecureKeyInfo keyInfo,
                                                                        RunId twillRunId,
                                                                        String remoteHost) {
    return () -> {
      try {
        KeyStore clientKeyStore = KeyStores.load(locationFactory.create(keyInfo.getKeyDirectory())
                                                   .append(keyInfo.getClientKeyStoreFile()),
                                                 () -> "");
        KeyStore serverKeyStore = KeyStores.load(locationFactory.create(keyInfo.getKeyDirectory())
                                                   .append(keyInfo.getServerKeyStoreFile()),
                                                 () -> "");

        // Creates a runtime monitor and starts it
        RuntimeMonitorClient runtimeMonitorClient = new RuntimeMonitorClient(
          remoteHost,
          cConf.getInt(Constants.RuntimeMonitor.SERVER_PORT),
          HttpRequestConfig.DEFAULT, clientKeyStore, KeyStores.createTrustStore(serverKeyStore)
        );
        RuntimeMonitor runtimeMonitor = new RuntimeMonitor(programRunId, cConf, runtimeMonitorClient,
                                                           datasetFramework, transactional,
                                                           messagingContext, monitorScheduler);
        RemoteExecutionTwillController controller = new RemoteExecutionTwillController(twillRunId, runtimeMonitor);
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
}

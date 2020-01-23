/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.launcher;

import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.launcher.Launcher;
import org.apache.hadoop.conf.Configuration;
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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 *
 */
public class LauncherTwillRunnerService implements TwillRunnerService {
  private static final Logger LOG = LoggerFactory.getLogger(LauncherTwillRunnerService.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final Map<ProgramRunId, LauncherTwillConrtoller> controllers;
  private final Lock controllersLock;
  private final MultiThreadMessagingContext messagingContext;
  private final RemoteExecutionLogProcessor logProcessor;
  private final MetricsCollectionService metricsCollectionService;
  private final Launcher launcher;
  private LocationCache locationCache;
  private Path cachePath;

  @Inject
  LauncherTwillRunnerService(CConfiguration cConf, Configuration hConf,
                             LocationFactory locationFactory, MessagingService messagingService,
                             RemoteExecutionLogProcessor logProcessor,
                             MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.controllers = new ConcurrentHashMap<>();
    this.controllersLock = new ReentrantLock();
    this.logProcessor = logProcessor;
    this.metricsCollectionService = metricsCollectionService;
    this.launcher = new LauncherExtensionLoader(cConf.get("launcher.extensions.dir")).get("dataproc-launcher");
    if (this.launcher == null) {
      LOG.info("Test Launcher is null");
    }
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
  }

  @Override
  public void stop() {
    // TODO remove all the resources
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
    LOG.info("Preparing test launcher with twill application");
    Configuration config = new Configuration(hConf);
    // Restrict the usage to launch user program only.
    if (!(application instanceof ProgramTwillApplication)) {
      throw new IllegalArgumentException("Only instance of ProgramTwillApplication is supported");
    }

    ProgramTwillApplication programTwillApp = (ProgramTwillApplication) application;
    ProgramRunId programRunId = programTwillApp.getProgramRunId();
    ProgramOptions programOptions = programTwillApp.getProgramOptions();
    LOG.info("Creating test launcher preparer with twill application");
    return new LauncherTwillPreparer(cConf, config,
                                     application.configure(), programRunId, programOptions, null,
                                     locationCache, locationFactory, launcher);
  }

  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    ProgramId programId = TwillAppNames.fromTwillAppName(applicationName, false);
    if (programId == null) {
      return null;
    }
    return controllers.get(programId.run(runId));
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    ProgramId programId = TwillAppNames.fromTwillAppName(applicationName, false);
    if (programId == null) {
      return Collections.emptyList();
    }

    return controllers.entrySet().stream()
      .filter(entry -> programId.equals(entry.getKey().getParent()))
      .map(Map.Entry::getValue)
      .map(TwillController.class::cast)
      ::iterator;
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    // Groups the controllers by the ProgramId, then transform it to an Iterator of LiveInfo
    return controllers.entrySet().stream()
      .collect(Collectors.groupingBy(e -> e.getKey().getParent(),
                                     Collectors.mapping(e -> TwillController.class.cast(e.getValue()),
                                                        Collectors.toList())))
      .entrySet().stream()
      .map(e -> new LiveInfo() {
        @Override
        public String getApplicationName() {
          return TwillAppNames.toTwillAppName(e.getKey());
        }

        @Override
        public Iterable<TwillController> getControllers() {
          return e.getValue();
        }
      })
      .map(LiveInfo.class::cast)
      ::iterator;
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay, long delay,
                                               TimeUnit unit) {
    // This method is deprecated and not used in CDAP
    throw new UnsupportedOperationException("The scheduleSecureStoreUpdate method is deprecated, " +
                                              "use setSecureStoreRenewer instead");
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay, long delay, long retryDelay,
                                           TimeUnit unit) {
    return () -> { };
  }
}

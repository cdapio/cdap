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

import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.ssh.SSHConfig;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import co.cask.cdap.internal.provision.SSHKeyInfo;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.Node;
import com.google.common.io.ByteStreams;
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
import org.apache.twill.filesystem.Location;
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
  private final MessagingService messagingService;
  private final DatasetFramework dsFramework;
  private final TransactionSystemClient txClient;
  private LocationCache locationCache;
  private Path cachePath;

  @Inject
  RemoteExecutionTwillRunnerService(CConfiguration cConf, Configuration hConf,
                                    LocationFactory locationFactory, MessagingService messagingService,
                                    DatasetFramework dsFramework, TransactionSystemClient txClient) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.messagingService = messagingService;
    this.dsFramework = dsFramework;
    this.txClient = txClient;
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
    try {
      if (cachePath != null) {
        DirUtils.deleteDirectoryContents(cachePath.toFile());
      }
    } catch (IOException e) {
      LOG.warn("Exception raised during stop", e);
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

    try {
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
      SSHKeyInfo keyInfo = GSON.fromJson(systemArgs.getOption(ProgramOptionConstants.CLUSTER_KEY_INFO),
                                         SSHKeyInfo.class);

      Location privateKeyLocation = locationFactory.create(keyInfo.getKeyDirectory())
                                                   .append(keyInfo.getPrivateKeyFile());
      byte[] privateKey = ByteStreams.toByteArray(Locations.newInputSupplier(privateKeyLocation));

      SSHConfig sshConfig = SSHConfig.builder(masterNode.getProperties().get("ip.external"))
        .setUser(keyInfo.getUsername())
        .setPrivateKey(privateKey).build();

      return new RemoteExecutionTwillPreparer(cConf, config, sshConfig, programRunId,
                                              application.configure(), RunIds.generate(),
                                              null, locationCache, locationFactory, messagingService,
                                              dsFramework, txClient);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
}

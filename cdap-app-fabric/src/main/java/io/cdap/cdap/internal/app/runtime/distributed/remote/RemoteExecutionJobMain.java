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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.app.guice.TwillModule;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.logging.common.UncaughtExceptionHandler;
import io.cdap.cdap.internal.app.runtime.distributed.runtimejob.DefaultRuntimeJob;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobEnvironment;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The main class to setup the {@link RuntimeJobEnvironment} for remote execution.
 */
public class RemoteExecutionJobMain {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionJobMain.class);

  private final DefaultRuntimeJob runtimeJob = new DefaultRuntimeJob();

  private InMemoryZKServer zkServer;
  private TwillRunnerService twillRunnerService;
  private LocationFactory locationFactory;

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());
    new RemoteExecutionJobMain().doMain(args);
  }

  /**
   * The main method for the Application. It expects the first argument contains the program run id.
   *
   * @param args arguments provided to the application
   * @throws Exception if failed to the the job
   */
  private void doMain(String[] args) throws Exception {
    if (args.length < 1) {
      throw new IllegalArgumentException("Missing runId from the first argument");
    }

    // Stop the job when this process get terminated
    Runtime.getRuntime().addShutdownHook(new Thread(runtimeJob::requestStop));

    System.setProperty(Constants.Zookeeper.TWILL_ZK_SERVER_LOCALHOST, "false");
    RunId runId = RunIds.fromString(args[0]);
    CConfiguration cConf = CConfiguration.create();

    // Namespace the HDFS on the current cluster to segregate multiple runs on the same cluster
    cConf.set(Constants.CFG_HDFS_NAMESPACE, "/twill-" + runId);

    try {
      RemoteExecutionRuntimeJobEnvironment jobEnv = initialize(cConf);
      runtimeJob.run(jobEnv);
    } finally {
      destroy();
    }
  }

  @VisibleForTesting
  RemoteExecutionRuntimeJobEnvironment initialize(CConfiguration cConf) throws Exception {
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    InetSocketAddress zkAddr = ResolvingDiscoverable.resolve(zkServer.getLocalAddress());
    String zkConnectStr = String.format("%s:%d", zkAddr.getHostString(), zkAddr.getPort());

    LOG.debug("In memory ZK started at {}", zkConnectStr);

    cConf.set(Constants.Zookeeper.QUORUM, zkConnectStr);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new DFSLocationModule(),
      new InMemoryDiscoveryModule(),
      new TwillModule(),
      new AuthenticationContextModules().getProgramContainerModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // don't need to perform any impersonation from within user programs
          bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);

          // Binds a no-op SecureStore for the TwillModule to setup TokenSecureStoreRenewer.
          bind(SecureStore.class).toInstance(new SecureStore() {
            @Override
            public List<SecureStoreMetadata> list(String namespace) {
              return Collections.emptyList();
            }

            @Override
            public SecureStoreData get(String namespace, String name) throws Exception {
              throw new NotFoundException("Secure key " + name + " not found in namespace " + namespace);
            }
          });
        }
      }
    );

    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.Zookeeper.QUORUM, zkConnectStr);

    locationFactory = injector.getInstance(LocationFactory.class);
    locationFactory.create("/").mkdirs();

    twillRunnerService = injector.getInstance(TwillRunnerService.class);
    twillRunnerService.start();

    return new RemoteExecutionRuntimeJobEnvironment(locationFactory, twillRunnerService, properties);
  }

  @VisibleForTesting
  void destroy() {
    if (twillRunnerService != null) {
      try {
        twillRunnerService.stop();
      } catch (Exception e) {
        LOG.warn("Failed to stop twill runner", e);
      }
    }

    if (locationFactory != null) {
      Location location = locationFactory.create("/");
      try {
        location.delete(true);
      } catch (IOException e) {
        LOG.warn("Failed to delete location {}", location, e);
      }
    }

    if (zkServer != null) {
      try {
        zkServer.stopAndWait();
      } catch (Exception e) {
        LOG.warn("Failed to stop ZK server", e);
      }
    }
  }

  private static final class RemoteExecutionRuntimeJobEnvironment implements RuntimeJobEnvironment {

    private final LocationFactory locationFactory;
    private final TwillRunner twillRunner;
    private final Map<String, String> properties;

    private RemoteExecutionRuntimeJobEnvironment(LocationFactory locationFactory, TwillRunner twillRunner,
                                                 Map<String, String> properties) {
      this.locationFactory = locationFactory;
      this.twillRunner = twillRunner;
      this.properties = Collections.unmodifiableMap(properties);
    }

    @Override
    public LocationFactory getLocationFactory() {
      return locationFactory;
    }

    @Override
    public TwillRunner getTwillRunner() {
      return twillRunner;
    }

    @Override
    public Map<String, String> getProperties() {
      return properties;
    }
  }
}

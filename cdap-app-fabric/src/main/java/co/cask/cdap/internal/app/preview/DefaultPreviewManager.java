/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.preview;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.preview.PreviewManager;
import co.cask.cdap.app.preview.PreviewRequest;
import co.cask.cdap.app.preview.PreviewRunner;
import co.cask.cdap.app.preview.PreviewRunnerModule;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.preview.PreviewDiscoveryRuntimeModule;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.config.guice.ConfigStoreModule;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.preview.PreviewDataModules;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactStore;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.cdap.security.guice.preview.PreviewSecureStoreModule;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Class responsible for creating the injector for preview and starting it.
 */
public class DefaultPreviewManager implements PreviewManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultPreviewManager.class);
  private static final String PREFIX = "preview-";

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final DiscoveryService discoveryService;
  private final DatasetFramework datasetFramework;
  private final PreferencesStore preferencesStore;
  private final SecureStore secureStore;
  private final TransactionManager transactionManager;
  private final ArtifactRepository artifactRepository;
  private final ArtifactStore artifactStore;
  private final AuthorizerInstantiator authorizerInstantiator;
  private final StreamAdmin streamAdmin;
  private final StreamCoordinatorClient streamCoordinatorClient;
  private final PrivilegesManager privilegesManager;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final Cache<ApplicationId, Injector> appInjectors;

  @Inject
  DefaultPreviewManager(final CConfiguration cConf, Configuration hConf, DiscoveryService discoveryService,
                        @Named(DataSetsModules.BASE_DATASET_FRAMEWORK) DatasetFramework datasetFramework,
                        PreferencesStore preferencesStore, SecureStore secureStore,
                        TransactionManager transactionManager, ArtifactRepository artifactRepository,
                        ArtifactStore artifactStore, AuthorizerInstantiator authorizerInstantiator,
                        StreamAdmin streamAdmin, StreamCoordinatorClient streamCoordinatorClient,
                        PrivilegesManager privilegesManager, AuthorizationEnforcer authorizationEnforcer) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.datasetFramework = datasetFramework;
    this.discoveryService = discoveryService;
    this.preferencesStore = preferencesStore;
    this.secureStore = secureStore;
    this.transactionManager = transactionManager;
    this.artifactRepository = artifactRepository;
    this.artifactStore = artifactStore;
    this.authorizerInstantiator = authorizerInstantiator;
    this.streamAdmin = streamAdmin;
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.privilegesManager = privilegesManager;
    this.authorizationEnforcer = authorizationEnforcer;

    this.appInjectors = CacheBuilder.newBuilder()
      .maximumSize(cConf.getInt(Constants.Preview.PREVIEW_CACHE_SIZE, 10))
      .removalListener(new RemovalListener<ApplicationId, Injector>() {
        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(RemovalNotification<ApplicationId, Injector> notification) {
          Injector injector = notification.getValue();
          if (injector != null) {
            PreviewRunner runner = injector.getInstance(PreviewRunner.class);
            if (runner instanceof Service) {
              stopQuietly((Service) runner);
            }
          }
          ApplicationId application = notification.getKey();
          if (application == null) {
            return;
          }
          removePreviewDir(application);
        }
      })
      .build();
  }

  @Override
  public ApplicationId start(NamespaceId namespace, AppRequest<?> appRequest) throws Exception {
    Set<String> realDatasets = appRequest.getPreview() == null ? new HashSet<String>()
      : appRequest.getPreview().getRealDatasets();

    ApplicationId previewApp = namespace.app(PREFIX + System.currentTimeMillis());
    Injector injector = createPreviewInjector(previewApp, realDatasets);
    PreviewRunner runner = injector.getInstance(PreviewRunner.class);
    if (runner instanceof Service) {
      ((Service) runner).startAndWait();
    }
    try {
      runner.startPreview(new PreviewRequest<>(getProgramIdFromRequest(previewApp, appRequest), appRequest));
    } catch (Exception e) {
      if (runner instanceof Service) {
        stopQuietly((Service) runner);
      }
      removePreviewDir(previewApp);
      throw e;
    }
    appInjectors.put(previewApp, injector);
    return previewApp;
  }

  @Override
  public PreviewRunner getRunner(ApplicationId preview) throws NotFoundException {
    Injector injector = appInjectors.getIfPresent(preview);
    if (injector == null) {
      throw new NotFoundException(preview);
    }

    return injector.getInstance(PreviewRunner.class);
  }

  /**
   * Create injector for the given application id.
   */
  @VisibleForTesting
  Injector createPreviewInjector(ApplicationId applicationId, Set<String> datasetNames) throws IOException {
    CConfiguration previewcConf = CConfiguration.copy(cConf);
    java.nio.file.Path previewDirPath = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR), "preview").toAbsolutePath();

    Files.createDirectories(previewDirPath);
    java.nio.file.Path previewDir = Files.createDirectories(Paths.get(previewDirPath.toAbsolutePath().toString(),
                                                                      applicationId.getApplication()));
    previewcConf.set(Constants.CFG_LOCAL_DATA_DIR, previewDir.toString());
    Configuration previewhConf = new Configuration(hConf);
    previewhConf.set(Constants.CFG_LOCAL_DATA_DIR, previewDir.toString());
    previewcConf.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, previewDir.toString());
    previewcConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, false);

    return Guice.createInjector(
      new ConfigModule(previewcConf, previewhConf),
      new IOModule(),
      new AuthenticationContextModules().getMasterModule(),
      new SecurityModules().getStandaloneModules(),
      new PreviewSecureStoreModule(secureStore),
      new PreviewDiscoveryRuntimeModule(discoveryService),
      new LocationRuntimeModule().getStandaloneModules(),
      new ConfigStoreModule().getStandaloneModule(),
      new PreviewRunnerModule(artifactRepository, artifactStore, authorizerInstantiator, authorizationEnforcer,
                              privilegesManager, streamAdmin, streamCoordinatorClient, preferencesStore),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new PreviewDataModules().getDataFabricModule(transactionManager),
      new PreviewDataModules().getDataSetsModule(datasetFramework, datasetNames),
      new DataSetServiceModules().getStandaloneModules(),
      new MetricsClientRuntimeModule().getStandaloneModules(),
      new LoggingModules().getStandaloneModules(),
      new NamespaceStoreModule().getStandaloneModules(),
      new MessagingClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
        }

        @Provides
        @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS)
        @SuppressWarnings("unused")
        public InetAddress providesHostname(CConfiguration cConf) {
          String address = cConf.get(Constants.Preview.ADDRESS);
          return Networks.resolve(address, new InetSocketAddress("localhost", 0).getAddress());
        }
      }
    );
  }

  private ProgramId getProgramIdFromRequest(ApplicationId preview, AppRequest request) throws BadRequestException {
    if (request.getPreview() == null) {
      throw new BadRequestException("Preview config cannot be null");
    }

    String programName = request.getPreview().getProgramName();
    ProgramType programType = request.getPreview().getProgramType();

    if (programName == null || programType == null) {
      throw new IllegalArgumentException("ProgramName or ProgramType cannot be null.");
    }

    return preview.program(programType, programName);
  }

  private void stopQuietly(Service service) {
    try {
      service.stopAndWait();
    } catch (Exception e) {
      LOG.debug("Error stopping the preview runner.", e);
    }
  }

  private void removePreviewDir(ApplicationId application) {
    java.nio.file.Path previewDirPath = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                  "preview", application.getApplication()).toAbsolutePath();

    try {
      DataTracerFactoryProvider.removeDataTracerFactory(application);
      DirUtils.deleteDirectoryContents(previewDirPath.toFile());
    } catch (IOException e) {
      LOG.debug("Error deleting the preview directory {}", previewDirPath, e);
    }
  }
}

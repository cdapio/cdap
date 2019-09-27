/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewRunnerModule;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.preview.PreviewDiscoveryRuntimeModule;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.config.guice.ConfigStoreModule;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.preview.PreviewDataModules;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.logging.guice.LocalLogAppenderModule;
import io.cdap.cdap.logging.read.FileLogReader;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizerInstantiator;
import io.cdap.cdap.security.guice.preview.PreviewSecureStoreModule;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.PrivilegesManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.StreamSupport;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Class responsible for creating the injector for preview and starting it.
 */
public class DefaultPreviewManager extends AbstractIdleService implements PreviewManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultPreviewManager.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final SConfiguration sConf;
  private final DiscoveryService discoveryService;
  private final DatasetFramework datasetFramework;
  private final PreferencesService preferencesService;
  private final SecureStore secureStore;
  private final TransactionSystemClient transactionSystemClient;
  private final ArtifactRepository artifactRepository;
  private final ArtifactStore artifactStore;
  private final AuthorizerInstantiator authorizerInstantiator;
  private final PrivilegesManager privilegesManager;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final Cache<ApplicationId, Injector> appInjectors;
  private final Path previewDataDir;
  private final ProgramRuntimeProviderLoader programRuntimeProviderLoader;

  @Inject
  DefaultPreviewManager(CConfiguration cConf, Configuration hConf,
                        SConfiguration sConf, DiscoveryService discoveryService,
                        @Named(DataSetsModules.BASE_DATASET_FRAMEWORK) DatasetFramework datasetFramework,
                        PreferencesService preferencesService, SecureStore secureStore,
                        TransactionSystemClient transactionSystemClient, ArtifactRepository artifactRepository,
                        ArtifactStore artifactStore, AuthorizerInstantiator authorizerInstantiator,
                        PrivilegesManager privilegesManager, AuthorizationEnforcer authorizationEnforcer,
                        ProgramRuntimeProviderLoader programRuntimeProviderLoader) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.sConf = sConf;
    this.datasetFramework = datasetFramework;
    this.discoveryService = discoveryService;
    this.preferencesService = preferencesService;
    this.secureStore = secureStore;
    this.transactionSystemClient = transactionSystemClient;
    this.artifactRepository = artifactRepository;
    this.artifactStore = artifactStore;
    this.authorizerInstantiator = authorizerInstantiator;
    this.privilegesManager = privilegesManager;
    this.authorizationEnforcer = authorizationEnforcer;
    this.previewDataDir = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR), "preview").toAbsolutePath();
    this.programRuntimeProviderLoader = programRuntimeProviderLoader;

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
            removePreviewDir(injector.getInstance(Key.get(ProgramId.class,
                                                          Names.named(PreviewRunnerModule.PREVIEW_PROGRAM_ID))));
          }
        }
      })
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    File previewDir = previewDataDir.toFile();

    // there should be at most 10 directories so the process should not take very long
    for (File file : DirUtils.listFiles(previewDir, File::isDirectory)) {
      ProgramId programId;
      String name = file.getName();
      try {
        String[] parts = name.split("\\.");
        programId = new ProgramId(parts[0], parts[1], parts[2], parts[3]);
      } catch (Exception e) {
        // if there is an exception converting to a preview id, just continue
        LOG.debug("Failed to parse the file directory {} to a valid preview id", name, e);
        continue;
      }
      Injector injector = createPreviewInjector(programId);
      PreviewRunner runner = injector.getInstance(PreviewRunner.class);
      if (runner instanceof Service) {
        ((Service) runner).startAndWait();
      }
      appInjectors.put(programId.getParent(), injector);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    appInjectors.asMap().forEach((applicationId, injector) -> {
      PreviewRunner runner = injector.getInstance(PreviewRunner.class);
      if (runner instanceof Service) {
        stopQuietly((Service) runner);
      }
    });
  }

  @Override
  public ApplicationId start(NamespaceId namespace, AppRequest<?> appRequest) throws Exception {
    // make sure preview id is unique for each run
    ApplicationId previewApp = namespace.app(RunIds.generate().getId());
    ProgramId programId = getProgramIdFromRequest(previewApp, appRequest);
    Injector injector = createPreviewInjector(programId);
    PreviewRunner runner = injector.getInstance(PreviewRunner.class);
    if (runner instanceof Service) {
      ((Service) runner).startAndWait();
    }
    try {
      runner.startPreview(new PreviewRequest<>(programId, appRequest));
    } catch (Exception e) {
      if (runner instanceof Service) {
        stopQuietly((Service) runner);
      }
      removePreviewDir(programId);
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

  @Override
  public LogReader getLogReader(ApplicationId preview) throws NotFoundException {
    Injector injector = appInjectors.getIfPresent(preview);
    if (injector == null) {
      throw new NotFoundException(preview);
    }

    return injector.getInstance(LogReader.class);
  }

  /**
   * Create injector for the given application id.
   */
  @VisibleForTesting
  Injector createPreviewInjector(ProgramId programId) throws IOException {
    CConfiguration previewCConf = CConfiguration.copy(cConf);

    // Change all services bind address to local host
    String localhost = InetAddress.getLoopbackAddress().getHostName();
    StreamSupport.stream(previewCConf.spliterator(), false)
      .map(Map.Entry::getKey)
      .filter(s -> s.endsWith(".bind.address"))
      .forEach(key -> previewCConf.set(key, localhost));

    Path previewDir = Files.createDirectories(getPreviewDirPath(programId));

    previewCConf.set(Constants.CFG_LOCAL_DATA_DIR, previewDir.toString());
    previewCConf.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, previewDir.toString());
    previewCConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, false);
    // Use No-SQL store for preview data
    previewCConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);

    // Setup Hadoop configuration
    Configuration previewHConf = new Configuration(hConf);
    previewHConf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    previewHConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
                     previewDir.resolve("fs").toUri().toString());

    SConfiguration previewSConf = SConfiguration.copy(sConf);

    return Guice.createInjector(
      new ConfigModule(previewCConf, previewHConf, previewSConf),
      new IOModule(),
      new AuthenticationContextModules().getMasterModule(),
      new PreviewSecureStoreModule(secureStore),
      new PreviewDiscoveryRuntimeModule(discoveryService),
      new LocalLocationModule(),
      new ConfigStoreModule(),
      new PreviewRunnerModule(artifactRepository, artifactStore, authorizerInstantiator, authorizationEnforcer,
                              privilegesManager, preferencesService, programRuntimeProviderLoader, programId),
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new PreviewDataModules().getDataFabricModule(transactionSystemClient),
      new PreviewDataModules().getDataSetsModule(datasetFramework),
      new DataSetServiceModules().getStandaloneModules(),
      // Use the in-memory module for metrics collection, which metrics still get persisted to dataset, but
      // save threads for reading metrics from TMS, as there won't be metrics in TMS.
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new LocalLogAppenderModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      Modules.override(new MetadataReaderWriterModules().getInMemoryModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          // we don't start a metadata service in preview, so don't attempt to create any metadata
          bind(MetadataServiceClient.class).to(NoOpMetadataServiceClient.class);
        }
      }),
      new ProvisionerModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ArtifactRepository.class)
            .annotatedWith(Names.named(AppFabricServiceRuntimeModule.NOAUTH_ARTIFACT_REPO))
            .to(DefaultArtifactRepository.class)
            .in(Scopes.SINGLETON);
          bind(LogReader.class).to(FileLogReader.class).in(Scopes.SINGLETON);
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

  @VisibleForTesting
  Cache<ApplicationId, Injector> getCache() {
    return appInjectors;
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

  private void removePreviewDir(ProgramId programId) {
    Path previewDirPath = getPreviewDirPath(programId);
    try {
      DataTracerFactoryProvider.removeDataTracerFactory(programId.getParent());
      DirUtils.deleteDirectoryContents(previewDirPath.toFile());
    } catch (IOException e) {
      LOG.debug("Error deleting the preview directory {}", previewDirPath, e);
    }
  }

  private Path getPreviewDirPath(ProgramId programId) {
    // the preview directory will be <namespace-name>.<app-id>.<program-type>.<program-name>
    return previewDataDir.resolve(String.format("%s.%s.%s.%s", programId.getNamespace(), programId.getApplication(),
                                                programId.getType().name(), programId.getProgram()));
  }
}

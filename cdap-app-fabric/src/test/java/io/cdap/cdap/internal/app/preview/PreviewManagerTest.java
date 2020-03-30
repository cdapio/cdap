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

package io.cdap.cdap.internal.app.preview;

import com.google.common.util.concurrent.Service;
import com.google.gson.JsonElement;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.preview.DefaultPreviewRunnerModule;
import io.cdap.cdap.app.preview.PreviewHttpModule;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewRunnerModule;
import io.cdap.cdap.app.preview.PreviewRunnerModuleFactory;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.config.guice.ConfigStoreModule;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReaderProvider;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinderProvider;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.logging.guice.LocalLogAppenderModule;
import io.cdap.cdap.logging.guice.LogReaderRuntimeModules;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metadata.MetadataServiceModule;
import io.cdap.cdap.metadata.PreferencesFetcherProvider;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.metrics.query.MetricsQueryHelper;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizerInstantiator;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.PrivilegesManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit test for {@link PreviewManager}.
 */
public class PreviewManagerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final AtomicReference<PreviewStatus> PREVIEW_STATUS = new AtomicReference<>();

  private TransactionManager txManager;
  private PreviewManager previewManager;

  @Before
  public void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.Preview.PREVIEW_CACHE_SIZE, 2);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, new Configuration()),
      new IOModule(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getStandaloneModules(),
      new TransactionExecutorModule(),
      new DataSetServiceModules().getInMemoryModules(),
      new InMemoryDiscoveryModule(),
      new AppFabricServiceRuntimeModule().getInMemoryModules(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new NonCustomLocationUnitTestModule(),
      new LocalLogAppenderModule(),
      new LogReaderRuntimeModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new ConfigStoreModule(),
      new MetadataServiceModule(),
      new MetadataReaderWriterModules().getInMemoryModules(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getStandaloneModules(),
      new SecureStoreServerModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new PreviewHttpModule() {
        @Override
        protected void bindPreviewRunnerFactory(Binder binder) {
          binder.install(
            new FactoryModuleBuilder()
              .implement(PreviewRunnerModule.class, MockPreviewRunnerModule.class)
              .build(PreviewRunnerModuleFactory.class)
          );
        }
      },
      new ProvisionerModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      }
    );
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    previewManager = injector.getInstance(PreviewManager.class);
    if (previewManager instanceof Service) {
      ((Service) previewManager).startAndWait();
    }

    PREVIEW_STATUS.set(null);
  }

  @After
  public void finish() {
    if (previewManager instanceof Service) {
      ((Service) previewManager).stopAndWait();
    }
    txManager.stopAndWait();
  }

  @Test(expected = IllegalStateException.class)
  public void testLimit() throws Exception {
    PreviewConfig previewConfig = new PreviewConfig("test", ProgramType.WORKFLOW, null, null);
    previewManager.start(NamespaceId.DEFAULT, new AppRequest<>(new ArtifactSummary("test", "1.0"), null,
                                                               previewConfig));
    previewManager.start(NamespaceId.DEFAULT, new AppRequest<>(new ArtifactSummary("test", "1.0"), null,
                                                               previewConfig));

    // We allow 2 concurrent previews, so running the third one would fail
    previewManager.start(NamespaceId.DEFAULT, new AppRequest<>(new ArtifactSummary("test", "1.0"), null,
                                                               previewConfig));
  }

  @Test
  public void testReclaim() throws Exception {
    PreviewConfig previewConfig = new PreviewConfig("test", ProgramType.WORKFLOW, null, null);
    previewManager.start(NamespaceId.DEFAULT, new AppRequest<>(new ArtifactSummary("test", "1.0"), null,
                                                               previewConfig));
    previewManager.start(NamespaceId.DEFAULT, new AppRequest<>(new ArtifactSummary("test", "1.0"), null,
                                                               previewConfig));

    // Artificially completed the preview status
    PREVIEW_STATUS.set(new PreviewStatus(PreviewStatus.Status.COMPLETED, null,
                                         System.currentTimeMillis(), System.currentTimeMillis()));

    // Starting a new one should succeed
    previewManager.start(NamespaceId.DEFAULT, new AppRequest<>(new ArtifactSummary("test", "1.0"), null,
                                                               previewConfig));
  }

  /**
   * Mocking the {@link Module} to provide a binding to the {@link MockPreviewRunner}.
   */
  private static final class MockPreviewRunnerModule extends DefaultPreviewRunnerModule {

    @Inject
    MockPreviewRunnerModule(ArtifactRepositoryReaderProvider readerProvider, ArtifactStore artifactStore,
                            AuthorizerInstantiator authorizerInstantiator, AuthorizationEnforcer authorizationEnforcer,
                            PrivilegesManager privilegesManager, PreferencesService preferencesService,
                            ProgramRuntimeProviderLoader programRuntimeProviderLoader,
                            PluginFinderProvider pluginFinderProvider,
                            PreferencesFetcherProvider preferencesFetcherProvider,
                            @Assisted PreviewRequest previewRequest) {
      super(readerProvider, artifactStore, authorizerInstantiator, authorizationEnforcer,
            privilegesManager, preferencesService, programRuntimeProviderLoader, pluginFinderProvider,
            preferencesFetcherProvider, previewRequest);
    }

    @Override
    protected void bindPreviewRunner(Binder binder) {
      binder.bind(PreviewRunner.class).to(MockPreviewRunner.class);
    }
  }

  /**
   * Mocking the {@link PreviewRunner}, which doesn't actually run anything.
   */
  private static final class MockPreviewRunner implements PreviewRunner {

    private final PreviewRequest previewRequest;

    @Inject
    MockPreviewRunner(PreviewRequest previewRequest) {
      this.previewRequest = previewRequest;
    }


    @Override
    public PreviewRequest getPreviewRequest() {
      return previewRequest;
    }

    @Override
    public void startPreview() {
      // no-op
    }

    @Override
    public PreviewStatus getStatus() {
      return PREVIEW_STATUS.get();
    }

    @Override
    public void stopPreview() {
      // no-op
    }

    @Override
    public Set<String> getTracers() {
      return null;
    }

    @Override
    public Map<String, List<JsonElement>> getData(String tracerName) {
      return null;
    }

    @Override
    public ProgramRunId getProgramRunId() {
      return null;
    }

    @Override
    public RunRecordDetail getRunRecord() {
      return null;
    }

    @Override
    public MetricsQueryHelper getMetricsQueryHelper() {
      return null;
    }
  }
}

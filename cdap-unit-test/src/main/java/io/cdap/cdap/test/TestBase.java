/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.test;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.messaging.MessagingAdmin;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.MonitorHandlerModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.preview.PreviewHttpModule;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.test.TestRunner;
import io.cdap.cdap.common.twill.NoopTwillRunnerService;
import io.cdap.cdap.common.utils.OSDetector;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.explore.executor.ExploreExecutorService;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.explore.guice.ExploreRuntimeModule;
import io.cdap.cdap.gateway.handlers.AuthorizationHandler;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.internal.app.services.ProgramNotificationSubscriberService;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.provision.MockProvisionerModule;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.logging.guice.LocalLogAppenderModule;
import io.cdap.cdap.logging.guice.LogReaderRuntimeModules;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.BasicMessagingAdmin;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.metadata.FieldLineageAdmin;
import io.cdap.cdap.metadata.LineageAdmin;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metadata.MetadataService;
import io.cdap.cdap.metadata.MetadataServiceModule;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.scheduler.CoreSchedulerService;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizerInstantiator;
import io.cdap.cdap.security.authorization.InvalidAuthorizerException;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.Authorizer;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.cdap.test.internal.ApplicationManagerFactory;
import io.cdap.cdap.test.internal.ArtifactManagerFactory;
import io.cdap.cdap.test.internal.DefaultApplicationManager;
import io.cdap.cdap.test.internal.DefaultArtifactManager;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;

/**
 * Base class to inherit from for unit-test.
 * It provides testing functionality for {@link io.cdap.cdap.api.app.Application}.
 * To clean App Fabric state, you can use the {@link #clear} method.
 * <p>
 * Custom configurations for CDAP can be set by using {@link ClassRule} and {@link TestConfiguration}.
 * </p>
 *
 * @see TestConfiguration
 */
@RunWith(TestRunner.class)
public class TestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestBase.class);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  static Injector injector;
  private static CConfiguration cConf;
  private static int nestedStartCount;
  private static boolean firstInit = true;
  private static MetricsCollectionService metricsCollectionService;
  private static Scheduler scheduler;
  private static ExploreExecutorService exploreExecutorService;
  private static ExploreClient exploreClient;
  private static DatasetOpExecutorService dsOpService;
  private static DatasetService datasetService;
  private static TransactionManager txService;
  private static MetricsManager metricsManager;
  private static TestManager testManager;
  private static NamespaceAdmin namespaceAdmin;
  private static AuthorizerInstantiator authorizerInstantiator;
  private static SecureStore secureStore;
  private static SecureStoreManager secureStoreManager;
  private static MessagingService messagingService;
  private static Scheduler programScheduler;
  private static MessagingContext messagingContext;
  private static PreviewManager previewManager;
  private static MetadataService metadataService;
  private static MetadataSubscriberService metadataSubscriberService;
  private static MetadataStorage metadataStorage;
  private static MetadataAdmin metadataAdmin;
  private static FieldLineageAdmin fieldLineageAdmin;
  private static LineageAdmin lineageAdmin;
  private static AppFabricServer appFabricServer;

  // This list is to record ApplicationManager create inside @Test method
  private static final List<ApplicationManager> applicationManagers = new ArrayList<>();

  @BeforeClass
  public static void initialize() throws Exception {
    if (nestedStartCount++ > 0) {
      return;
    }
    File localDataDir = TMP_FOLDER.newFolder();

    cConf = createCConf(localDataDir);

    org.apache.hadoop.conf.Configuration hConf = new org.apache.hadoop.conf.Configuration();
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());

    // Windows specific requirements
    if (OSDetector.isWindows()) {
      File tmpDir = TMP_FOLDER.newFolder();
      File binDir = new File(tmpDir, "bin");
      Assert.assertTrue(binDir.mkdirs());

      copyTempFile("hadoop.dll", tmpDir);
      copyTempFile("winutils.exe", binDir);
      System.setProperty("hadoop.home.dir", tmpDir.getAbsolutePath());
      System.load(new File(tmpDir, "hadoop.dll").getAbsolutePath());
    }

    injector = Guice.createInjector(
      createDataFabricModule(),
      new TransactionExecutorModule(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new LocalLocationModule(),
      new InMemoryDiscoveryModule(),
      new AppFabricServiceRuntimeModule().getInMemoryModules(),
      new MonitorHandlerModule(false),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new SecureStoreServerModule(),
      new MetadataReaderWriterModules().getInMemoryModules(),
      new MetadataServiceModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsManager.class).toProvider(MetricsManagerProvider.class);
        }
      },
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new LocalLogAppenderModule(),
      new LogReaderRuntimeModules().getInMemoryModules(),
      new ExploreRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new PreviewHttpModule(),
      new MockProvisionerModule(),
      new AbstractModule() {
        @Override
        @SuppressWarnings("deprecation")
        protected void configure() {
          install(new FactoryModuleBuilder().implement(ApplicationManager.class, DefaultApplicationManager.class)
                    .build(ApplicationManagerFactory.class));
          install(new FactoryModuleBuilder().implement(ArtifactManager.class, DefaultArtifactManager.class)
                    .build(ArtifactManagerFactory.class));
          bind(TemporaryFolder.class).toInstance(TMP_FOLDER);
          bind(AuthorizationHandler.class).in(Scopes.SINGLETON);

          // Needed by MonitorHandlerModuler
          bind(TwillRunner.class).to(NoopTwillRunnerService.class);
        }
      }
    );

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }

    txService = injector.getInstance(TransactionManager.class);
    txService.startAndWait();

    metadataSubscriberService = injector.getInstance(MetadataSubscriberService.class);
    metadataStorage = injector.getInstance(MetadataStorage.class);
    metadataAdmin = injector.getInstance(MetadataAdmin.class);
    metadataStorage.createIndex();
    metadataService = injector.getInstance(MetadataService.class);
    metadataService.startAndWait();

    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class),
                                    injector.getInstance(StructuredTableRegistry.class));

    dsOpService = injector.getInstance(DatasetOpExecutorService.class);
    dsOpService.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
      exploreExecutorService.startAndWait();
      // wait for explore service to be discoverable
      DiscoveryServiceClient discoveryService = injector.getInstance(DiscoveryServiceClient.class);
      EndpointStrategy endpointStrategy = new RandomEndpointStrategy(() ->
        discoveryService.discover(Constants.Service.EXPLORE_HTTP_USER_SERVICE));
      Preconditions.checkNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS),
                                 "%s service is not up after 5 seconds", Constants.Service.EXPLORE_HTTP_USER_SERVICE);
      exploreClient = injector.getInstance(ExploreClient.class);
    }
    programScheduler = injector.getInstance(Scheduler.class);
    if (programScheduler instanceof Service) {
      ((Service) programScheduler).startAndWait();
    }

    testManager = injector.getInstance(UnitTestManager.class);
    metricsManager = injector.getInstance(MetricsManager.class);
    authorizerInstantiator = injector.getInstance(AuthorizerInstantiator.class);

    // This is needed so the logged-in user can successfully create the default namespace
    if (cConf.getBoolean(Constants.Security.Authorization.ENABLED)) {
      String user = System.getProperty("user.name");
      SecurityRequestContext.setUserId(user);
      InstanceId instance = new InstanceId(cConf.get(Constants.INSTANCE_NAME));
      Principal principal = new Principal(user, Principal.PrincipalType.USER);
      authorizerInstantiator.get().grant(Authorizable.fromEntityId(instance), principal,
                                         ImmutableSet.of(Action.ADMIN));
      authorizerInstantiator.get().grant(Authorizable.fromEntityId(NamespaceId.DEFAULT), principal,
                                         ImmutableSet.of(Action.ADMIN));
    }
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    if (firstInit) {
      // only create the default namespace on first test. if multiple tests are run in the same JVM,
      // then any time after the first time, the default namespace already exists. That is because
      // the namespaceAdmin.delete(Id.Namespace.DEFAULT) in finish() only clears the default namespace
      // but does not remove it entirely
      namespaceAdmin.create(NamespaceMeta.DEFAULT);
      ProfileService profileService = injector.getInstance(ProfileService.class);
      profileService.saveProfile(ProfileId.NATIVE, Profile.NATIVE);
    }
    secureStore = injector.getInstance(SecureStore.class);
    secureStoreManager = injector.getInstance(SecureStoreManager.class);
    messagingContext = new MultiThreadMessagingContext(messagingService);
    firstInit = false;
    previewManager = injector.getInstance(PreviewManager.class);
    fieldLineageAdmin = injector.getInstance(FieldLineageAdmin.class);
    lineageAdmin = injector.getInstance(LineageAdmin.class);
    metadataSubscriberService.startAndWait();
    if (previewManager instanceof Service) {
      ((Service) previewManager).startAndWait();
    }
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();

    scheduler = injector.getInstance(Scheduler.class);
    if (scheduler instanceof Service) {
      ((Service) scheduler).startAndWait();
    }
    if (scheduler instanceof CoreSchedulerService) {
      ((CoreSchedulerService) scheduler).waitUntilFunctional(10, TimeUnit.SECONDS);
    }
  }

  /**
   * If a subclass has a {@link BeforeClass} method, this can be called within that method to determine whether this
   * is the first time it was called. This is useful in test suites, where most initialization should only happen once
   * at the very first init.
   *
   * @return whether this is the first time the class has been initialized.
   */
  protected static boolean isFirstInit() {
    return nestedStartCount == 1;
  }

  private static TestManager getTestManager() {
    Preconditions.checkState(testManager != null, "Test framework is not yet running");
    return testManager;
  }

  @Before
  public void beforeTest() throws Exception {
    applicationManagers.clear();
  }

  /**
   * By default after each test finished, it will stop all apps started during the test.
   * Sub-classes can override this method to provide different behavior.
   */
  @After
  public void afterTest() throws Exception {
    for (ApplicationManager manager : applicationManagers) {
      manager.stopAll();
    }
  }

  private static class MetricsManagerProvider implements Provider<MetricsManager> {
    private final MetricStore metricStore;

    @Inject
    @SuppressWarnings("unused")
    private MetricsManagerProvider(MetricStore metricStore) {
      this.metricStore = metricStore;
    }

    @Override
    public MetricsManager get() {
      return new MetricsManager(metricStore);
    }
  }

  private static CConfiguration createCConf(File localDataDir) throws IOException {
    CConfiguration cConf = CConfiguration.create();

    // Setup defaults that can be overridden by user
    cConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, true);
    cConf.setBoolean(Constants.Explore.START_ON_DEMAND, false);
    cConf.set(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR, "");

    // Setup test case specific configurations.
    // The system properties are usually setup by TestConfiguration class using @ClassRule
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith(TestConfiguration.PROPERTY_PREFIX)) {
        String value = System.getProperty(key);
        cConf.set(key.substring(TestConfiguration.PROPERTY_PREFIX.length()), System.getProperty(key));
        LOG.info("Custom configuration set: {} = {}", key, value);
      }
    }

    // These configurations cannot be overridden by user
    // configure all services except for router to bind to localhost
    String localhost = InetAddress.getLoopbackAddress().getHostAddress();
    cConf.set(Constants.Service.MASTER_SERVICES_BIND_ADDRESS, localhost);
    cConf.set(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS, localhost);
    cConf.set(Constants.Transaction.Container.ADDRESS, localhost);
    cConf.set(Constants.Dataset.Executor.ADDRESS, localhost);
    cConf.set(Constants.Metrics.ADDRESS, localhost);
    cConf.set(Constants.MetricsProcessor.BIND_ADDRESS, localhost);
    cConf.set(Constants.LogSaver.ADDRESS, localhost);
    cConf.set(Constants.Security.AUTH_SERVER_BIND_ADDRESS, localhost);
    cConf.set(Constants.Explore.SERVER_ADDRESS, localhost);
    cConf.set(Constants.Metadata.SERVICE_BIND_ADDRESS, localhost);
    cConf.set(Constants.Preview.ADDRESS, localhost);

    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    cConf.set(Constants.Explore.LOCAL_DATA_DIR, TMP_FOLDER.newFolder("hive").getAbsolutePath());

    // Speed up test
    cConf.setLong(Constants.Scheduler.EVENT_POLL_DELAY_MILLIS, 100L);
    cConf.setLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS, 100L);

    return cConf;
  }

  private static Module createDataFabricModule() {
    return Modules.override(new DataFabricModules().getInMemoryModules())
      .with(new AbstractModule() {

        @Override
        protected void configure() {
          // we inject a TxSystemClient that creates transaction objects with additional fields for validation
          bind(InMemoryTxSystemClient.class).in(Scopes.SINGLETON);
          bind(TransactionSystemClient.class).to(RevealingTxSystemClient.class).in(Scopes.SINGLETON);
        }
      });
  }

  private static void copyTempFile(String infileName, File outDir) throws IOException {
    URL url = TestBase.class.getClassLoader().getResource(infileName);
    if (url == null) {
      throw new IOException("Failed to get resource for " + infileName);
    }
    File outFile = new File(outDir, infileName);
    ByteStreams.copy(Resources.newInputStreamSupplier(url), Files.newOutputStreamSupplier(outFile));
  }

  @AfterClass
  public static void finish() throws Exception {
    if (--nestedStartCount != 0) {
      return;
    }

    if (previewManager instanceof Service) {
      ((Service) previewManager).stopAndWait();
    }

    if (cConf.getBoolean(Constants.Security.Authorization.ENABLED)) {
      InstanceId instance = new InstanceId(cConf.get(Constants.INSTANCE_NAME));
      Principal principal = new Principal(System.getProperty("user.name"), Principal.PrincipalType.USER);
      authorizerInstantiator.get().grant(Authorizable.fromEntityId(instance), principal, ImmutableSet.of(Action.ADMIN));
      authorizerInstantiator.get().grant(Authorizable.fromEntityId(NamespaceId.DEFAULT),
                                         principal, ImmutableSet.of(Action.ADMIN));
    }
    
    namespaceAdmin.delete(NamespaceId.DEFAULT);
    authorizerInstantiator.close();

    if (programScheduler instanceof Service) {
      ((Service) programScheduler).stopAndWait();
    }
    metricsCollectionService.stopAndWait();
    if (scheduler instanceof Service) {
      ((Service) scheduler).stopAndWait();
    }
    Closeables.closeQuietly(exploreClient);
    if (exploreExecutorService != null) {
      exploreExecutorService.stopAndWait();
    }
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    metadataService.stopAndWait();
    metadataSubscriberService.stopAndWait();
    Closeables.closeQuietly(metadataStorage);
    txService.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
    appFabricServer.stopAndWait();
  }

  protected MetricsManager getMetricsManager() {
    return metricsManager;
  }

  /**
   * Deploys an {@link Application}.
   * Programs defined in the application must be in the same or children package as the application.
   *
   * @param namespace the namespace to deploy the application to
   * @param applicationClz the application class
   * @param bundleEmbeddedJars any extra jars to bundle in the application jar
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  protected static ApplicationManager deployApplication(NamespaceId namespace,
                                                        Class<? extends Application> applicationClz,
                                                        File... bundleEmbeddedJars) {
    return deployApplication(namespace, applicationClz, null, bundleEmbeddedJars);
  }


  /**
   * Deploys an {@link Application} with a config.
   * Programs defined in the application must be in the same or children package as the application.
   *
   * @param namespace the namespace to deploy the application to
   * @param applicationClz the application class
   * @param appConfig the application configApp
   * @param bundleEmbeddedJars any extra jars to bundle in the application jar
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  protected static ApplicationManager deployApplication(NamespaceId namespace,
                                                        Class<? extends Application> applicationClz, Config appConfig,
                                                        File... bundleEmbeddedJars) {
    ApplicationManager applicationManager = getTestManager().deployApplication(namespace, applicationClz, appConfig,
                                                                               bundleEmbeddedJars);
    applicationManagers.add(applicationManager);
    return applicationManager;
  }

  /**
   * Deploys an {@link Application}.
   * Programs defined in the application must be in the same or children package as the application.
   *
   * @param applicationClz the application class
   * @param bundleEmbeddedJars any extra jars to bundle in the application jar
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  protected static ApplicationManager deployApplication(Class<? extends Application> applicationClz,
                                                        File... bundleEmbeddedJars) {
    return deployApplication(NamespaceId.DEFAULT, applicationClz, bundleEmbeddedJars);
  }

  /**
   * Deploys an {@link Application}.
   * Programs defined in the application must be in the same or children package as the application.
   *
   * @param applicationClz the application class
   * @param appConfig the application config
   * @param bundleEmbeddedJars any extra jars to bundle in the application jar
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  protected static ApplicationManager deployApplication(Class<? extends Application> applicationClz, Config appConfig,
                                                        File... bundleEmbeddedJars) {
    return deployApplication(NamespaceId.DEFAULT, applicationClz, appConfig, bundleEmbeddedJars);
  }

  /**
   * Deploys an {@link Application} with version. The application artifact must already exist.
   *
   * @param appId the id of the application to create
   * @param appRequest the application create or update request
   * @return An {@link ApplicationManager} to manage the deployed application
   */
  protected static ApplicationManager deployApplication(ApplicationId appId,
                                                        AppRequest appRequest) throws Exception {
    ApplicationManager appManager = getTestManager().deployApplication(appId, appRequest);
    applicationManagers.add(appManager);
    return appManager;
  }

  /**
   * Add the specified artifact.
   *
   * @param artifactId the id of the artifact to add
   * @param artifactFile the contents of the artifact. Must be a valid jar file containing apps or plugins
   * @throws Exception
   */
  protected static ArtifactManager addArtifact(ArtifactId artifactId, File artifactFile) throws Exception {
    return getTestManager().addArtifact(artifactId, artifactFile);
  }

  /**
   * Build an application artifact from the specified class and then add it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @return an {@link ArtifactManager} to manage the added artifact
   */
  protected static ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass) throws Exception {
    return getTestManager().addAppArtifact(artifactId, appClass);
  }

  /**
   * Build an application artifact from the specified class and then add it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @param exportPackages the packages to export and place in the manifest of the jar to build. This should include
   *                       packages that contain classes that plugins for the application will implement.
   * @return an {@link ArtifactManager} to manage the added artifact
   */
  protected static ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass,
                                                  String... exportPackages) throws Exception {
    return getTestManager().addAppArtifact(artifactId, appClass, exportPackages);
  }

  /**
   * Build an application artifact from the specified class and then add it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @param manifest the manifest to use when building the jar
   * @return an {@link ArtifactManager} to manage the added artifact
   */
  protected static ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass,
                                                  Manifest manifest) throws Exception {
    return getTestManager().addAppArtifact(artifactId, appClass, manifest);
  }

  /**
   * Build an artifact from the specified plugin classes and then add it. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param artifactId the id of the artifact to add
   * @param parent the parent artifact it extends
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @return {@link ArtifactManager} to manage the added plugin artifact
   */
  protected static ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                                     Class<?> pluginClass,
                                                     Class<?>... pluginClasses) throws Exception {
    return getTestManager().addPluginArtifact(artifactId, parent, pluginClass, pluginClasses);
  }

  /**
   * Build an artifact from the specified plugin classes and then add it. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param artifactId the id of the artifact to add
   * @param parent the parent artifact it extends
   * @param additionalPlugins any plugin classes that need to be explicitly declared because they cannot be found
   *                          by inspecting the jar. This is true for 3rd party plugins, such as jdbc drivers
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @return an {@link ArtifactManager} to manage the added plugin artifact
   */
  protected static ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                                     Set<PluginClass> additionalPlugins,
                                                     Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    return getTestManager().addPluginArtifact(artifactId, parent, additionalPlugins, pluginClass, pluginClasses);
  }

  /**
   * Build an artifact from the specified plugin classes and then add it. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param artifactId the id of the artifact to add
   * @param parentArtifacts the parent artifacts it extends
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @return an {@link ArtifactManager} to manage the added plugin artifact
   */
  protected static ArtifactManager addPluginArtifact(ArtifactId artifactId,
                                                     Set<ArtifactRange> parentArtifacts, Class<?> pluginClass,
                                                     Class<?>... pluginClasses) throws Exception {
    return getTestManager().addPluginArtifact(artifactId, parentArtifacts, pluginClass, pluginClasses);
  }

  /**
   * Clear the state of app fabric, by removing all deployed applications, Datasets and Streams.
   * This method could be called between two unit tests, to make them independent.
   */
  protected static void clear() {
    try {
      getTestManager().clear();
    } catch (Exception e) {
      // Unchecked exception to maintain compatibility until we remove this method
      throw Throwables.propagate(e);
    }
  }

  /**
   * Deploys {@link DatasetModule}.
   *
   * @param datasetModuleId the module id
   * @param datasetModule module class
   * @throws Exception
   */
  protected static void deployDatasetModule(DatasetModuleId datasetModuleId,
                                            Class<? extends DatasetModule> datasetModule) throws Exception {
    getTestManager().deployDatasetModule(datasetModuleId, datasetModule);
  }

  /**
   * Deploys {@link DatasetModule}.
   *
   * @param moduleName name of the module
   * @param datasetModule module class
   * @throws Exception
   */
  protected static void deployDatasetModule(String moduleName,
                                            Class<? extends DatasetModule> datasetModule) throws Exception {
    deployDatasetModule(NamespaceId.DEFAULT.datasetModule(moduleName), datasetModule);
  }

  /**
   * Adds an instance of a dataset.
   *
   * @param datasetId the dataset id
   * @param datasetTypeName dataset type name, like "table" or "fileset"
   * @param props properties
   * @param <T> type of the dataset admin
   * @return a DatasetAdmin to manage the dataset instance
   */
  protected static <T extends DatasetAdmin> T addDatasetInstance(String datasetTypeName, DatasetId datasetId,
                                                                 DatasetProperties props) throws Exception {
    return getTestManager().addDatasetInstance(datasetTypeName, datasetId, props);
  }

  /**
   * Adds an instance of a dataset.
   *
   * @param datasetTypeName dataset type name
   * @param datasetInstanceName instance name
   * @param props properties
   * @param <T> type of the dataset admin
   * @return a DatasetAdmin to manage the dataset instance
   */
  protected static <T extends DatasetAdmin> T addDatasetInstance(String datasetTypeName,
                                                                 String datasetInstanceName,
                                                                 DatasetProperties props) throws Exception {
    return addDatasetInstance(datasetTypeName, NamespaceId.DEFAULT.dataset(datasetInstanceName), props);
  }

  /**
   * Adds an instance of dataset.
   *
   * @param datasetId the dataset id
   * @param datasetTypeName dataset type name
   * @param <T> type of the dataset admin
   * @return a DatasetAdmin to manage the dataset instance
   */
  protected final <T extends DatasetAdmin> T addDatasetInstance(DatasetId datasetId,
                                                                String datasetTypeName) throws Exception {
    return addDatasetInstance(datasetTypeName, datasetId, DatasetProperties.EMPTY);
  }

  /**
   * Adds an instance of dataset.
   *
   * @param datasetTypeName dataset type name
   * @param datasetInstanceName instance name
   * @param <T> type of the dataset admin
   * @return a DatasetAdmin to manage the dataset instance
   */
  protected final <T extends DatasetAdmin> T addDatasetInstance(String datasetTypeName,
                                                                String datasetInstanceName) throws Exception {
    return addDatasetInstance(datasetTypeName, NamespaceId.DEFAULT.dataset(datasetInstanceName),
                              DatasetProperties.EMPTY);
  }

  /**
   * Deletes an instance of dataset.
   *
   * @param datasetId the dataset to delete
   */
  protected void deleteDatasetInstance(DatasetId datasetId) throws Exception {
    getTestManager().deleteDatasetInstance(datasetId);
  }

  /**
   * Gets Dataset manager of Dataset instance of type {@literal <}T>.
   *
   * @param datasetId the id of the dataset to get
   * @return Dataset Manager of Dataset instance of type <T>
   * @throws Exception
   */
  protected final <T> DataSetManager<T> getDataset(DatasetId datasetId) throws Exception {
    return getTestManager().getDataset(datasetId);
  }

  /**
   * Gets Dataset manager of Dataset instance of type {@literal <}T>.
   *
   * @param datasetInstanceName instance name of dataset
   * @return Dataset Manager of Dataset instance of type {@literal <}T>
   * @throws Exception
   */
  protected final <T> DataSetManager<T> getDataset(String datasetInstanceName) throws Exception {
    return getDataset(NamespaceId.DEFAULT.dataset(datasetInstanceName));
  }

  /**
   * Gets the app detail of an application
   *
   * @param applicationId the app id of the application
   * @return ApplicationDetail of the app
   */
  protected final ApplicationDetail getAppDetail(ApplicationId applicationId) throws Exception {
    return getTestManager().getApplicationDetail(applicationId);
  }

  /**
   * Add a new schedule to an existing application
   *
   * @param scheduleId the ID of the schedule to add
   * @param scheduleDetail the {@link ScheduleDetail} describing the new schedule.
   */
  protected final void addSchedule(ScheduleId scheduleId, ScheduleDetail scheduleDetail) throws Exception {
    getTestManager().addSchedule(scheduleId, scheduleDetail);
  }

  /**
   * Delete an existing schedule.
   *
   * @param scheduleId the ID of the schedule to be deleted
   */
  protected final void deleteSchedule(ScheduleId scheduleId) throws Exception {
    getTestManager().deleteSchedule(scheduleId);
  }

  /**
   * Update an existing schedule.
   *
   * @param scheduleId the ID of the schedule to add
   * @param scheduleDetail the {@link ScheduleDetail} describing the updated schedule.
   */
  protected final void updateSchedule(ScheduleId scheduleId, ScheduleDetail scheduleDetail) throws Exception {
    getTestManager().updateSchedule(scheduleId, scheduleDetail);
  }

  /**
   * Returns a JDBC connection that allows the running of SQL queries over data sets.
   *
   * @param namespace namespace for the connection
   * @return Connection to use to run queries
   */
  protected final Connection getQueryClient(NamespaceId namespace) throws Exception {
    if (!cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      throw new UnsupportedOperationException("Explore service is disabled. QueryClient not supported.");
    }
    return getTestManager().getQueryClient(namespace);
  }

  /**
   * Returns a JDBC connection that allows the running of SQL queries over data sets.
   */
  protected final Connection getQueryClient() throws Exception {
    return getQueryClient(NamespaceId.DEFAULT);
  }

  /**
   * Returns a {@link MessagingContext} for interacting with the messaging system.
   */
  protected final MessagingContext getMessagingContext() {
    return messagingContext;
  }

  /**
   * Returns a {@link MessagingAdmin} for the given namespace.
   */
  protected static MessagingAdmin getMessagingAdmin(String namespace) {
    return getMessagingAdmin(new NamespaceId(namespace));
  }

  protected static MessagingAdmin getMessagingAdmin(NamespaceId namespace) {
    return new BasicMessagingAdmin(messagingService, namespace);
  }

  /**
   * @return {@link MetadataAdmin} to interact with metadata
   */
  protected static MetadataAdmin getMetadataAdmin() {
    return metadataAdmin;
  }

  /**
   * Returns a {@link NamespaceAdmin} to interact with namespaces.
   */
  protected static NamespaceAdmin getNamespaceAdmin() {
    return namespaceAdmin;
  }

  /**
   * Returns a {@link SecureStore} to interact with secure storage.
   */
  protected static SecureStore getSecureStore() {
    return secureStore;
  }

  /**
   * Returns a {@link SecureStoreManager} to interact with secure storage.
   */
  protected static SecureStoreManager getSecureStoreManager() {
    return secureStoreManager;
  }

  /**
   * Returns an {@link Authorizer} for performing authorization operations.
   */
  @Beta
  protected static Authorizer getAuthorizer() throws IOException, InvalidAuthorizerException {
    return authorizerInstantiator.get();
  }

  /**
   * Returns a {@link PreviewManager} to interact with preview.
   */
  protected static PreviewManager getPreviewManager() {
    return previewManager;
  }

  /**
   * Returns a {@link FieldLineageAdmin to interact with field lineage.
   */
  protected static FieldLineageAdmin getFieldLineageAdmin() {
    return fieldLineageAdmin;
  }

  /**
   * Returns a {@link LineageAdmin to interact with field lineage.
   */
  protected static LineageAdmin getLineageAdmin() {
    return lineageAdmin;
  }

  /**
   * Returns the {@link CConfiguration} used in tests.
   */
  protected static CConfiguration getConfiguration() {
    return cConf;
  }

  /**
   * Deletes all applications in the specified namespace.
   *
   * @param namespaceId the namespace from which to delete all applications
   */
  protected static void deleteAllApplications(NamespaceId namespaceId) throws Exception {
    getTestManager().deleteAllApplications(namespaceId);
  }


  /**
   * Executes the given {@link HttpRequest}.
   */
  protected final HttpResponse executeHttp(HttpRequest request) throws IOException {
    return HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
  }
}

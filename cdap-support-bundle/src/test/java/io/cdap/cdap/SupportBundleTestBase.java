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

package io.cdap.cdap;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.app.store.ServiceStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.logging.service.LogQueryService;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.metadata.MetadataService;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProtoConstraintCodec;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.scheduler.CoreSchedulerService;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.cdap.support.handlers.SupportBundleHttpHandler;
import io.cdap.cdap.support.services.SupportBundleInternalService;
import io.cdap.cdap.support.task.factory.SupportBundlePipelineInfoTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleSystemLogTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import io.cdap.cdap.support.task.factory.SupportBundleVMInfoTaskFactory;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.HttpHandler;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;
import javax.annotation.Nullable;
import org.apache.tephra.TransactionManager;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * AppFabric HttpHandler Test classes can extend this class, this will allow the HttpService be setup before
 * running the handler tests, this also gives the ability to run individual test cases.
 */
public abstract class SupportBundleTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  protected static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .registerTypeAdapter(Trigger.class, new TriggerCodec())
    .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
    .registerTypeAdapter(Constraint.class, new ProtoConstraintCodec())
    .create();
  private static final String API_KEY = "SampleTestApiKey";

  private static final Type LIST_RUN_RECORD_TYPE = new TypeToken<List<RunRecord>>() { }.getType();

  private static Injector injector;

  private static EndpointStrategy appFabricEndpointStrategy;
  private static MessagingService messagingService;
  private static TransactionManager txManager;
  private static AppFabricServer appFabricServer;
  private static MetricsCollectionService metricsCollectionService;
  private static DatasetOpExecutorService dsOpService;
  private static DatasetService datasetService;
  private static ServiceStore serviceStore;
  private static MetadataStorage metadataStorage;
  private static MetadataService metadataService;
  private static MetadataSubscriberService metadataSubscriberService;
  private static LocationFactory locationFactory;
  private static LogQueryService logQueryService;
  private static SupportBundleInternalService supportBundleInternalService;

  private static HttpRequestConfig httpRequestConfig;

  protected static final String TEST_NAMESPACE1 = "testsupportbundlenamespace1";
  protected static final NamespaceMeta TEST_NAMESPACE_META1 = new NamespaceMeta.Builder()
    .setName(TEST_NAMESPACE1)
    .setDescription(TEST_NAMESPACE1)
    .build();

  @BeforeClass
  public static void beforeClass() throws Throwable {
    initializeAndStartServices(createBasicCConf());
  }

  protected static void initializeAndStartServices(CConfiguration cConf) throws Exception {
    initializeAndStartServices(cConf, new AbstractModule() {
      @Override
      protected void configure() {
        // needed because we set Kerberos to true in DefaultNamespaceAdminTest
        bind(UGIProvider.class).to(CurrentUGIProvider.class);
        bind(MetadataSubscriberService.class).in(Scopes.SINGLETON);
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
          binder(), HttpHandler.class, Names.named(Constants.SupportBundle.HANDLERS_NAME));

        CommonHandlers.add(handlerBinder);
        handlerBinder.addBinding().to(SupportBundleHttpHandler.class);
        Multibinder<SupportBundleTaskFactory> supportBundleTaskFactoryMultibinder = Multibinder.newSetBinder(
          binder(), SupportBundleTaskFactory.class, Names.named(Constants.SupportBundle.TASK_FACTORY));
        supportBundleTaskFactoryMultibinder.addBinding().to(SupportBundlePipelineInfoTaskFactory.class);
        supportBundleTaskFactoryMultibinder.addBinding().to(SupportBundleSystemLogTaskFactory.class);
        supportBundleTaskFactoryMultibinder.addBinding().to(SupportBundleVMInfoTaskFactory.class);
      }
    });
  }

  protected static void initializeAndStartServices(CConfiguration cConf, Module overrides) throws Exception {
    injector = Guice.createInjector(
      Modules.override(new SupportBundleTestModule(cConf, null)).with(overrides));

    int connectionTimeout = cConf.getInt(Constants.HTTP_CLIENT_CONNECTION_TIMEOUT_MS);
    int readTimeout = cConf.getInt(Constants.HTTP_CLIENT_READ_TIMEOUT_MS);
    httpRequestConfig = new HttpRequestConfig(connectionTimeout, readTimeout, false);

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));
    metadataStorage = injector.getInstance(MetadataStorage.class);
    metadataStorage.createIndex();

    dsOpService = injector.getInstance(DatasetOpExecutorService.class);
    dsOpService.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    appFabricEndpointStrategy = new RandomEndpointStrategy(
      () -> discoveryClient.discover(Constants.Service.APP_FABRIC_HTTP));
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    serviceStore = injector.getInstance(ServiceStore.class);
    serviceStore.startAndWait();
    metadataService = injector.getInstance(MetadataService.class);
    metadataService.startAndWait();
    metadataSubscriberService = injector.getInstance(MetadataSubscriberService.class);
    metadataSubscriberService.startAndWait();
    logQueryService = injector.getInstance(LogQueryService.class);
    logQueryService.startAndWait();
    locationFactory = getInjector().getInstance(LocationFactory.class);
    supportBundleInternalService = injector.getInstance(SupportBundleInternalService.class);
    supportBundleInternalService.startAndWait();

    Scheduler programScheduler = injector.getInstance(Scheduler.class);
    // Wait for the scheduler to be functional.
    if (programScheduler instanceof CoreSchedulerService) {
      try {
        ((CoreSchedulerService) programScheduler).waitUntilFunctional(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @AfterClass
  public static void afterClass() throws IOException {
    appFabricServer.stopAndWait();
    metricsCollectionService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txManager.stopAndWait();
    serviceStore.stopAndWait();
    metadataSubscriberService.stopAndWait();
    metadataService.stopAndWait();
    logQueryService.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
    Closeables.closeQuietly(metadataStorage);
    supportBundleInternalService.stopAndWait();
  }

  protected static CConfiguration createBasicCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Service.MASTER_SERVICES_BIND_ADDRESS, InetAddress.getLoopbackAddress().getHostAddress());
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("data").getAbsolutePath());
    cConf.setInt(Constants.Capability.AUTO_INSTALL_THREADS, 5);
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    // add the plugin exclusion if one has been set by the test class
    String excludedRequirements = System.getProperty(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE);
    if (excludedRequirements != null) {
      cConf.set(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, excludedRequirements);
    }
    // Use a shorter delay to speedup tests
    cConf.setLong(Constants.Metadata.MESSAGING_POLL_DELAY_MILLIS, 100L);
    return cConf;
  }

  protected static Injector getInjector() {
    return injector;
  }

  protected static URI getEndPoint(String path) {
    Discoverable discoverable = appFabricEndpointStrategy.pick(5, TimeUnit.SECONDS);
    Assert.assertNotNull("SupportBundle endpoint is missing, service may not be running.", discoverable);
    return URIScheme.createURI(discoverable,"%s", path);
  }

  protected static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  protected static HttpResponse doGet(String resource, @Nullable Map<String, String> headers) throws Exception {
    HttpRequest.Builder builder = HttpRequest.get(getEndPoint(resource).toURL());

    if (headers != null) {
      builder.addHeaders(headers);
    }
    addStandardHeaders(builder);
    return HttpRequests.execute(builder.build(), httpRequestConfig);
  }

  protected static HttpResponse doPut(String resource, @Nullable String body) throws Exception {
    HttpRequest.Builder builder = HttpRequest.put(getEndPoint(resource).toURL());
    addStandardHeaders(builder);
    if (body != null) {
      builder.withBody(body);
    }
    return HttpRequests.execute(builder.build(), httpRequestConfig);
  }

  private static HttpRequest.Builder addStandardHeaders(HttpRequest.Builder builder) {
    builder.addHeader(Constants.Gateway.API_KEY, API_KEY);
    String userId = SecurityRequestContext.getUserId();
    if (userId != null) {
      builder.addHeader(Constants.Security.Headers.USER_ID, userId);
    }
    return builder;
  }

  private static void assertResponseCode(int expectedCode, HttpResponse response) {
    Assert.assertEquals("Wrong response code with message " + response.getResponseBodyAsString(), expectedCode,
                        response.getResponseCode());
  }

  protected HttpResponse deploy(Class<?> application, int expectedCode, @Nullable String apiVersion,
                                @Nullable String namespace) throws Exception {
    return deploy(application, expectedCode, apiVersion, namespace, null, null, null);
  }

  /**
   * Deploys an application with (optionally) a defined app name and app version
   */
  protected HttpResponse deploy(Class<?> application, int expectedCode, @Nullable String apiVersion,
                                @Nullable String namespace, @Nullable String artifactVersion,
                                @Nullable Config appConfig, @Nullable String ownerPrincipal) throws Exception {
    namespace = namespace == null ? Id.Namespace.DEFAULT.getId() : namespace;
    apiVersion = apiVersion == null ? Constants.Gateway.API_VERSION_3_TOKEN : apiVersion;
    artifactVersion = artifactVersion == null ? String.format("1.0.%d", System.currentTimeMillis()) : artifactVersion;

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.BUNDLE_VERSION, artifactVersion);

    File artifactJar = buildAppArtifact(application, application.getSimpleName(), manifest);

    String versionedApiPath = getVersionedAPIPath("apps/", apiVersion, namespace);
    HttpRequest.Builder builder = HttpRequest.post(getEndPoint(versionedApiPath).toURL())
      .addHeader(Constants.Gateway.API_KEY, "api-key-example")
      .addHeader(AbstractAppFabricHttpHandler.ARCHIVE_NAME_HEADER,
                 String.format("%s-%s.jar", application.getSimpleName(), artifactVersion));

    if (appConfig != null) {
      builder.addHeader(AbstractAppFabricHttpHandler.APP_CONFIG_HEADER, GSON.toJson(appConfig));
    }
    if (ownerPrincipal != null) {
      builder.addHeader(AbstractAppFabricHttpHandler.PRINCIPAL_HEADER, ownerPrincipal);
    }
    builder.withBody(artifactJar);

    HttpResponse response = HttpRequests.execute(builder.build(), httpRequestConfig);
    if (expectedCode != response.getResponseCode()) {
      Assert.fail(String.format(
        "Expected response code %d but got %d when trying to deploy app '%s' in namespace '%s'. "
          + "Response message = '%s'",
        expectedCode, response.getResponseCode(), application.getName(), namespace, response.getResponseMessage()));
    }
    return response;
  }

  protected String getVersionedAPIPath(String nonVersionedApiPath, String namespace) {
    return getVersionedAPIPath(nonVersionedApiPath, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
  }

  protected String getVersionedAPIPath(String nonVersionedApiPath, String version, String namespace) {
    if (!Constants.Gateway.API_VERSION_3_TOKEN.equals(version)) {
      throw new IllegalArgumentException(String.format("Unsupported version '%s'. Only v3 is supported.", version));
    }
    Preconditions.checkArgument(namespace != null, "Namespace cannot be null for v3 APIs.");
    return String.format("/%s/namespaces/%s/%s", version, namespace, nonVersionedApiPath);
  }

  protected List<RunRecord> getProgramRuns(ProgramId program, ProgramRunStatus status) throws Exception {
    String path =
      String.format("apps/%s/versions/%s/%s/%s/runs?status=%s", program.getApplication(), program.getVersion(),
                    program.getType().getCategoryName(), program.getProgram(), status.toString());
    HttpResponse response = doGet(getVersionedAPIPath(path, program.getNamespace()));
    assertResponseCode(200, response);
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_RUN_RECORD_TYPE);
  }

  protected HttpResponse createNamespace(NamespaceId namespaceId) throws Exception {
    return doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, namespaceId.getNamespace()), null);
  }

  protected HttpResponse deleteNamespace(NamespaceId namespaceId) throws IOException {
    String path = String.format("%s/unrecoverable/namespaces/%s",
                                Constants.Gateway.API_VERSION_3, namespaceId.getNamespace());
    HttpRequest.Builder builder = HttpRequest.delete(getEndPoint(path).toURL());
    addStandardHeaders(builder);
    return HttpRequests.execute(builder.build(), httpRequestConfig);
  }

  protected HttpResponse getNamespace(String name) throws Exception {
    Preconditions.checkArgument(name != null, "namespace name cannot be null");
    return doGet(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, name));
  }

  private File buildAppArtifact(Class<?> cls, String name, Manifest manifest) throws IOException {
    if (!name.endsWith(".jar")) {
      name += ".jar";
    }
    File destination = new File(TEMP_FOLDER.newFolder(), name);
    return buildAppArtifact(cls, manifest, destination);
  }

  protected File buildAppArtifact(Class<?> cls, Manifest manifest, File destination) throws IOException {
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, cls, manifest);
    Locations.linkOrCopyOverwrite(appJar, destination);
    return destination;
  }

  protected static HttpResponse doDelete(String resource) throws Exception {
    return HttpRequests.execute(addStandardHeaders(HttpRequest.delete(getEndPoint(resource).toURL()))
                                  .build(), httpRequestConfig);
  }

  protected static String getResponseApplicationRecordVersion(String jsonResponse) {
    return ((List<ApplicationRecord>) GSON.fromJson(jsonResponse,
                                                    new TypeToken<List<ApplicationRecord>>() { }.getType()))
      .get(0).getAppVersion();
  }
}

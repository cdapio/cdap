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

package io.cdap.cdap.internal.app.services.http;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.app.store.ServiceStore;
import io.cdap.cdap.client.DatasetClient;
import io.cdap.cdap.client.MetadataClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.test.PluginJarHelper;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.internal.guice.AppFabricTestModule;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metadata.MetadataService;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.BatchApplicationDetail;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.BatchProgramHistory;
import io.cdap.cdap.proto.BatchProgramSchedule;
import io.cdap.cdap.proto.DatasetMeta;
import io.cdap.cdap.proto.EntityScope;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProtoConstraintCodec;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.runtime.spi.profile.ProfileStatus;
import io.cdap.cdap.scheduler.CoreSchedulerService;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;

/**
 * AppFabric HttpHandler Test classes can extend this class, this will allow the HttpService be setup before
 * running the handler tests, this also gives the ability to run individual test cases.
 */
public abstract class AppFabricTestBase {
  protected static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .registerTypeAdapter(Trigger.class, new TriggerCodec())
    .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
    .registerTypeAdapter(Constraint.class, new ProtoConstraintCodec())
    .create();
  private static final String API_KEY = "SampleTestApiKey";

  private static final Type BATCH_PROGRAM_RUNS_TYPE = new TypeToken<List<BatchProgramHistory>>() { }.getType();
  private static final Type LIST_JSON_OBJECT_TYPE = new TypeToken<List<JsonObject>>() { }.getType();
  private static final Type LIST_RUNRECORD_TYPE = new TypeToken<List<RunRecord>>() { }.getType();
  private static final Type SET_TRING_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Type LIST_PROFILE = new TypeToken<List<Profile>>() { }.getType();

  protected static final Type LIST_MAP_STRING_STRING_TYPE = new TypeToken<List<Map<String, String>>>() { }.getType();
  protected static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  protected static final String NONEXISTENT_NAMESPACE = "12jr0j90jf3foieoi33";

  protected static final String TEST_NAMESPACE1 = "testnamespace1";
  protected static final NamespaceMeta TEST_NAMESPACE_META1 = new NamespaceMeta.Builder()
    .setName(TEST_NAMESPACE1)
    .setDescription(TEST_NAMESPACE1)
    .build();
  protected static final String TEST_NAMESPACE2 = "testnamespace2";
  protected static final NamespaceMeta TEST_NAMESPACE_META2 = new NamespaceMeta.Builder()
    .setName(TEST_NAMESPACE2)
    .setDescription(TEST_NAMESPACE2)
    .build();

  protected static final String VERSION1 = "1.0.0";
  protected static final String VERSION2 = "2.0.0";

  private static Injector injector;

  private static EndpointStrategy appFabricEndpointStrategy;
  private static MessagingService messagingService;
  private static TransactionManager txManager;
  private static AppFabricServer appFabricServer;
  private static MetricsCollectionService metricsCollectionService;
  private static DatasetOpExecutorService dsOpService;
  private static DatasetService datasetService;
  private static TransactionSystemClient txClient;
  private static ServiceStore serviceStore;
  private static MetadataStorage metadataStorage;
  private static MetadataService metadataService;
  private static DefaultMetadataServiceClient metadataServiceClient;
  private static MetadataSubscriberService metadataSubscriberService;
  private static LocationFactory locationFactory;
  private static DatasetClient datasetClient;
  private static MetadataClient metadataClient;
  private static MetricStore metricStore;

  private static HttpRequestConfig httpRequestConfig;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws Throwable {
    initializeAndStartServices(createBasicCConf(), null);
  }

  protected static void initializeAndStartServices(CConfiguration cConf,
                                                   @Nullable SConfiguration sConf) throws Exception {
    initializeAndStartServices(cConf, sConf, new AbstractModule() {
      @Override
      protected void configure() {
        // needed because we set Kerberos to true in DefaultNamespaceAdminTest
        bind(UGIProvider.class).to(CurrentUGIProvider.class);
        bind(MetadataSubscriberService.class).in(Scopes.SINGLETON);
      }
    });
  }

  protected static void initializeAndStartServices(CConfiguration cConf, @Nullable SConfiguration sConf,
                                                   Module overrides) throws Exception {
    injector = Guice.createInjector(
      Modules.override(new AppFabricTestModule(cConf, sConf)).with(overrides));

    int connectionTimeout = cConf.getInt(Constants.HTTP_CLIENT_CONNECTION_TIMEOUT_MS);
    int readTimeout = cConf.getInt(Constants.HTTP_CLIENT_READ_TIMEOUT_MS);
    httpRequestConfig = new HttpRequestConfig(connectionTimeout, readTimeout, false);

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    structuredTableRegistry.initialize();
    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class),
                                    structuredTableRegistry);
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
    txClient = injector.getInstance(TransactionSystemClient.class);
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    serviceStore = injector.getInstance(ServiceStore.class);
    serviceStore.startAndWait();
    metadataService = injector.getInstance(MetadataService.class);
    metadataService.startAndWait();
    metadataSubscriberService = injector.getInstance(MetadataSubscriberService.class);
    metadataSubscriberService.startAndWait();
    locationFactory = getInjector().getInstance(LocationFactory.class);
    datasetClient = new DatasetClient(getClientConfig(discoveryClient, Constants.Service.DATASET_MANAGER));
    metadataClient = new MetadataClient(getClientConfig(discoveryClient, Constants.Service.METADATA_SERVICE));
    metadataServiceClient = new DefaultMetadataServiceClient(discoveryClient);
    metricStore = injector.getInstance(MetricStore.class);

    Scheduler programScheduler = injector.getInstance(Scheduler.class);
    // Wait for the scheduler to be functional.
    if (programScheduler instanceof CoreSchedulerService) {
      try {
        ((CoreSchedulerService) programScheduler).waitUntilFunctional(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    createNamespaces();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    deleteNamespaces();
    appFabricServer.stopAndWait();
    metricsCollectionService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txManager.stopAndWait();
    serviceStore.stopAndWait();
    metadataSubscriberService.stopAndWait();
    metadataService.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
    Closeables.closeQuietly(metadataStorage);
  }

  protected static CConfiguration createBasicCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Service.MASTER_SERVICES_BIND_ADDRESS, InetAddress.getLoopbackAddress().getHostAddress());
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder("data").getAbsolutePath());
    cConf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
    cConf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    String updateSchedules = System.getProperty(Constants.AppFabric.APP_UPDATE_SCHEDULES);
    if (updateSchedules != null) {
      cConf.set(Constants.AppFabric.APP_UPDATE_SCHEDULES, updateSchedules);
    }
    // add the plugin exclusion if one has been set by the test class
    String excludedRequirements = System.getProperty(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE);
    if (excludedRequirements != null) {
      cConf.set(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, excludedRequirements);
    }
    // Use a shorter delay to speedup tests
    cConf.setLong(Constants.Scheduler.EVENT_POLL_DELAY_MILLIS, 100L);
    cConf.setLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS, 100L);
    cConf.setLong(Constants.Metadata.MESSAGING_POLL_DELAY_MILLIS, 100L);

    cConf.setBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE, true);
    cConf.set(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR, tmpFolder.newFolder("system-artifacts").getAbsolutePath());

    // reduce the number of constraint checker threads
    cConf.setInt(Constants.Scheduler.JOB_QUEUE_NUM_PARTITIONS, 2);
    // reduce the number of app fabric threads
    cConf.setInt(Constants.AppFabric.WORKER_THREADS, 2);
    cConf.setInt(Constants.AppFabric.EXEC_THREADS, 5);
    // reduce the number of dataset service threads
    cConf.setInt(Constants.Dataset.Manager.WORKER_THREADS, 2);
    cConf.setInt(Constants.Dataset.Manager.EXEC_THREADS, 5);
    // reduce the number of dataset executor threads
    cConf.setInt(Constants.Dataset.Executor.WORKER_THREADS, 2);
    cConf.setInt(Constants.Dataset.Executor.EXEC_THREADS, 5);
    return cConf;
  }

  protected static Injector getInjector() {
    return injector;
  }

  protected static TransactionSystemClient getTxClient() {
    return txClient;
  }

  protected static URI getEndPoint(String path) {
    Discoverable discoverable = appFabricEndpointStrategy.pick(5, TimeUnit.SECONDS);
    Assert.assertNotNull("AppFabric endpoint is missing, service may not be running.", discoverable);
    // The path is literal and we need to escape "%" before passing to createURI, which takes a format string.
    return URIScheme.createURI(discoverable, path.replace("%", "%%"));
  }

  protected static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  protected static HttpResponse doGet(String resource, @Nullable Map<String, String> headers) throws Exception {
    HttpRequest.Builder builder = HttpRequest.get(getEndPoint(resource).toURL());

    if (headers != null) {
      builder.addHeaders(headers);
    }
    builder.addHeader(Constants.Gateway.API_KEY, API_KEY);
    return HttpRequests.execute(builder.build(), httpRequestConfig);
  }

  protected static HttpResponse doPost(String resource) throws Exception {
    return doPost(resource, null, null);
  }

  protected static HttpResponse doPost(String resource, String body) throws Exception {
    return doPost(resource, body, null);
  }

  protected static HttpResponse doPost(String resource, @Nullable String body,
                                       @Nullable Map<String, String> headers) throws Exception {
    HttpRequest.Builder builder = HttpRequest.post(getEndPoint(resource).toURL());

    if (headers != null) {
      builder.addHeaders(headers);
    }
    builder.addHeader(Constants.Gateway.API_KEY, API_KEY);

    if (body != null) {
      builder.withBody(body);
    }
    return HttpRequests.execute(builder.build(), httpRequestConfig);
  }

  protected static HttpResponse doPut(String resource, @Nullable String body) throws Exception {
    HttpRequest.Builder builder = HttpRequest.put(getEndPoint(resource).toURL());
    builder.addHeader(Constants.Gateway.API_KEY, API_KEY);
    if (body != null) {
      builder.withBody(body);
    }
    return HttpRequests.execute(builder.build(), httpRequestConfig);
  }

  protected static HttpResponse doDelete(String resource) throws Exception {
    return HttpRequests.execute(HttpRequest.delete(getEndPoint(resource).toURL())
                                  .addHeader(Constants.Gateway.API_KEY, API_KEY).build(), httpRequestConfig);
  }

  protected static <T> T readResponse(HttpResponse response, Type type) {
    return GSON.fromJson(response.getResponseBodyAsString(), type);
  }

  protected static <T> T readResponse(HttpResponse response, Type type, Gson gson) {
    return gson.fromJson(response.getResponseBodyAsString(), type);
  }

  protected HttpResponse addAppArtifact(Id.Artifact artifactId, Class<?> cls) throws Exception {

    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, cls, new Manifest());

    try {
      return addArtifact(artifactId, Locations.newInputSupplier(appJar), Collections.emptySet());
    } finally {
      appJar.delete();
    }
  }

  protected HttpResponse addPluginArtifact(Id.Artifact artifactId, Class<?> cls,
                                           Manifest manifest,
                                           Set<ArtifactRange> parents) throws Exception {
    return addPluginArtifact(artifactId, cls, manifest, parents, null);
  }

  /**
   * Adds a plugin artifact. This method is present for testing invalid plugin class json and it usage is not
   * recommended.
   * @param artifactId the artifact id
   * @param cls the application class
   * @param manifest manifest
   * @param parents parents range of artifact
   * @param pluginClassesJSON JSON representation for plugin classes
   * @return {@link HttpResponse} recieved response
   * @throws Exception
   */
  protected HttpResponse addPluginArtifact(Id.Artifact artifactId, Class<?> cls,
                                           Manifest manifest,
                                           @Nullable Set<ArtifactRange> parents,
                                           @Nullable String pluginClassesJSON) throws Exception {
    Location appJar = PluginJarHelper.createPluginJar(locationFactory, manifest, cls);
    try {
      return addArtifact(artifactId, Locations.newInputSupplier(appJar), parents, pluginClassesJSON);
    } finally {
      appJar.delete();
    }
  }

  // add an artifact and return the response code
  protected HttpResponse addArtifact(Id.Artifact artifactId, InputSupplier<? extends InputStream> artifactContents,
                                     Set<ArtifactRange> parents) throws Exception {
    return addArtifact(artifactId, artifactContents, parents, null);
  }

  /**
   * This method accepts GSON serialized form of plugin classes for testing purpose.
   */
  private HttpResponse addArtifact(Id.Artifact artifactId, InputSupplier<? extends InputStream> artifactContents,
                                   Set<ArtifactRange> parents, @Nullable String pluginClassesJSON)
    throws Exception {
    String path = getVersionedAPIPath("artifacts/" + artifactId.getName(), artifactId.getNamespace().getId());
    HttpRequest.Builder builder = HttpRequest.post(getEndPoint(path).toURL())
      .addHeader(Constants.Gateway.API_KEY, "api-key-example")
      .addHeader("Artifact-Version", artifactId.getVersion().getVersion());

    if (parents != null && !parents.isEmpty()) {
      builder.addHeader("Artifact-Extends", Joiner.on('/').join(parents));
    }
    // Note: we purposefully do not check for empty string and let it pass as the header because we want to test the
    // behavior where plugin classes header is set to empty string. This is what is passed by the UI. For more
    // details see: https://issues.cask.co/browse/CDAP-14578
    if (pluginClassesJSON != null) {
      builder.addHeader("Artifact-Plugins", pluginClassesJSON);
    }
    builder.withBody(artifactContents);
    return HttpRequests.execute(builder.build(), httpRequestConfig);
  }

  // add artifact properties and return the response code
  protected HttpResponse addArtifactProperties(Id.Artifact artifactId,
                                               Map<String, String> properties) throws Exception {
    String nonNamespacePath = String.format("artifacts/%s/versions/%s/properties",
                                            artifactId.getName(), artifactId.getVersion());
    String path = getVersionedAPIPath(nonNamespacePath, artifactId.getNamespace().getId());
    HttpRequest request = HttpRequest.put(getEndPoint(path).toURL())
      .withBody(properties.toString())
      .build();
    return HttpRequests.execute(request, httpRequestConfig);
  }

  /**
   * Deploys an application.
   */
  protected HttpResponse deploy(Class<?> application, int expectedCode) throws Exception {
    return deploy(application, expectedCode, null, null);
  }

  protected HttpResponse deploy(Class<?> application, int expectedCode, @Nullable String apiVersion,
                                @Nullable String namespace) throws Exception {
    return deploy(application, expectedCode, apiVersion, namespace, null, null, null);
  }

  protected HttpResponse deploy(Class<?> application, int expectedCode, @Nullable String apiVersion,
                                @Nullable String namespace, @Nullable Config appConfig) throws Exception {
    return deploy(application, expectedCode, apiVersion, namespace, null, appConfig,
                  null);
  }

  protected HttpResponse deploy(Class<?> application, int expectedCode, @Nullable String apiVersion,
                                @Nullable String namespace,
                                @Nullable String ownerPrincipal) throws Exception {
    return deploy(application, expectedCode, apiVersion, namespace, null, null, ownerPrincipal);
  }

  protected HttpResponse deploy(Id.Application appId,
                                AppRequest<?> appRequest) throws Exception {
    String deployPath = getVersionedAPIPath("apps/" + appId.getId(), appId.getNamespaceId());
    return executeDeploy(HttpRequest.put(getEndPoint(deployPath).toURL()), appRequest);
  }

  protected HttpResponse deploy(ApplicationId appId, AppRequest<? extends Config> appRequest) throws Exception {
    String deployPath = getVersionedAPIPath(String.format("apps/%s/versions/%s/create", appId.getApplication(),
                                                          appId.getVersion()),
                                            appId.getNamespace());
    return executeDeploy(HttpRequest.post(getEndPoint(deployPath).toURL()), appRequest);
  }

  private HttpResponse executeDeploy(HttpRequest.Builder requestBuilder,
                                     AppRequest<?> appRequest) throws Exception {
    requestBuilder.addHeader(Constants.Gateway.API_KEY, "api-key-example");
    requestBuilder.addHeader(HttpHeaderNames.CONTENT_TYPE.toString(), MediaType.APPLICATION_JSON);
    requestBuilder.withBody(GSON.toJson(appRequest));
    return HttpRequests.execute(requestBuilder.build(), httpRequestConfig);
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
    File expandDir = tmpFolder.newFolder();
    BundleJarUtil.unJar(Locations.toLocation(artifactJar), expandDir);

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
      Assert.fail(
        String.format("Expected response code %d but got %d when trying to deploy app '%s' in namespace '%s'. " +
                        "Response message = '%s'", expectedCode, response.getResponseCode(),
                      application.getName(), namespace, response.getResponseMessage()));
    }
    return response;
  }

  protected String getVersionedAPIPath(String nonVersionedApiPath, String namespace) {
    return getVersionedAPIPath(nonVersionedApiPath, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
  }

  protected String getVersionedAPIPath(String nonVersionedApiPath, String version, String namespace) {
    if (!Constants.Gateway.API_VERSION_3_TOKEN.equals(version)) {
      throw new IllegalArgumentException(
        String.format("Unsupported version '%s'. Only v3 is supported.", version));
    }
    Preconditions.checkArgument(namespace != null, "Namespace cannot be null for v3 APIs.");
    return String.format("/%s/namespaces/%s/%s", version, namespace, nonVersionedApiPath);
  }

  protected List<JsonObject> getAppList(String namespace) throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, namespace));
    Assert.assertEquals(200, response.getResponseCode());
    return readResponse(response, LIST_JSON_OBJECT_TYPE);
  }

  /**
   * Gets a list of {@link BatchApplicationDetail} from the give set of application version
   *
   * @param namespace the namespace to read from
   * @param appVersions list of appId and version pair.
   */
  protected List<BatchApplicationDetail> getAppDetails(String namespace,
                                                       Collection<ImmutablePair<String, String>> appVersions)
    throws Exception {
    List<Map<String, String>> request = appVersions.stream()
      .map(e -> (e.getSecond() == null)
        ? Collections.singletonMap("appId", e.getFirst())
        : ImmutableMap.of("appId", e.getFirst(), "version", e.getSecond()))
      .collect(Collectors.toList());
    HttpResponse response = doPost(getVersionedAPIPath("appdetail", Constants.Gateway.API_VERSION_3_TOKEN, namespace),
                                   GSON.toJson(request));
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("application/json", getFirstHeaderValue(response, HttpHeaderNames.CONTENT_TYPE.toString()));
    return readResponse(response, new TypeToken<List<BatchApplicationDetail>>() { }.getType());
  }

  protected JsonObject getAppDetails(String namespace, String appName) throws Exception {
    HttpResponse response = getAppResponse(namespace, appName);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("application/json", getFirstHeaderValue(response, HttpHeaderNames.CONTENT_TYPE.toString()));
    return readResponse(response, JsonObject.class);
  }

  protected HttpResponse getAppResponse(String namespace, String appName) throws Exception {
    return doGet(getVersionedAPIPath(String.format("apps/%s", appName),
                                     Constants.Gateway.API_VERSION_3_TOKEN, namespace));
  }

  protected HttpResponse getAppResponse(String namespace, String appName, String appVersion) throws Exception {
    return doGet(getVersionedAPIPath(String.format("apps/%s/versions/%s", appName, appVersion),
                                     Constants.Gateway.API_VERSION_3_TOKEN, namespace));
  }

  protected Set<String> getAppVersions(String namespace, String appName) throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath(String.format("apps/%s/versions", appName),
                                                      Constants.Gateway.API_VERSION_3_TOKEN, namespace));
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("application/json", getFirstHeaderValue(response, HttpHeaderNames.CONTENT_TYPE.toString()));
    return readResponse(response, SET_TRING_TYPE);
  }

  protected JsonObject getAppDetails(String namespace, String appName, String appVersion) throws Exception {
    HttpResponse response = getAppResponse(namespace, appName, appVersion);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("application/json", getFirstHeaderValue(response, HttpHeaderNames.CONTENT_TYPE.toString()));
    return readResponse(response, JsonObject.class);
  }

  /**
   * Checks the given schedule states.
   */
  protected void assertSchedule(final Id.Program program, final String scheduleName,
                                boolean scheduled, long timeout, TimeUnit timeoutUnit) throws Exception {
    Tasks.waitFor(scheduled, () -> {
      String statusURL = getVersionedAPIPath(String.format("apps/%s/schedules/%s/status",
                                                           program.getApplicationId(), scheduleName),
                                             Constants.Gateway.API_VERSION_3_TOKEN, program.getNamespaceId());
      HttpResponse response = doGet(statusURL);
      Preconditions.checkState(200 == response.getResponseCode());
      Map<String, String> result = GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
      return result != null && "SCHEDULED".equals(result.get("status"));
    }, timeout, timeoutUnit, 100, TimeUnit.MILLISECONDS);
  }

  protected void deleteApp(Id.Application app, int expectedResponseCode) throws Exception {
    HttpResponse response = doDelete(getVersionedAPIPath("apps/" + app.getId(), app.getNamespaceId()));
    Assert.assertEquals(expectedResponseCode, response.getResponseCode());
  }

  protected void deleteApp(ApplicationId app, int expectedResponseCode) throws Exception {
    HttpResponse response = doDelete(getVersionedAPIPath(
      String.format("/apps/%s/versions/%s", app.getApplication(), app.getVersion()), app.getNamespace()));
    Assert.assertEquals(expectedResponseCode, response.getResponseCode());
  }

  protected void deleteApp(final Id.Application app, int expectedResponseCode,
                           long timeout, TimeUnit timeoutUnit) throws Exception {
    String apiPath = getVersionedAPIPath("apps/" + app.getId(), app.getNamespaceId());
    Tasks.waitFor(expectedResponseCode, () -> doDelete(apiPath).getResponseCode(),
                  timeout, timeoutUnit, 100, TimeUnit.MILLISECONDS);
  }

  protected void deleteArtifact(Id.Artifact artifact, int expectedResponseCode) throws Exception {
    String path = String.format("artifacts/%s/versions/%s", artifact.getName(), artifact.getVersion().getVersion());
    HttpResponse response = doDelete(getVersionedAPIPath(path, artifact.getNamespace().getId()));
    Assert.assertEquals(expectedResponseCode, response.getResponseCode());
  }

  protected List<Profile> listProfiles(NamespaceId namespace,
                                       boolean includeSystem, int expectedCode) throws Exception {
    HttpResponse response = doGet(String.format("/v3/namespaces/%s/profiles?includeSystem=%s",
                                                namespace.getNamespace(), includeSystem));
    Assert.assertEquals(expectedCode, response.getResponseCode());
    if (expectedCode == HttpResponseStatus.OK.code()) {
      return GSON.fromJson(response.getResponseBodyAsString(), LIST_PROFILE);
    }
    return Collections.emptyList();
  }

  protected List<Profile> listSystemProfiles(int expectedCode) throws Exception {
    HttpResponse response = doGet("/v3/profiles");
    Assert.assertEquals(expectedCode, response.getResponseCode());
    if (expectedCode == HttpResponseStatus.OK.code()) {
      return GSON.fromJson(response.getResponseBodyAsString(), LIST_PROFILE);
    }
    return Collections.emptyList();
  }

  protected Optional<Profile> getProfile(ProfileId profileId, int expectedCode) throws Exception {
    HttpResponse response = doGet(String.format("/v3/namespaces/%s/profiles/%s",
                                                profileId.getNamespace(), profileId.getProfile()));
    Assert.assertEquals(expectedCode, response.getResponseCode());
    if (expectedCode == HttpResponseStatus.OK.code()) {
      return Optional.of(GSON.fromJson(response.getResponseBodyAsString(), Profile.class));
    }
    return Optional.empty();
  }

  protected Optional<Profile> getSystemProfile(String profileName, int expectedCode) throws Exception {
    HttpResponse response = doGet(String.format("/v3/profiles/%s", profileName));
    Assert.assertEquals(expectedCode, response.getResponseCode());
    if (expectedCode == HttpResponseStatus.OK.code()) {
      return Optional.of(GSON.fromJson(response.getResponseBodyAsString(), Profile.class));
    }
    return Optional.empty();
  }

  protected void putProfile(ProfileId profileId, Object profile,
                            int expectedCode) throws Exception {
    HttpResponse response = doPut(String.format("/v3/namespaces/%s/profiles/%s",
                                                profileId.getNamespace(),
                                                profileId.getProfile()), GSON.toJson(profile));
    Assert.assertEquals(expectedCode, response.getResponseCode());
  }

  protected void putSystemProfile(String profileName, Object profile, int expectedCode) throws Exception {
    HttpResponse response = doPut(String.format("/v3/profiles/%s", profileName), GSON.toJson(profile));
    Assert.assertEquals(expectedCode, response.getResponseCode());
  }

  protected void deleteProfile(ProfileId profileId, int expectedCode) throws Exception {
    HttpResponse response = doDelete(String.format("/v3/namespaces/%s/profiles/%s",
                                                   profileId.getNamespace(), profileId.getProfile()));
    Assert.assertEquals(expectedCode, response.getResponseCode());
  }

  protected void deleteSystemProfile(String profileName, int expectedCode) throws Exception {
    HttpResponse response = doDelete(String.format("/v3/profiles/%s", profileName));
    Assert.assertEquals(expectedCode, response.getResponseCode());
  }

  protected Optional<ProfileStatus> getProfileStatus(ProfileId profileId, int expectedCode) throws Exception {
    HttpResponse response = doGet(String.format("/v3/namespaces/%s/profiles/%s/status",
                                                profileId.getNamespace(), profileId.getProfile()));
    Assert.assertEquals(expectedCode, response.getResponseCode());
    if (expectedCode == HttpResponseStatus.OK.code()) {
      return Optional.of(GSON.fromJson(response.getResponseBodyAsString(), ProfileStatus.class));
    }
    return Optional.empty();
  }

  protected void enableProfile(ProfileId profileId, int expectedCode) throws Exception {
    HttpResponse response = doPost(String.format("/v3/namespaces/%s/profiles/%s/enable", profileId.getNamespace(),
                                                 profileId.getProfile()));
    Assert.assertEquals(expectedCode, response.getResponseCode());
  }

  protected void disableProfile(ProfileId profileId, int expectedCode) throws Exception {
    HttpResponse response = doPost(String.format("/v3/namespaces/%s/profiles/%s/disable",
                                                 profileId.getNamespace(), profileId.getProfile()));
    Assert.assertEquals(expectedCode, response.getResponseCode());
  }

  protected void disableSystemProfile(String profileName, int expectedCode) throws Exception {
    HttpResponse response = doPost(String.format("/v3/profiles/%s/disable", profileName));
    Assert.assertEquals(expectedCode, response.getResponseCode());
  }

  /**
   * Starts the given program.
   */
  protected void startProgram(Id.Program program) throws Exception {
    startProgram(program, 200);
  }

  /**
   * Starts the given program.
   */
  protected void startProgram(ProgramId program) throws Exception {
    startProgram(program, 200);
  }

  /**
   * Tries to start the given program and expect the call completed with the status.
   */
  protected void startProgram(Id.Program program, int expectedStatusCode) throws Exception {
    startProgram(program, Collections.emptyMap(), expectedStatusCode);
  }

  /**
   * Starts the given program with the given runtime arguments.
   */
  protected void startProgram(Id.Program program, Map<String, String> args) throws Exception {
    startProgram(program, args, 200);
  }

  /**
   * Tries to start the given program with the given runtime arguments and expect the call completed with the status.
   */
  protected void startProgram(Id.Program program, Map<String, String> args, int expectedStatusCode) throws Exception {
    String path = String.format("apps/%s/%s/%s/start",
                                program.getApplicationId(),
                                program.getType().getCategoryName(),
                                program.getId());
    startProgram(path, program.getNamespaceId(), args, expectedStatusCode);
  }

  /**
   * Tries to start the given program with the given runtime arguments and expect the call completed with the status.
   */
  protected void startProgram(ProgramId program, Map<String, String> args, int expectedStatusCode) throws Exception {
    String path = String.format("apps/%s/versions/%s/%s/%s/start",
                                program.getApplication(),
                                program.getVersion(),
                                program.getType().getCategoryName(),
                                program.getProgram());
    startProgram(path, program.getNamespace(), args, expectedStatusCode);
  }

  /**
   * Tries to start the given program with the given runtime arguments and expect the call completed with the status.
   */
  protected void startProgram(ProgramId program, int expectedStatusCode) throws Exception {
    startProgram(program, Collections.emptyMap(), expectedStatusCode);
  }

  /**
   * Tries to start the given program with the given runtime arguments and expect the call completed with the status.
   */
  protected void startProgram(String path, String namespaceId, Map<String, String> args,
                              int expectedStatusCode) throws Exception {
    HttpResponse response = doPost(getVersionedAPIPath(path, namespaceId), GSON.toJson(args));
    Assert.assertEquals(expectedStatusCode, response.getResponseCode());
  }

  /**
   * Tries to restart programs in the application which were stopped between start and end.
   */
  protected int restartPrograms(ApplicationId applicationId, long startTimeSeconds, long endTimeSeconds)
    throws Exception {
    String path =
      String.format("apps/%s/versions/%s/restart-programs?start-time-seconds=%d&end-time-seconds=%d",
                    applicationId.getApplication(), applicationId.getVersion(), startTimeSeconds, endTimeSeconds);
    String versionedPath = getVersionedAPIPath(path, Constants.Gateway.API_VERSION_3_TOKEN,
                                                           applicationId.getNamespace());
    HttpResponse response = doPut(versionedPath, null);
    return response.getResponseCode();
  }

  /**
   * Tries to start the given program with the given runtime arguments and expect the call completed with the status.
   */
  protected void debugProgram(Id.Program program, int expectedStatusCode) throws Exception {
    String path = String.format("apps/%s/%s/%s/debug",
                                program.getApplicationId(),
                                program.getType().getCategoryName(),
                                program.getId());
    HttpResponse response = doPost(getVersionedAPIPath(path, program.getNamespaceId()),
                                   GSON.toJson(ImmutableMap.<String, String>of()));
    Assert.assertEquals(expectedStatusCode, response.getResponseCode());
  }

  /**
   * Stops the given program.
   */
  protected void stopProgram(Id.Program program) throws Exception {
    stopProgram(program, 200);
  }

  /**
   * Stops the given program.
   */
  protected void stopProgram(ProgramId program) throws Exception {
    stopProgram(program, null, 200, null);
  }

  /**
   * Tries to stop the given program and expect the call completed with the status.
   */
  protected void stopProgram(Id.Program program, int expectedStatusCode) throws Exception {
    stopProgram(program, null, expectedStatusCode);
  }

  protected void stopProgram(Id.Program program, String runId, int expectedStatusCode) throws Exception {
    stopProgram(program, runId, expectedStatusCode, null);
  }

  protected void stopProgram(Id.Program program, String runId, int expectedStatusCode,
                             String expectedMessage) throws Exception {
    String path;
    if (runId == null) {
      path = String.format("apps/%s/%s/%s/stop", program.getApplicationId(), program.getType().getCategoryName(),
                           program.getId());
    } else {
      path = String.format("apps/%s/%s/%s/runs/%s/stop", program.getApplicationId(),
                           program.getType().getCategoryName(), program.getId(), runId);
    }
    stopProgram(path, program.getNamespaceId(), expectedStatusCode, expectedMessage);
  }

  protected void stopProgram(ProgramId program, @Nullable String runId, int expectedStatusCode,
                             String expectedMessage) throws Exception {
    String path;
    if (runId == null) {
      path = String.format("apps/%s/versions/%s/%s/%s/stop", program.getApplication(), program.getVersion(),
                           program.getType().getCategoryName(), program.getProgram());
    } else {
      // TODO: HTTP endpoint for stopping a program run of an app version not implemented
      path = null;
    }
    stopProgram(path, program.getNamespace(), expectedStatusCode, expectedMessage);
  }

  private void stopProgram(String path, String namespaceId, int expectedStatusCode,
                           String expectedMessage) throws Exception {

    HttpResponse response = doPost(getVersionedAPIPath(path, namespaceId));
    Assert.assertEquals(expectedStatusCode, response.getResponseCode());
    if (expectedMessage != null) {
      Assert.assertEquals(expectedMessage, response.getResponseBodyAsString());
    }
  }

  /**
   * Waits for the given program to transit to the given state.
   */
  protected void waitState(final Id.Program programId, String state) throws Exception {
    Tasks.waitFor(state, () -> {
      String path = String.format("apps/%s/%s/%s/status",
                                  programId.getApplicationId(),
                                  programId.getType().getCategoryName(), programId.getId());
      HttpResponse response = doGet(getVersionedAPIPath(path, programId.getNamespaceId()));
      if (response.getResponseCode() == 404) {
        return null;
      }
      JsonObject status = GSON.fromJson(response.getResponseBodyAsString(), JsonObject.class);
      if (status == null || !status.has("status")) {
        return null;
      }
      return status.get("status").getAsString();
    }, 60, TimeUnit.SECONDS);
  }

  /**
   * Waits for the given program to transit to the given state.
   */
  protected void waitState(final ProgramId programId, String state) throws Exception {
    Tasks.waitFor(state, () -> {
      String path = String.format("apps/%s/versions/%s/%s/%s/status",
                                  programId.getApplication(),
                                  programId.getVersion(),
                                  programId.getType().getCategoryName(), programId.getProgram());
      HttpResponse response = doGet(getVersionedAPIPath(path, programId.getNamespace()));
      if (response.getResponseCode() == 404) {
        return null;
      }
      JsonObject status = GSON.fromJson(response.getResponseBodyAsString(), JsonObject.class);
      if (status == null || !status.has("status")) {
        return null;
      }
      return status.get("status").getAsString();
    }, 60, TimeUnit.SECONDS);
  }

  private static void createNamespaces() throws Exception {
    HttpResponse response = doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, TEST_NAMESPACE1),
                                  GSON.toJson(TEST_NAMESPACE_META1));
    Assert.assertEquals(200, response.getResponseCode());

    response = doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, TEST_NAMESPACE2),
                     GSON.toJson(TEST_NAMESPACE_META2));
    Assert.assertEquals(200, response.getResponseCode());
  }

  private static void deleteNamespaces() throws Exception {
    Tasks.waitFor(200, () ->
                    doDelete(String.format("%s/unrecoverable/namespaces/%s",
                                           Constants.Gateway.API_VERSION_3, TEST_NAMESPACE1)).getResponseCode(),
                  10, TimeUnit.SECONDS);

    Tasks.waitFor(200, () ->
                    doDelete(String.format("%s/unrecoverable/namespaces/%s",
                                           Constants.Gateway.API_VERSION_3, TEST_NAMESPACE2)).getResponseCode(),
                  10, TimeUnit.SECONDS);
  }

  protected String getProgramStatus(Id.Program program) throws Exception {
    return getStatus(programStatus(program));
  }

  protected void programStatus(Id.Program program, int expectedStatus) throws Exception {
    Assert.assertEquals(expectedStatus, programStatus(program).getResponseCode());
  }

  private HttpResponse programStatus(Id.Program program) throws Exception {
    String path = String.format("apps/%s/%s/%s/status",
                                program.getApplicationId(),
                                program.getType().getCategoryName(),
                                program.getId());
    return doGet(getVersionedAPIPath(path, program.getNamespaceId()));
  }

  /**
   * Waits for the given program to transit to the given state.
   */
  protected String getProgramStatus(final ProgramId programId) throws Exception {

    String path = String.format("apps/%s/versions/%s/%s/%s/status",
                                programId.getApplication(),
                                programId.getVersion(),
                                programId.getType().getCategoryName(), programId.getProgram());
    HttpResponse response = doGet(getVersionedAPIPath(path, programId.getNamespace()));
    Assert.assertEquals(200, response.getResponseCode());
    Map<String, String> o = GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
    return o.get("status");

  }

  private String getStatus(HttpResponse response) {
    Assert.assertEquals(200, response.getResponseCode());
    Map<String, String> o = GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
    return o.get("status");
  }

  protected int reEnableSchedules(String namespace, long startTimeMillis, long endTimeMillis) throws Exception {
    String scheduleSuspend =
      String.format("schedules/re-enable?start-time-millis=%d&end-time-millis=%d", startTimeMillis, endTimeMillis);
    String versionedScheduledSuspend = getVersionedAPIPath(scheduleSuspend, Constants.Gateway.API_VERSION_3_TOKEN,
                                                           namespace);
    HttpResponse response = doPut(versionedScheduledSuspend, null);
    return response.getResponseCode();
  }

  protected int suspendSchedule(String namespace, String appName, String schedule) throws Exception {
    String scheduleSuspend = String.format("apps/%s/schedules/%s/suspend", appName, schedule);
    String versionedScheduledSuspend = getVersionedAPIPath(scheduleSuspend, Constants.Gateway.API_VERSION_3_TOKEN,
                                                           namespace);
    HttpResponse response = doPost(versionedScheduledSuspend);
    return response.getResponseCode();
  }

  protected int resumeSchedule(String namespace, String appName, String schedule) throws Exception {
    String scheduleResume = String.format("apps/%s/schedules/%s/resume", appName, schedule);
    HttpResponse response = doPost(getVersionedAPIPath(scheduleResume, Constants.Gateway.API_VERSION_3_TOKEN,
                                                       namespace));
    return response.getResponseCode();
  }

  protected List<ScheduleDetail> listSchedules(String namespace, String appName,
                                               @Nullable String appVersion) throws Exception {
    String schedulesUrl = String.format("apps/%s/versions/%s/schedules", appName,
                                        appVersion == null ? ApplicationId.DEFAULT_VERSION : appVersion);
    return doGetSchedules(namespace, schedulesUrl);
  }

  protected List<ScheduleDetail> listSchedulesByTriggerProgram(String namespace, ProgramId programId,
                                                               ProgramStatus... programStatuses)
    throws Exception {

    return listSchedulesByTriggerProgram(namespace, programId, null, programStatuses);
  }

  protected List<ScheduleDetail> listSchedulesByTriggerProgram(String namespace, ProgramId programId,
                                                               @Nullable ProgramScheduleStatus scheduleStatus,
                                                               ProgramStatus... programStatuses)
    throws Exception {
    String schedulesUrl = String.format("schedules/trigger-type/program-status?trigger-namespace-id=%s" +
                                          "&trigger-app-name=%s&trigger-app-version=%s" +
                                          "&trigger-program-type=%s&trigger-program-name=%s", programId.getNamespace(),
                                        programId.getApplication(), programId.getVersion(),
                                        programId.getType().getCategoryName(), programId.getProgram());
    if (programStatuses.length > 0) {
      List<String> statusNames = Arrays.stream(programStatuses).map(Enum::name).collect(Collectors.toList());
      schedulesUrl = schedulesUrl + "&trigger-program-statuses=" + Joiner.on(",").join(statusNames);
    }
    if (scheduleStatus != null) {
      schedulesUrl = schedulesUrl + "&schedule-status=" + scheduleStatus;
    }
    return doGetSchedules(namespace, schedulesUrl);
  }

  protected List<ScheduleDetail> getSchedules(String namespace, String appName,
                                              String workflowName) throws Exception {
    String schedulesUrl = String.format("apps/%s/workflows/%s/schedules", appName, workflowName);
    return doGetSchedules(namespace, schedulesUrl);
  }

  protected List<ScheduleDetail> getSchedules(String namespace, String appName, String appVersion,
                                              String workflowName) throws Exception {
    String schedulesUrl = String.format("apps/%s/versions/%s/workflows/%s/schedules", appName, appVersion,
                                        workflowName);
    return doGetSchedules(namespace, schedulesUrl);
  }

  protected List<ScheduleDetail> getSchedules(String namespace, String appName,
                                              String workflowName, ProtoTrigger.Type type) throws Exception {
    String schedulesUrl = String.format("apps/%s/workflows/%s/schedules?trigger-type=%s",
                                        appName, workflowName, type.getCategoryName());
    return doGetSchedules(namespace, schedulesUrl);
  }

  protected List<ScheduleDetail> getSchedules(String namespace, String appName, String appVersion,
                                              String workflowName, ProtoTrigger.Type type) throws Exception {
    String schedulesUrl = String.format("apps/%s/versions/%s/workflows/%s/schedules?trigger-type=%s",
                                        appName, appVersion, workflowName, type.getCategoryName());
    return doGetSchedules(namespace, schedulesUrl);
  }

  protected List<ScheduleDetail> getSchedules(String namespace, String appName, String appVersion,
                                              String workflowName, ProgramScheduleStatus status) throws Exception {
    String schedulesUrl = String.format("apps/%s/versions/%s/workflows/%s/schedules?schedule-status=%s",
                                        appName, appVersion, workflowName, status.name());
    return doGetSchedules(namespace, schedulesUrl);
  }

  private List<ScheduleDetail> doGetSchedules(String namespace, String schedulesUrl) throws Exception {
    String versionedUrl = getVersionedAPIPath(schedulesUrl, namespace);
    HttpResponse response = doGet(versionedUrl);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    return readResponse(response, Schedulers.SCHEDULE_DETAILS_TYPE);
  }

  protected HttpResponse addSchedule(String namespace, String appName, @Nullable String appVersion, String scheduleName,
                                     ScheduleDetail schedule) throws Exception {
    appVersion = appVersion == null ? ApplicationId.DEFAULT_VERSION : appVersion;
    String path = String.format("apps/%s/versions/%s/schedules/%s", appName, appVersion, scheduleName);
    return doPut(getVersionedAPIPath(path, namespace), GSON.toJson(schedule));
  }

  protected HttpResponse enableSchedule(String namespace, String appName,
                                        @Nullable String appVersion, String scheduleName) throws Exception {
    appVersion = appVersion == null ? ApplicationId.DEFAULT_VERSION : appVersion;
    String path = String.format("apps/%s/versions/%s/schedules/%s/enable", appName, appVersion, scheduleName);
    return doPost(getVersionedAPIPath(path, namespace));
  }

  protected HttpResponse deleteSchedule(String namespace, String appName, @Nullable String appVersion,
                                        String scheduleName) throws Exception {
    appVersion = appVersion == null ? ApplicationId.DEFAULT_VERSION : appVersion;
    String path = String.format("apps/%s/versions/%s/schedules/%s", appName, appVersion, scheduleName);
    return doDelete(getVersionedAPIPath(path, namespace));
  }

  protected HttpResponse updateSchedule(String namespace, String appName, @Nullable String appVersion,
                                        String scheduleName,
                                        ScheduleDetail scheduleDetail) throws Exception {
    appVersion = appVersion == null ? ApplicationId.DEFAULT_VERSION : appVersion;
    String path = String.format("apps/%s/versions/%s/schedules/%s/update", appName, appVersion, scheduleName);
    return doPost(getVersionedAPIPath(path, namespace), GSON.toJson(scheduleDetail));
  }

  protected ScheduleDetail getSchedule(String namespace, String appName, @Nullable String appVersion,
                                       String scheduleName) throws Exception {
    appVersion = appVersion == null ? ApplicationId.DEFAULT_VERSION : appVersion;
    String path = String.format("apps/%s/versions/%s/schedules/%s", appName, appVersion, scheduleName);
    HttpResponse response = doGet(getVersionedAPIPath(path, namespace));
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    return readResponse(response, ScheduleDetail.class);
  }

  /**
   * Returns a list of {@link ScheduledRuntime}.
   *
   * @param programId the program id
   * @param next if true, fetch the list of future run times. If false, fetch the list of past run times.
   */
  protected List<ScheduledRuntime> getScheduledRunTimes(ProgramId programId, boolean next) throws Exception {
    String nextRunTimeUrl = String.format("apps/%s/workflows/%s/%sruntime", programId.getApplication(),
                                          programId.getProgram(), next ? "next" : "previous");
    String versionedUrl = getVersionedAPIPath(nextRunTimeUrl, Constants.Gateway.API_VERSION_3_TOKEN,
                                              programId.getNamespace());
    HttpResponse response = doGet(versionedUrl);
    Assert.assertEquals(200, response.getResponseCode());
    return readResponse(response, new TypeToken<List<ScheduledRuntime>>() { }.getType());
  }

  /**
   * Returns a list of {@link BatchProgramSchedule}.
   *
   * @param namespace the namespace to query in
   * @param programIds list of programs to query for scheuled run time
   * @param next if true, fetch the list of future run times. If false, fetch the list of past run times.
   * @return a list of {@link BatchProgramSchedule}
   */
  protected List<BatchProgramSchedule> getScheduledRunTimes(String namespace,
                                                            Collection<? extends ProgramId> programIds,
                                                            boolean next) throws Exception {
    Assert.assertTrue(programIds.stream().map(ProgramId::getNamespace).allMatch(namespace::equals));

    String url = String.format("%sruntime", next ? "next" : "previous");
    String versionedUrl = getVersionedAPIPath(url, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    List<BatchProgram> batchPrograms = programIds.stream()
      .map(id -> new BatchProgram(id.getApplication(), id.getType(), id.getProgram()))
      .collect(Collectors.toList());
    HttpResponse response = doPost(versionedUrl, GSON.toJson(batchPrograms));
    Assert.assertEquals(200, response.getResponseCode());
    return readResponse(response, new TypeToken<List<BatchProgramSchedule>>() { }.getType());
  }


  protected Map<String, String> getMetadataProperties(EntityId entityId) throws Exception {
    return metadataClient.getProperties(entityId.toMetadataEntity());
  }

  protected Map<String, String> getMetadataProperties(MetadataEntity entity, MetadataScope scope) throws Exception {
    return metadataClient.getProperties(entity, scope);
  }

  protected Set<String> getMetadataTags(MetadataEntity entity, MetadataScope scope) throws Exception {
    return metadataClient.getTags(entity, scope);
  }

  protected void createMetadataMutation(MetadataMutation.Create createMutation) {
    metadataServiceClient.create(createMutation);
  }

  protected void updateMetadataMutation(MetadataMutation.Update updateMutation) {
    metadataServiceClient.update(updateMutation);
  }

  protected void dropMetadataMutation(MetadataMutation.Drop dropMutation) {
    metadataServiceClient.drop(dropMutation);
  }

  protected void removeMetadataMutation(MetadataMutation.Remove removeMutation) {
    metadataServiceClient.remove(removeMutation);
  }

  protected void verifyNoRunWithStatus(final Id.Program program, final ProgramRunStatus status) throws Exception {
    Tasks.waitFor(0, () -> getProgramRuns(program, status).size(), 60, TimeUnit.SECONDS);
  }

  protected void verifyProgramRuns(Id.Program program, ProgramRunStatus status) throws Exception {
    verifyProgramRuns(program, status, 0);
  }

  protected void verifyProgramRuns(final Id.Program program, final ProgramRunStatus status, final int expected)
    throws Exception {
    Tasks.waitFor(true, () -> getProgramRuns(program, status).size() > expected, 60, TimeUnit.SECONDS);
  }

  protected List<BatchProgramHistory> getProgramRuns(NamespaceId namespace, List<ProgramId> programs) throws Exception {
    List<BatchProgram> request = programs.stream()
      .map(program -> new BatchProgram(program.getApplication(), program.getType(), program.getProgram()))
      .collect(Collectors.toList());

    HttpResponse response = doPost(getVersionedAPIPath("runs", namespace.getNamespace()), GSON.toJson(request));
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), BATCH_PROGRAM_RUNS_TYPE);
  }

  protected List<RunRecord> getProgramRuns(Id.Program program, ProgramRunStatus status) throws Exception {
    String path = String.format("apps/%s/%s/%s/runs?status=%s", program.getApplicationId(),
                                program.getType().getCategoryName(), program.getId(), status.name());
    HttpResponse response = doGet(getVersionedAPIPath(path, program.getNamespaceId()));
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_RUNRECORD_TYPE);
  }

  protected void verifyProgramRuns(final ProgramId program, ProgramRunStatus status) throws Exception {
    verifyProgramRuns(program, status, 0);
  }

  private void verifyProgramRuns(ProgramId program, ProgramRunStatus status, int expected) throws Exception {
    Tasks.waitFor(true, () -> getProgramRuns(program, status).size() > expected, 60, TimeUnit.SECONDS);
  }

  protected void assertProgramRuns(final ProgramId program, final ProgramRunStatus status, final int expected)
    throws Exception {
    Tasks.waitFor(true, () -> getProgramRuns(program, status).size() == expected, 15, TimeUnit.SECONDS);
  }

  protected List<RunRecord> getProgramRuns(ProgramId program, ProgramRunStatus status) throws Exception {
    String path = String.format("apps/%s/versions/%s/%s/%s/runs?status=%s", program.getApplication(),
                                program.getVersion(), program.getType().getCategoryName(), program.getProgram(),
                                status.toString());
    HttpResponse response = doGet(getVersionedAPIPath(path, program.getNamespace()));
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), LIST_RUNRECORD_TYPE);
  }

  protected HttpResponse createNamespace(String id) throws Exception {
    return doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, id), null);
  }

  protected HttpResponse deleteNamespace(String name) throws Exception {
    return doDelete(String.format("%s/unrecoverable/namespaces/%s", Constants.Gateway.API_VERSION_3, name));
  }

  protected HttpResponse createNamespace(String metadata, String id) throws Exception {
    return doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, id), metadata);
  }

  protected HttpResponse listAllNamespaces() throws Exception {
    return doGet(String.format("%s/namespaces", Constants.Gateway.API_VERSION_3));
  }

  protected HttpResponse getNamespace(String name) throws Exception {
    Preconditions.checkArgument(name != null, "namespace name cannot be null");
    return doGet(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, name));
  }

  protected HttpResponse deleteNamespaceData(String name) throws Exception {
    return doDelete(String.format("%s/unrecoverable/namespaces/%s/datasets", Constants.Gateway.API_VERSION_3, name));
  }

  protected HttpResponse setProperties(String id, NamespaceMeta meta) throws Exception {
    return doPut(String.format("%s/namespaces/%s/properties", Constants.Gateway.API_VERSION_3, id),
                 GSON.toJson(meta));
  }

  protected String getPreferenceURI() {
    return "";
  }

  protected String getPreferenceURI(String namespace) {
    return String.format("%s/namespaces/%s", getPreferenceURI(), namespace);
  }

  protected String getPreferenceURI(String namespace, String appId) {
    return String.format("%s/apps/%s", getPreferenceURI(namespace), appId);
  }

  protected String getPreferenceURI(String namespace, String appId, String programType, String programId) {
    return String.format("%s/%s/%s", getPreferenceURI(namespace, appId), programType, programId);
  }

  protected void setPreferences(String uri, Map<String, String> props, int expectedStatus) throws Exception {
    HttpResponse response = doPut(String.format("/v3/%s/preferences", uri), GSON.toJson(props));
    Assert.assertEquals(expectedStatus, response.getResponseCode());
  }

  protected Map<String, String> getPreferences(String uri, boolean resolved, int expectedStatus) throws Exception {
    String request = String.format("/v3/%s/preferences", uri);
    if (resolved) {
      request += "?resolved=true";
    }
    HttpResponse response = doGet(request);
    Assert.assertEquals(expectedStatus, response.getResponseCode());
    if (expectedStatus == 200) {
      return GSON.fromJson(response.getResponseBodyAsString(), MAP_STRING_STRING_TYPE);
    }
    return null;
  }

  protected PreferencesDetail getPreferencesInternal(String uri, boolean resolved,
                                                     HttpResponseStatus expectedResponseStatus) throws Exception {
    String url = String.format("%s/%s/preferences", Constants.Gateway.INTERNAL_API_VERSION_3, uri);
    if (resolved) {
      url += "?resolved=true";
    }
    HttpResponse response = doGet(url);
    Assert.assertEquals(expectedResponseStatus.code(), response.getResponseCode());
    if (expectedResponseStatus != HttpResponseStatus.OK) {
      return null;
    }
    return GSON.fromJson(response.getResponseBodyAsString(), PreferencesDetail.class);
  }


  protected void deletePreferences(String uri, int expectedStatus) throws Exception {
    HttpResponse response = doDelete(String.format("/v3/%s/preferences", uri));
    Assert.assertEquals(expectedStatus, response.getResponseCode());
  }

  protected File buildAppArtifact(Class<?> cls, String name) throws IOException {
    return buildAppArtifact(cls, name, new Manifest());
  }

  private File buildAppArtifact(Class<?> cls, String name, Manifest manifest) throws IOException {
    if (!name.endsWith(".jar")) {
      name += ".jar";
    }
    File destination = new File(tmpFolder.newFolder(), name);
    return buildAppArtifact(cls, manifest, destination);
  }

  protected File buildAppArtifact(Class<?> cls, Manifest manifest, File destination) throws IOException {
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, cls, manifest);
    Files.copy(Locations.newInputSupplier(appJar), destination);
    return destination;
  }

  protected DatasetMeta getDatasetMeta(DatasetId datasetId)
    throws UnauthorizedException, UnauthenticatedException, NotFoundException, IOException {
    return datasetClient.get(datasetId);
  }

  protected long getProfileTotalMetric(String metricName) {
    Map<String, String> tags = new HashMap<>();
    tags.put(Constants.Metrics.Tag.PROFILE, Profile.NATIVE_NAME);
    tags.put(Constants.Metrics.Tag.PROFILE_SCOPE, EntityScope.SYSTEM.name());
    MetricDataQuery query =
      new MetricDataQuery(0, 0, Integer.MAX_VALUE, "system." + metricName,
                          AggregationFunction.SUM, tags, Collections.emptyList());
    Collection<MetricTimeSeries> results = metricStore.query(query);
    if (results.isEmpty()) {
      return 0;
    }
    // since it is totals query and not groupBy specified, we know there's one time series
    List<TimeValue> timeValues = results.iterator().next().getTimeValues();
    if (timeValues.isEmpty()) {
      return 0;
    }

    // since it is totals, we know there's one value only
    return timeValues.get(0).getValue();
  }

  private static ClientConfig getClientConfig(DiscoveryServiceClient discoveryClient, String service) {
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(() -> discoveryClient.discover(service));
    Discoverable discoverable = endpointStrategy.pick(1, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);
    ConnectionConfig connectionConfig = ConnectionConfig.builder()
      .setHostname(discoverable.getSocketAddress().getHostName())
      .setPort(discoverable.getSocketAddress().getPort())
      .setSSLEnabled(URIScheme.HTTPS.isMatch(discoverable))
      .build();
    return ClientConfig.builder().setVerifySSLCert(false).setConnectionConfig(connectionConfig).build();
  }

  /**
   * Returns the first value of the given header from the given response. If there is no such header, {@code null} is
   * returned.
   */
  @Nullable
  private static String getFirstHeaderValue(HttpResponse response, String name) {
    return response.getHeaders().get(name).stream().findFirst().orElse(null);
  }
}

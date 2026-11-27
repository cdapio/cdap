/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.metadata.system.DatasetSystemMetadataProvider;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.PluginId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link SystemMetadataRebuilder}, and for the two ways to start it: the REST endpoint
 * and the check of the {@link MetadataSubscriberService}.
 */
public class SystemMetadataRebuilderTest extends AppFabricTestBase {

  private static final String USER_TAG = "usertag";

  private MetadataStorage storage;
  private ArtifactSummary artifact;
  private MetadataEntity app;
  private MetadataEntity workflow;
  private MetadataEntity artifactEntity;
  private MetadataEntity dataset;
  // the system metadata of the entities after the deployment of the application
  private Map<MetadataEntity, Metadata> deployed;

  @Before
  public void deployApplication() throws Exception {
    storage = getInjector().getInstance(MetadataStorage.class);
    deploy(AllProgramsApp.class, 200);
    ApplicationDetail detail = getAppDetails(NamespaceId.DEFAULT.getNamespace(),
        AllProgramsApp.NAME);
    ApplicationId appId = NamespaceId.DEFAULT.app(AllProgramsApp.NAME, detail.getAppVersion());
    artifact = detail.getArtifact();
    app = appId.toMetadataEntity();
    dataset = NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME).toMetadataEntity();
    workflow = appId.program(ProgramType.WORKFLOW,
        AllProgramsApp.NoOpWorkflow.NAME).toMetadataEntity();
    artifactEntity = NamespaceId.DEFAULT.artifact(
        artifact.getName(), artifact.getVersion()).toMetadataEntity();

    // The deployment writes the metadata in the background. The profile of the workflow is the
    // last item.
    Tasks.waitFor(true, () -> readSystem(workflow).getProperties(MetadataScope.SYSTEM)
            .containsKey("profile") && !readSystem(dataset).getProperties().isEmpty()
            && !readSystem(app).getProperties().isEmpty()
            && !readSystem(artifactEntity).getProperties().isEmpty(),
        20, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    deployed = new LinkedHashMap<>();
    for (MetadataEntity entity : new MetadataEntity[]{app, workflow, artifactEntity, dataset}) {
      deployed.put(entity, readSystem(entity));
    }

    storage.apply(new MetadataMutation.Update(app,
        new Metadata(MetadataScope.USER, ImmutableSet.of(USER_TAG))), MutationOptions.DEFAULT);
    for (MetadataEntity entity : deployed.keySet()) {
      storage.apply(new MetadataMutation.Remove(entity, MetadataScope.SYSTEM),
          MutationOptions.DEFAULT);
      Assert.assertEquals(Metadata.EMPTY, readSystem(entity));
    }
  }

  @After
  public void restoreDeployedMetadata() throws Exception {
    if (deployed != null) {
      for (Map.Entry<MetadataEntity, Metadata> entry : deployed.entrySet()) {
        storage.apply(new MetadataMutation.Remove(entry.getKey(), MetadataScope.SYSTEM),
            MutationOptions.DEFAULT);
        storage.apply(new MetadataMutation.Update(entry.getKey(), entry.getValue()),
            MutationOptions.DEFAULT);
      }
    }
  }

  @Test
  public void testRebuildEndpointRestoresSystemMetadata() throws Exception {
    Assert.assertEquals(200, callRebuild("metadata-internals/rebuild").getResponseCode());

    assertSystemMetadataRestored();
    Assert.assertEquals(ImmutableSet.of(USER_TAG),
        storage.read(new Read(app, MetadataScope.USER)).getTags(MetadataScope.USER));
  }

  @Test
  public void testRebuildEndpointWithDropDeletesUserMetadata() throws Exception {
    Assert.assertEquals(200,
        callRebuild("metadata-internals/rebuild?drop=true").getResponseCode());

    assertSystemMetadataRestored();
    Assert.assertEquals(Metadata.EMPTY, storage.read(new Read(app, MetadataScope.USER)));
  }

  @Test
  public void testRebuildPreservesExistingCreationTimeAndExtraSystemMetadata() throws Exception {
    Map<MetadataEntity, String> existingCreationTimes = ImmutableMap.of(
        app, "1001",
        workflow, "1002",
        artifactEntity, "1003");
    for (Map.Entry<MetadataEntity, String> entry : existingCreationTimes.entrySet()) {
      Map<String, String> props =
          new HashMap<>(deployed.get(entry.getKey()).getProperties(MetadataScope.SYSTEM));
      props.put(MetadataConstants.CREATION_TIME_KEY, entry.getValue());
      storage.apply(new MetadataMutation.Update(entry.getKey(),
          new Metadata(MetadataScope.SYSTEM,
              deployed.get(entry.getKey()).getTags(MetadataScope.SYSTEM), props)),
          MutationOptions.DEFAULT);
    }
    // Add system metadata that the rebuild cannot regenerate from the stores.
    storage.apply(new MetadataMutation.Update(app,
        new Metadata(MetadataScope.SYSTEM, ImmutableSet.of("emitted-tag"),
            ImmutableMap.of("emitted-prop", "emitted-val"))),
        MutationOptions.DEFAULT);

    Assert.assertEquals(200, callRebuild("metadata-internals/rebuild").getResponseCode());

    for (Map.Entry<MetadataEntity, String> entry : existingCreationTimes.entrySet()) {
      Assert.assertEquals(entry.getKey().toString(), entry.getValue(),
          readSystem(entry.getKey()).getProperties(MetadataScope.SYSTEM)
              .get(MetadataConstants.CREATION_TIME_KEY));
    }
    Metadata rebuiltApp = readSystem(app);
    Assert.assertEquals("emitted-val",
        rebuiltApp.getProperties(MetadataScope.SYSTEM).get("emitted-prop"));
    Set<String> expectedAppTags = new HashSet<>(deployed.get(app).getTags(MetadataScope.SYSTEM));
    expectedAppTags.add("emitted-tag");
    Assert.assertEquals(expectedAppTags, rebuiltApp.getTags(MetadataScope.SYSTEM));
  }

  @Test
  public void testRebuildUsesLatestApplicationVersionAndMergesSharedPlugins() throws Exception {
    Store store = getInjector().getInstance(DefaultStore.class);
    ArtifactId apiArtifactId =
        NamespaceId.DEFAULT.artifact(artifact.getName(), artifact.getVersion()).toApiArtifactId();
    PluginClass pluginClass = PluginClass.builder()
        .setName("SharedPlugin").setType("transform").setDescription("shared plugin")
        .setClassName("io.cdap.test.SharedPlugin").setRequirements(Requirements.EMPTY).build();
    Plugin sharedPlugin = new Plugin(Collections.emptyList(), apiArtifactId, pluginClass,
        PluginProperties.builder().build());

    // Older version sorts lexicographically after latest version, to verify latestOnly filtering.
    ApplicationId olderAppId = NamespaceId.DEFAULT.app("VersionedPluginApp", "v2-older");
    ApplicationId latestAppId = NamespaceId.DEFAULT.app("VersionedPluginApp", "v1-latest");
    ApplicationId secondAppId = NamespaceId.DEFAULT.app("SecondPluginApp", "v1");
    PluginId sharedPluginId = new PluginId(NamespaceId.DEFAULT.getNamespace(), sharedPlugin);

    store.addLatestApplication(olderAppId, new ApplicationMeta(olderAppId.getApplication(),
        createAppSpec(olderAppId, apiArtifactId, "older description",
            ImmutableMap.of("p1", sharedPlugin)),
        new ChangeDetail(null, null, null, 2000L)));
    store.addLatestApplication(latestAppId, new ApplicationMeta(latestAppId.getApplication(),
        createAppSpec(latestAppId, apiArtifactId, "latest description",
            ImmutableMap.of("p1", sharedPlugin, "p2", sharedPlugin)),
        new ChangeDetail(null, null, null, 3000L)));
    store.addLatestApplication(secondAppId, new ApplicationMeta(secondAppId.getApplication(),
        createAppSpec(secondAppId, apiArtifactId, "second app",
            ImmutableMap.of("p1", sharedPlugin)),
        new ChangeDetail(null, null, null, 4000L)));

    try {
      Assert.assertEquals(200, callRebuild("metadata-internals/rebuild").getResponseCode());

      Map<String, String> rebuiltVersionedProps =
          readSystem(latestAppId.toMetadataEntity()).getProperties(MetadataScope.SYSTEM);
      Assert.assertEquals("latest description",
          rebuiltVersionedProps.get(MetadataConstants.DESCRIPTION_KEY));
      Assert.assertEquals("v1-latest", rebuiltVersionedProps.get("version"));
      Assert.assertEquals("3000",
          rebuiltVersionedProps.get(MetadataConstants.CREATION_TIME_KEY));

      Map<String, String> pluginProps =
          readSystem(sharedPluginId.toMetadataEntity()).getProperties(MetadataScope.SYSTEM);
      Assert.assertEquals(ImmutableMap.of(
          "default:VersionedPluginApp", "2",
          "default:SecondPluginApp", "1"), pluginProps);
    } finally {
      store.removeApplication(olderAppId);
      store.removeApplication(latestAppId);
      store.removeApplication(secondAppId);
      storage.apply(new MetadataMutation.Drop(latestAppId.toMetadataEntity()),
          MutationOptions.DEFAULT);
      storage.apply(new MetadataMutation.Drop(secondAppId.toMetadataEntity()),
          MutationOptions.DEFAULT);
      storage.apply(new MetadataMutation.Drop(sharedPluginId.toMetadataEntity()),
          MutationOptions.DEFAULT);
    }
  }

  @Test
  public void testSubscriberRebuildsOnceIfStorageRequiresIt() throws Exception {
    AtomicBoolean rebuildRequired = new AtomicBoolean(true);
    AtomicInteger checks = new AtomicInteger();
    MetadataStorage flaggedStorage = createFlaggedStorage(rebuildRequired, checks, 0);
    MetadataSubscriberService subscriber = createSubscriber(flaggedStorage);

    subscriber.preProcess();

    Assert.assertFalse(rebuildRequired.get());
    assertSystemMetadataRestored();

    // the subscriber asks the storage one time only
    subscriber.preProcess();
    Assert.assertEquals(1, checks.get());
  }

  @Test
  public void testSubscriberSkipsRebuildWhenNotRequired() throws Exception {
    AtomicBoolean rebuildRequired = new AtomicBoolean(false);
    AtomicInteger checks = new AtomicInteger();
    MetadataStorage cleanStorage = createFlaggedStorage(rebuildRequired, checks, 0);
    MetadataSubscriberService subscriber = createSubscriber(cleanStorage);

    subscriber.preProcess();
    subscriber.preProcess();

    Assert.assertEquals(1, checks.get());
    Assert.assertEquals(Metadata.EMPTY, readSystem(app));
  }

  @Test
  public void testSubscriberDelaysRetryAfterRebuildCheckFails() throws Exception {
    AtomicBoolean rebuildRequired = new AtomicBoolean(true);
    AtomicInteger checks = new AtomicInteger();
    MetadataStorage failingOnceStorage = createFlaggedStorage(rebuildRequired, checks, 1);
    MetadataSubscriberService subscriber = createSubscriber(failingOnceStorage);

    // First run catches the IOException without throwing and without rebuilding.
    subscriber.preProcess();
    Assert.assertEquals(1, checks.get());
    Assert.assertTrue(rebuildRequired.get());
    Assert.assertEquals(Metadata.EMPTY, readSystem(app));

    // Immediate second run does not retry during the retry delay window.
    subscriber.preProcess();
    Assert.assertEquals(1, checks.get());
    Assert.assertTrue(rebuildRequired.get());
    Assert.assertEquals(Metadata.EMPTY, readSystem(app));
  }

  private MetadataStorage createFlaggedStorage(AtomicBoolean rebuildRequired,
      AtomicInteger checks, int initialFailures) {
    AtomicInteger remainingFailures = new AtomicInteger(initialFailures);
    return (MetadataStorage) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[]{MetadataStorage.class},
        (proxy, method, args) -> {
          switch (method.getName()) {
            case "isRebuildRequired":
              checks.incrementAndGet();
              if (remainingFailures.getAndDecrement() > 0) {
                throw new IOException("storage temporarily unavailable");
              }
              return rebuildRequired.get();
            case "markRebuilt":
              rebuildRequired.set(false);
              return null;
            default:
              try {
                return method.invoke(storage, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
          }
        });
  }

  private MetadataSubscriberService createSubscriber(MetadataStorage targetStorage) {
    Injector injector = getInjector();
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
    MetricsCollectionService metrics = injector.getInstance(MetricsCollectionService.class);
    return new MetadataSubscriberService(
        injector.getInstance(CConfiguration.class), injector.getInstance(MessagingService.class),
        metrics, targetStorage, transactionRunner,
        new SystemMetadataRebuilder(targetStorage, transactionRunner,
            injector.getInstance(ArtifactStore.class), metrics));
  }

  private static ApplicationSpecification createAppSpec(ApplicationId appId,
      ArtifactId artifactId, String description,
      Map<String, Plugin> plugins) {
    return new DefaultApplicationSpecification(
        appId.getApplication(), appId.getVersion(), ProjectInfo.getVersion().toString(),
        description, "", artifactId,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), plugins);
  }

  private Metadata readSystem(MetadataEntity entity) throws IOException {
    return storage.read(new Read(entity, MetadataScope.SYSTEM));
  }

  private HttpResponse callRebuild(String resource) throws IOException {
    RemoteClient client = getInjector().getInstance(RemoteClientFactory.class).createRemoteClient(
        Constants.Service.METADATA_SERVICE, new DefaultHttpRequestConfig(false),
        Constants.Gateway.API_VERSION_3);
    return client.execute(client.requestBuilder(HttpMethod.POST, resource).build());
  }

  private void assertSystemMetadataRestored() throws IOException {
    for (Map.Entry<MetadataEntity, Metadata> entry : deployed.entrySet()) {
      MetadataEntity entity = entry.getKey();
      Map<String, String> expected =
          new HashMap<>(entry.getValue().getProperties(MetadataScope.SYSTEM));
      Metadata rebuilt = readSystem(entity);
      Map<String, String> actual = new HashMap<>(rebuilt.getProperties(MetadataScope.SYSTEM));
      expected.remove(MetadataConstants.CREATION_TIME_KEY);
      if (entity.equals(dataset)) {
        // The rebuild reads the dataset specification without instantiating the dataset or knowing
        // when it was created: creation-time and the instance-derived "batch" tag stay absent.
        Assert.assertFalse(actual.containsKey(MetadataConstants.CREATION_TIME_KEY));
        Assert.assertEquals(entity.toString(), expected, actual);
        Set<String> expectedTags = new HashSet<>(entry.getValue().getTags(MetadataScope.SYSTEM));
        expectedTags.remove(DatasetSystemMetadataProvider.BATCH_TAG);
        Assert.assertEquals(entity.toString(), expectedTags,
            rebuilt.getTags(MetadataScope.SYSTEM));
        continue;
      }
      // Missing creation-time must be populated from the application store or current time.
      String rebuiltCreationTime = actual.remove(MetadataConstants.CREATION_TIME_KEY);
      Assert.assertNotNull(entity.toString(), rebuiltCreationTime);
      Assert.assertTrue(entity.toString(), Long.parseLong(rebuiltCreationTime) > 0);
      Assert.assertEquals(entity.toString(), expected, actual);
      Assert.assertEquals(entity.toString(), entry.getValue().getTags(MetadataScope.SYSTEM),
          rebuilt.getTags(MetadataScope.SYSTEM));
    }
  }
}

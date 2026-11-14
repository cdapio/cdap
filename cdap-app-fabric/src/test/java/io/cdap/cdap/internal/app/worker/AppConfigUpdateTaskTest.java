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

package io.cdap.cdap.internal.app.worker;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ApplicationConfigUpdateAction;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ApplicationUpdateContext;
import io.cdap.cdap.api.app.ApplicationUpdateResult;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AppConfigUpdateTask}.
 */
public class AppConfigUpdateTaskTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
          new GsonBuilder())
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
      .registerTypeAdapter(Requirements.class, new RequirementsCodec())
      .create();

  private static CConfiguration cConf;
  private static Impersonator impersonator;
  private static ArtifactRepository artifactRepo;

  /**
   * Non-Application test class used to verify static initializer is not executed pre-verification.
   */
  public static class NonApplicationClass {
    static {
      System.setProperty("test.malicious.clinit", "true");
    }
  }

  /**
   * Test Application that does not support config updates.
   */
  public static class AppWithoutUpdateSupport extends AbstractApplication {
    @Override
    public void configure() {
    }

    @Override
    public boolean isUpdateSupported() {
      return false;
    }
  }

  /**
   * Test Application that returns null from updateConfig.
   */
  public static class NullResultApp extends AbstractApplication {
    @Override
    public void configure() {
    }

    @Override
    public boolean isUpdateSupported() {
      return true;
    }

    @Override
    public ApplicationUpdateResult<?> updateConfig(ApplicationUpdateContext updateContext) {
      return null;
    }
  }

  /**
   * Raw generic test Application that returns a Config subclass.
   */
  public static class RawGenericUpdatableApp extends AbstractApplication {
    public static class CustomSubConfig extends Config {
      private final String customField;

      public CustomSubConfig(String customField) {
        this.customField = customField;
      }
    }

    @Override
    public void configure() {
    }

    @Override
    public boolean isUpdateSupported() {
      return true;
    }

    @Override
    public ApplicationUpdateResult<Config> updateConfig(ApplicationUpdateContext updateContext) {
      return new ApplicationUpdateResult<>(new CustomSubConfig("preserved-subclass-field"));
    }
  }

  /**
   * Test Application that supports config updates.
   */
  public static class UpdatableTestApp extends AbstractApplication<UpdatableTestApp.TestConfig> {
    public static class TestConfig extends Config {
      private final String value;

      public TestConfig(String value) {
        this.value = value;
      }
    }

    @Override
    public void configure() {
    }

    @Override
    public boolean isUpdateSupported() {
      return true;
    }

    @Override
    public ApplicationUpdateResult<TestConfig> updateConfig(
        ApplicationUpdateContext updateContext) throws Exception {
      TestConfig current = updateContext.getConfig(TestConfig.class);
      return new ApplicationUpdateResult<>(new TestConfig(current.value + "-updated"));
    }
  }

  @BeforeClass
  public static void setup() throws IOException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    impersonator = new DefaultImpersonator(cConf, null);
    artifactRepo = new DefaultArtifactRepository(cConf, null, null, null, impersonator);
  }

  @Test
  public void testUpdateSuccess() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, UpdatableTestApp.class);
    File unpackedDir = TEMP_FOLDER.newFolder();
    BundleJarUtil.unJar(appJar, unpackedDir);

    ApplicationId appId = NamespaceId.DEFAULT.app("testApp");
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0.0");

    ArtifactLocalizerClient localizerClient = Mockito.mock(ArtifactLocalizerClient.class);
    Mockito.when(localizerClient.getUnpackedArtifactLocation(artifactId)).thenReturn(unpackedDir);

    ApplicationClass appClass = new ApplicationClass(UpdatableTestApp.class.getName(), "", null);
    AppConfigUpdateRequest request = new AppConfigUpdateRequest(
        appId,
        artifactId,
        appClass,
        createAppSpec(artifactId, "{\"value\":\"initial-value\"}"),
        Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
        Collections.singleton(ArtifactScope.USER),
        false);

    String updatedConfig = AppConfigUpdateTask.update(
        request, impersonator, artifactRepo, localizerClient);
    Assert.assertNotNull(updatedConfig);
    Assert.assertTrue(updatedConfig.contains("initial-value-updated"));
  }

  @Test
  public void testRawGenericAppPreservesSubclassConfig() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("testApp");
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0.0");
    ApplicationClass appClass =
        new ApplicationClass(RawGenericUpdatableApp.class.getName(), "", null);

    String updatedConfig = AppConfigUpdateTask.updateAppConfig(
        appId,
        artifactId,
        appClass,
        createAppSpec(artifactId, "{}"),
        RawGenericUpdatableApp.class.getClassLoader(),
        artifactRepo,
        Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
        ImmutableSet.of(ArtifactScope.USER),
        false);

    Assert.assertNotNull(updatedConfig);
    Assert.assertTrue(updatedConfig.contains("preserved-subclass-field"));
  }

  @Test
  public void testRequestSerialization() {
    ApplicationId appId = new NamespaceId("ns").app("testApp");
    ArtifactId artifactId = new NamespaceId("ns").artifact("testArtifact", "1.0.0");
    ApplicationClass appClass = new ApplicationClass(UpdatableTestApp.class.getName(), "desc", null);
    AppConfigUpdateRequest request = new AppConfigUpdateRequest(
        appId,
        artifactId,
        appClass,
        createAppSpec(artifactId, "{}"),
        Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
        Collections.singleton(ArtifactScope.USER),
        false);

    AppConfigUpdateRequest deserialized =
        GSON.fromJson(GSON.toJson(request), AppConfigUpdateRequest.class);

    Assert.assertEquals(appId, deserialized.getAppId());
    Assert.assertEquals(artifactId, deserialized.getArtifactId());
    Assert.assertEquals(appClass.getClassName(), deserialized.getApplicationClass().getClassName());
    Assert.assertEquals(
        Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
        deserialized.getUpdateActions());
    Assert.assertTrue(deserialized.getAllowedArtifactScopes().contains(ArtifactScope.USER));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testAppWithoutUpdateSupportFails() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("testApp");
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0.0");
    ApplicationClass appClass =
        new ApplicationClass(AppWithoutUpdateSupport.class.getName(), "", null);

    AppConfigUpdateTask.updateAppConfig(
        appId,
        artifactId,
        appClass,
        createAppSpec(artifactId, "{}"),
        AppWithoutUpdateSupport.class.getClassLoader(),
        artifactRepo,
        Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
        ImmutableSet.of(ArtifactScope.USER),
        false);
  }

  @Test(expected = IllegalStateException.class)
  public void testNullUpdateResultFails() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("testApp");
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0.0");
    ApplicationClass appClass = new ApplicationClass(NullResultApp.class.getName(), "", null);

    AppConfigUpdateTask.updateAppConfig(
        appId,
        artifactId,
        appClass,
        createAppSpec(artifactId, "{}"),
        NullResultApp.class.getClassLoader(),
        artifactRepo,
        Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
        ImmutableSet.of(ArtifactScope.USER),
        false);
  }

  @Test
  public void testNonApplicationClassRejectedWithoutClinit() throws Exception {
    System.clearProperty("test.malicious.clinit");
    try {
      ApplicationId appId = NamespaceId.DEFAULT.app("testApp");
      ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0.0");
      ApplicationClass appClass =
          new ApplicationClass(NonApplicationClass.class.getName(), "", null);

      try {
        AppConfigUpdateTask.updateAppConfig(
            appId,
            artifactId,
            appClass,
            createAppSpec(artifactId, "{}"),
            NonApplicationClass.class.getClassLoader(),
            artifactRepo,
            Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
            ImmutableSet.of(ArtifactScope.USER),
            false);
        Assert.fail("Expected IllegalStateException for non-Application class");
      } catch (IllegalStateException e) {
        Assert.assertTrue(e.getMessage().contains("Application main class is of invalid type"));
        Assert.assertNull(
            "Class static initializer should not have executed",
            System.getProperty("test.malicious.clinit"));
      }
    } finally {
      System.clearProperty("test.malicious.clinit");
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidRequestFails() throws Exception {
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    AppConfigUpdateTask task = new AppConfigUpdateTask(
        cConf, discoveryService, discoveryService, new NoOpMetricsCollectionService());

    RunnableTaskRequest request = RunnableTaskRequest.getBuilder(AppConfigUpdateTask.class.getName())
        .withParam("{}")
        .withNamespace(NamespaceId.DEFAULT.getNamespace())
        .build();
    RunnableTaskContext context = new RunnableTaskContext(request);
    try {
      task.run(context);
    } finally {
      Assert.assertTrue(context.isTerminateOnComplete());
    }
  }

  private static ApplicationSpecification createAppSpec(ArtifactId artifactId, String config) {
    return new DefaultApplicationSpecification(
        "testApp", "1.0.0", "desc", config,
        artifactId.toApiArtifactId(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
  }
}

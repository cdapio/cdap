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
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.deploy.InMemoryAppConfigUpdater;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.runtime.artifact.DefaultArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizer;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerHttpHandlerInternal;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.http.NettyHttpService;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
      .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
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
   * Abstract test Application that fails instantiation with a detail message.
   */
  public abstract static class AbstractTestApp extends AbstractApplication {
  }

  /**
   * Test Application without a no-arg constructor so instantiation fails with null detail message.
   */
  public static class NoDefaultCtorApp extends AbstractApplication {
    public NoDefaultCtorApp(String unused) {
    }

    @Override
    public void configure() {
    }
  }

  /**
   * Test Application that throws an Error during updateConfig.
   */
  public static class ErrorThrowingApp extends AbstractApplication {
    @Override
    public void configure() {
    }

    @Override
    public boolean isUpdateSupported() {
      return true;
    }

    @Override
    public ApplicationUpdateResult<?> updateConfig(ApplicationUpdateContext updateContext) {
      throw new AssertionError("simulated fatal error");
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

    ArtifactLocalizer mockLocalizer = Mockito.mock(ArtifactLocalizer.class);
    Mockito.when(mockLocalizer.getAndUnpackArtifact(artifactId)).thenReturn(unpackedDir);

    NettyHttpService localizerService = NettyHttpService.builder("test-artifact-localizer")
        .setHttpHandlers(new ArtifactLocalizerHttpHandlerInternal(mockLocalizer))
        .build();
    localizerService.start();
    try {
      CConfiguration localConf = CConfiguration.copy(cConf);
      localConf.setInt(
          Constants.ArtifactLocalizer.PORT, localizerService.getBindAddress().getPort());

      ApplicationClass appClass =
          new ApplicationClass(UpdatableTestApp.class.getName(), "", null);
      AppConfigUpdateRequest request = new AppConfigUpdateRequest(
          appId,
          artifactId,
          appClass,
          createAppSpec(artifactId, "{\"value\":\"initial-value\"}"),
          Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
          Collections.singleton(ArtifactScope.USER),
          false);

      InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
      AppConfigUpdateTask task = new AppConfigUpdateTask(
          localConf, discoveryService, discoveryService, new NoOpMetricsCollectionService());
      RunnableTaskRequest taskRequest =
          RunnableTaskRequest.getBuilder(AppConfigUpdateTask.class.getName())
              .withParam(GSON.toJson(request))
              .withNamespace(appId.getNamespace())
              .build();
      RunnableTaskContext context = new RunnableTaskContext(taskRequest);
      task.run(context);

      String updatedConfig = StandardCharsets.UTF_8.decode(context.getResult()).toString();
      Assert.assertTrue(context.isTerminateOnComplete());
      Assert.assertTrue(updatedConfig.contains("initial-value-updated"));
      Mockito.verify(mockLocalizer).getAndUnpackArtifact(artifactId);
    } finally {
      localizerService.stop();
    }
  }

  @Test
  public void testRawGenericAppPreservesSubclassConfig() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("testApp");
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0.0");
    ApplicationClass appClass =
        new ApplicationClass(RawGenericUpdatableApp.class.getName(), "", null);

    AppConfigUpdateRequest request = new AppConfigUpdateRequest(
        appId,
        artifactId,
        appClass,
        createAppSpec(artifactId, null),
        Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
        ImmutableSet.of(ArtifactScope.USER),
        false);
    String updatedConfig = InMemoryAppConfigUpdater.updateAppConfig(
        request, RawGenericUpdatableApp.class.getClassLoader(), artifactRepo);

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

    AppConfigUpdateRequest request = new AppConfigUpdateRequest(
        appId,
        artifactId,
        appClass,
        createAppSpec(artifactId, "{}"),
        Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
        ImmutableSet.of(ArtifactScope.USER),
        false);
    InMemoryAppConfigUpdater.updateAppConfig(
        request, AppWithoutUpdateSupport.class.getClassLoader(), artifactRepo);
  }

  @Test(expected = IllegalStateException.class)
  public void testNullUpdateResultFails() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("testApp");
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0.0");
    ApplicationClass appClass = new ApplicationClass(NullResultApp.class.getName(), "", null);

    AppConfigUpdateRequest request = new AppConfigUpdateRequest(
        appId,
        artifactId,
        appClass,
        createAppSpec(artifactId, "{}"),
        Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
        ImmutableSet.of(ArtifactScope.USER),
        false);
    InMemoryAppConfigUpdater.updateAppConfig(
        request, NullResultApp.class.getClassLoader(), artifactRepo);
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
        AppConfigUpdateRequest request = new AppConfigUpdateRequest(
            appId,
            artifactId,
            appClass,
            createAppSpec(artifactId, "{}"),
            Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
            ImmutableSet.of(ArtifactScope.USER),
            false);
        InMemoryAppConfigUpdater.updateAppConfig(
            request, NonApplicationClass.class.getClassLoader(), artifactRepo);
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

  @Test
  public void testUninstantiableAndErrorThrowingApps() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("testApp");
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0.0");

    for (Class<?> badAppClass : Arrays.asList(AbstractTestApp.class, NoDefaultCtorApp.class)) {
      AppConfigUpdateRequest request = new AppConfigUpdateRequest(
          appId,
          artifactId,
          new ApplicationClass(badAppClass.getName(), "", null),
          createAppSpec(artifactId, "{}"),
          Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
          ImmutableSet.of(ArtifactScope.USER),
          false);
      try {
        InMemoryAppConfigUpdater.updateAppConfig(
            request, badAppClass.getClassLoader(), artifactRepo);
        Assert.fail("Expected InvalidArtifactException for " + badAppClass.getName());
      } catch (InvalidArtifactException e) {
        Assert.assertTrue(
            e.getMessage().contains("Failed to load or instantiate application class"));
      }
    }

    AppConfigUpdateRequest errorRequest = new AppConfigUpdateRequest(
        appId,
        artifactId,
        new ApplicationClass(ErrorThrowingApp.class.getName(), "", null),
        createAppSpec(artifactId, "{}"),
        Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
        ImmutableSet.of(ArtifactScope.USER),
        false);
    try {
      InMemoryAppConfigUpdater.updateAppConfig(
          errorRequest, ErrorThrowingApp.class.getClassLoader(), artifactRepo);
      Assert.fail("Expected AssertionError");
    } catch (AssertionError e) {
      Assert.assertEquals("simulated fatal error", e.getMessage());
    }
  }

  @Test
  public void testThrowIfMissingDependency() throws Exception {
    ClassLoader noSparkLoader = new ClassLoader(null) {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        throw new ClassNotFoundException(name);
      }
    };
    ClassLoader sparkAvailableLoader = new ClassLoader(null) {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        if ("org.apache.spark.SparkContext".equals(name)) {
          return Object.class;
        }
        throw new ClassNotFoundException(name);
      }
    };

    Artifacts.throwIfMissingDependency(new IllegalArgumentException("not missing"), noSparkLoader);

    try {
      Artifacts.throwIfMissingDependency(
          new RuntimeException(new NoClassDefFoundError("Could not initialize class pkg.Broken")),
          noSparkLoader);
      Assert.fail("Expected InvalidArtifactException for failed static initialization");
    } catch (InvalidArtifactException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to initialize class:"));
    }

    try {
      Artifacts.throwIfMissingDependency(
          new NoClassDefFoundError("org/apache/spark/sql/SparkSession"), noSparkLoader);
      Assert.fail("Expected IllegalStateException when Spark is unavailable");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("unavailability of Spark"));
    }

    try {
      Artifacts.throwIfMissingDependency(
          new ClassNotFoundException("io.cdap.cdap.api.spark.Spark"), sparkAvailableLoader);
      Assert.fail("Expected InvalidArtifactException when Spark version is incompatible");
    } catch (InvalidArtifactException e) {
      Assert.assertTrue(e.getMessage().contains("Configured to use Spark located at"));
    }

    try {
      Artifacts.throwIfMissingDependency(
          new ClassNotFoundException("com.example.MissingLib"), noSparkLoader);
      Assert.fail("Expected InvalidArtifactException for missing dependency class");
    } catch (InvalidArtifactException e) {
      Assert.assertTrue(e.getMessage().contains("Missing class com.example.MissingLib."));
    }

    for (Throwable unnamed : Arrays.asList(
        new NoClassDefFoundError(), new NoClassDefFoundError(""))) {
      try {
        Artifacts.throwIfMissingDependency(unnamed, noSparkLoader);
        Assert.fail("Expected InvalidArtifactException for unnamed missing dependency");
      } catch (InvalidArtifactException e) {
        Assert.assertTrue(e.getMessage().contains("Missing required class."));
      }
    }
  }

  @Test
  public void testTaskParameterAndRequestFieldValidation() throws Exception {
    for (String emptyParam : Arrays.asList(null, "")) {
      try {
        runTaskWithParam(emptyParam);
        Assert.fail("Expected IllegalArgumentException for null or empty param");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("task parameter must not be null or empty"));
      }
    }

    ApplicationId appId = NamespaceId.DEFAULT.app("testApp");
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0.0");
    ApplicationClass appClass = new ApplicationClass(UpdatableTestApp.class.getName(), "", null);
    ApplicationSpecification spec = createAppSpec(artifactId, "{}");

    for (AppConfigUpdateRequest invalid : Arrays.asList(
        new AppConfigUpdateRequest(
            null, artifactId, appClass, spec, Collections.emptyList(), Collections.emptySet(),
            false),
        new AppConfigUpdateRequest(
            appId, null, appClass, spec, Collections.emptyList(), Collections.emptySet(), false),
        new AppConfigUpdateRequest(
            appId, artifactId, null, spec, Collections.emptyList(), Collections.emptySet(), false),
        new AppConfigUpdateRequest(
            appId, artifactId, appClass, null, Collections.emptyList(), Collections.emptySet(),
            false),
        new AppConfigUpdateRequest(
            appId, artifactId, appClass, spec, null, Collections.emptySet(), false),
        new AppConfigUpdateRequest(
            appId, artifactId, appClass, spec, Collections.emptyList(), null, false))) {
      try {
        runTaskWithParam(GSON.toJson(invalid));
        Assert.fail("Expected IllegalArgumentException for missing required request field");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("Invalid AppConfigUpdateRequest:"));
      }
    }
  }

  @Test
  public void testCreateInjectorBindsRunner() {
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    AppConfigUpdateTask task = new AppConfigUpdateTask(
        cConf, discoveryService, discoveryService, new NoOpMetricsCollectionService());
    Assert.assertNotNull(
        task.createInjector().getInstance(AppConfigUpdateTask.AppConfigUpdateTaskRunner.class));
  }

  private void runTaskWithParam(String param) throws Exception {
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    AppConfigUpdateTask task = new AppConfigUpdateTask(
        cConf, discoveryService, discoveryService, new NoOpMetricsCollectionService());

    RunnableTaskRequest.Builder builder =
        RunnableTaskRequest.getBuilder(AppConfigUpdateTask.class.getName())
            .withNamespace(NamespaceId.DEFAULT.getNamespace());
    if (param != null) {
      builder.withParam(param);
    }
    RunnableTaskContext context = new RunnableTaskContext(builder.build());
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

/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.common.io.Files;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.worker.TaskWorkerServiceTest;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InternalAccessEnforcer;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit test for {@link ArtifactLocalizerService}.
 */
public class ArtifactLocalizerServiceTest extends AppFabricTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private ArtifactLocalizerService localizerService;
  private CConfiguration cConf;
  private Injector securityInjector;
  private TokenManager tokenManager;

  private CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, InetAddress.getLoopbackAddress().getHostName());
    cConf.setInt(Constants.TaskWorker.PORT, 0);
    cConf.setInt(Constants.ArtifactLocalizer.PORT, 0);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    cConf.setInt(Constants.ArtifactLocalizer.CACHE_CLEANUP_INTERVAL_MIN, 60);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    return cConf;
  }

  private ArtifactLocalizerService setupArtifactLocalizerService(CConfiguration cConf) {
    DiscoveryServiceClient discoveryClient =
        getInjector().getInstance(DiscoveryServiceClient.class);
    CConfiguration secConf = CConfiguration.copy(cConf);
    secConf.setBoolean(Constants.Security.ENABLED, true);
    secConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    securityInjector = Guice.createInjector(
        new IOModule(),
        new ConfigModule(secConf),
        new CoreSecurityRuntimeModule().getInMemoryModules(),
        new AuthenticationContextModules().getMasterModule());
    tokenManager = securityInjector.getInstance(TokenManager.class);
    tokenManager.startAndWait();
    InternalAccessEnforcer internalAccessEnforcer =
        securityInjector.getInstance(InternalAccessEnforcer.class);

    RemoteClientFactory remoteClientFactory =
        new RemoteClientFactory(discoveryClient,
            new DefaultInternalAuthenticator(new AuthenticationTestContext()));
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    ArtifactLocalizerService artifactLocalizerService = new ArtifactLocalizerService(
        cConf, new ArtifactLocalizer(cConf, remoteClientFactory, (namespaceId, retryStrategy) -> {
          return new NoOpArtifactManager();
        }), new CommonNettyHttpServiceFactory(cConf, new NoOpMetricsCollectionService(),
            auditLogContexts -> {}, new NoOpAeadCipher()),
        remoteClientFactory, new NoOpRemoteAuthenticator(), internalAccessEnforcer);
    // start the service
    artifactLocalizerService.startAndWait();
    cConf.setInt(Constants.ArtifactLocalizer.PORT, artifactLocalizerService.getPort());

    return artifactLocalizerService;
  }

  @Before
  public void setUp() throws Exception {
    cConf = createCConf();
    localizerService = setupArtifactLocalizerService(cConf);
    getInjector().getInstance(ArtifactRepository.class).clear(NamespaceId.DEFAULT);
  }

  @After
  public void tearDown() throws Exception {
    if (this.localizerService != null) {
      this.localizerService.stopAndWait();
      this.localizerService = null;
    }
    if (this.tokenManager != null) {
      this.tokenManager.stopAndWait();
      this.tokenManager = null;
    }
  }

  @Test
  public void testValidateCallerEndpoint() throws Exception {
    URL validateUrl = new URL(String.format("http://127.0.0.1:%d/v3Internal/worker/validate-caller",
        localizerService.getPort()));

    // 1. Missing header -> 401
    HttpResponse missingResp = HttpRequests.execute(HttpRequest.post(validateUrl).build());
    Assert.assertEquals(401, missingResp.getResponseCode());

    // 2. Malformed header (no space / missing token) -> 401
    HttpResponse malformedResp = HttpRequests.execute(HttpRequest.post(validateUrl)
        .addHeader(Constants.Security.Headers.INTERNAL_AUTHORIZATION, "CDAP-Internal")
        .build());
    Assert.assertEquals(401, malformedResp.getResponseCode());

    // 3. Non-internal credential scheme -> 403
    HttpResponse bearerResp = HttpRequests.execute(HttpRequest.post(validateUrl)
        .addHeader(Constants.Security.Headers.INTERNAL_AUTHORIZATION, "Bearer some-oauth-token")
        .build());
    Assert.assertEquals(403, bearerResp.getResponseCode());

    // 4. Forged CDAP-Internal token -> 403
    HttpResponse forgedResp = HttpRequests.execute(HttpRequest.post(validateUrl)
        .addHeader(Constants.Security.Headers.INTERNAL_AUTHORIZATION,
            "CDAP-Internal Zm9yZ2VkLXRva2Vu")
        .build());
    Assert.assertEquals(403, forgedResp.getResponseCode());

    // 5. Valid TokenManager-signed CDAP-Internal token -> 200 OK
    Codec<AccessToken> accessTokenCodec =
        securityInjector.getInstance(Key.get(new TypeLiteral<Codec<AccessToken>>() { }));
    long now = System.currentTimeMillis();
    UserIdentity internalIdentity = new UserIdentity(
        "system", UserIdentity.IdentifierType.INTERNAL,
        Collections.emptyList(), now, now + 300_000L);
    String encodedToken = Base64.getEncoder().encodeToString(
        accessTokenCodec.encode(tokenManager.signIdentifier(internalIdentity)));
    HttpResponse validResp = HttpRequests.execute(HttpRequest.post(validateUrl)
        .addHeader(Constants.Security.Headers.INTERNAL_AUTHORIZATION,
            String.format("%s %s", Credential.CREDENTIAL_TYPE_INTERNAL, encodedToken))
        .build());
    Assert.assertEquals(200, validResp.getResponseCode());

    // 6. When INTERNAL_AUTH_ENABLED=false and STRICT=true, validateCaller fails closed with 403
    // even if a non-null InternalAccessEnforcer is passed and a valid-format header is provided.
    CConfiguration disabledAuthConf = createCConf();
    disabledAuthConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, false);
    disabledAuthConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), true);
    InternalAccessEnforcer enforcer = securityInjector.getInstance(InternalAccessEnforcer.class);
    RemoteClientFactory clientFactory = new RemoteClientFactory(
        getInjector().getInstance(DiscoveryServiceClient.class), new NoOpInternalAuthenticator());
    ArtifactLocalizerService disabledAuthSidecar = new ArtifactLocalizerService(
        disabledAuthConf,
        new ArtifactLocalizer(disabledAuthConf, clientFactory,
            (ns, retry) -> new NoOpArtifactManager()),
        new CommonNettyHttpServiceFactory(disabledAuthConf, new NoOpMetricsCollectionService(),
            auditLogContexts -> {}, new NoOpAeadCipher()),
        clientFactory, new NoOpRemoteAuthenticator(), enforcer);
    disabledAuthSidecar.startAndWait();
    try {
      URL disabledValidateUrl = new URL(String.format(
          "http://127.0.0.1:%d/v3Internal/worker/validate-caller", disabledAuthSidecar.getPort()));
      HttpResponse failClosedResp = HttpRequests.execute(HttpRequest.post(disabledValidateUrl)
          .addHeader(Constants.Security.Headers.INTERNAL_AUTHORIZATION,
              String.format("%s %s", Credential.CREDENTIAL_TYPE_INTERNAL, encodedToken))
          .build());
      Assert.assertEquals(403, failClosedResp.getResponseCode());
    } finally {
      disabledAuthSidecar.stopAndWait();
    }
  }

  @Test
  public void testCreateInjectorSkipsGcpRemoteAuthenticatorInStrictMode() throws Exception {
    CConfiguration strictConf = createCConf();
    strictConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS.getFeatureFlagString(), true);
    strictConf.setBoolean(
        "feature." + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(), true);
    strictConf.set(
        Constants.ArtifactLocalizer.REMOTE_AUTHENTICATOR_NAME, "gcp-remote-authenticator");
    strictConf.set(
        Constants.Security.CFG_FILE_BASED_KEYFILE_PATH,
        TEMP_FOLDER.newFile("keyfile.bin").getAbsolutePath());

    MasterEnvironment previousEnv = MasterEnvironments.getMasterEnvironment();
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    MasterEnvironment mockMasterEnv = Mockito.mock(MasterEnvironment.class);
    Mockito.when(mockMasterEnv.getDiscoveryServiceSupplier()).thenReturn(() -> discoveryService);
    Mockito.when(mockMasterEnv.getDiscoveryServiceClientSupplier())
        .thenReturn(() -> discoveryService);
    MasterEnvironments.setMasterEnvironment(mockMasterEnv);
    try {
      Injector sidecarInjector =
          ArtifactLocalizerTwillRunnable.createInjector(strictConf, new Configuration());
      RemoteAuthenticator remoteAuthenticator =
          sidecarInjector.getInstance(RemoteAuthenticator.class);
      Assert.assertTrue(remoteAuthenticator instanceof NoOpRemoteAuthenticator);
      ArtifactLocalizerService service =
          sidecarInjector.getInstance(ArtifactLocalizerService.class);
      Assert.assertNotNull(service);
    } finally {
      MasterEnvironments.setMasterEnvironment(previousEnv);
    }
  }

  @Test
  public void testUnpackArtifact() throws Exception {

    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());

    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    ArtifactLocalizerClient client =
        new ArtifactLocalizerClient(cConf, new NoOpInternalAuthenticator());

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(
        locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    File appJarFile = new File(TEMP_FOLDER.newFolder(),
        String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    File newAppJarFile = new File(TEMP_FOLDER.newFolder(),
        String.format("%s-%s-copy.jar", artifactId.getName(),
            artifactId.getVersion().getVersion()));
    Locations.linkOrCopy(appJar, appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);

    File unpackedDir = client.getUnpackedArtifactLocation(artifactId.toEntityId());

    // Make sure the artifact was actually cached
    validateUnpackDir(unpackedDir);

    // Call the sidecar again and make sure the same path was returned
    File sameUnpackedDir = client.getUnpackedArtifactLocation(artifactId.toEntityId());
    Assert.assertEquals(unpackedDir, sameUnpackedDir);

    // Delete and recreate the artifact to update the last modified date
    artifactRepository.deleteArtifact(artifactId);

    Thread.sleep(1000);
    Files.copy(appJarFile, newAppJarFile);
    artifactRepository.addArtifact(artifactId, newAppJarFile);

    File newUnpackDir = client.getUnpackedArtifactLocation(artifactId.toEntityId());

    //Make sure the two paths arent the same and the old directory still exists
    Assert.assertNotEquals(unpackedDir, newUnpackDir);
    validateUnpackDir(newUnpackDir);
    validateUnpackDir(unpackedDir);

    // Assert that the old cache directory is deleted after we run cleanup
    this.localizerService.forceCleanup();
    Assert.assertFalse(unpackedDir.exists());
  }

  private void validateUnpackDir(File unpackedFile) {
    // Make sure the directory exists
    Assert.assertTrue(unpackedFile.exists());
    Assert.assertTrue(unpackedFile.isDirectory());

    // Make sure theres multiple files in the directory and one of them is a manifest
    List<String> files = DirUtils.list(unpackedFile);
    Assert.assertTrue(files.size() > 1);
    Assert.assertTrue(files.stream().anyMatch(s -> s.equals("META-INF")));
  }

  @Test
  public void testArtifact() throws Exception {
    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    ArtifactLocalizerClient client =
        new ArtifactLocalizerClient(cConf, new NoOpInternalAuthenticator());

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(
        locationFactory, TaskWorkerServiceTest.TestRunnableClass.class);
    File appJarFile = new File(tmpFolder.newFolder(),
        String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    File newAppJarFile = new File(tmpFolder.newFolder(),
        String.format("%s-%s-copy.jar", artifactId.getName(),
            artifactId.getVersion().getVersion()));
    Locations.linkOrCopy(appJar, appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);

    File artifactPath = client.getArtifactLocation(artifactId.toEntityId());

    // Make sure the artifact was actually cached
    Assert.assertTrue(artifactPath.exists());
    Assert.assertTrue(artifactPath.isFile());

    // Call the sidecar again and make sure the same path was returned
    File sameArtifactPath = client.getArtifactLocation(artifactId.toEntityId());
    Assert.assertEquals(artifactPath, sameArtifactPath);

    // Delete and recreate the artifact to update the last modified date
    artifactRepository.deleteArtifact(artifactId);

    // This sleep is needed to delay the file copy so that the lastModified time on the file is different
    Thread.sleep(1000);

    // Wait a bit before recreating the artifact to make sure the last modified time is different
    Files.copy(appJarFile, newAppJarFile);
    artifactRepository.addArtifact(artifactId, newAppJarFile);

    File newArtifactPath = client.getArtifactLocation(artifactId.toEntityId());

    //Make sure the two paths arent the same and that the old one is gone
    Assert.assertNotEquals(artifactPath, newArtifactPath);
    Assert.assertTrue(newArtifactPath.exists());
  }

  private class NoOpArtifactManager implements ArtifactManager {
    @Override
    public List<ArtifactInfo> listArtifacts() throws IOException, AccessException {
      return Collections.emptyList();
    }

    @Override
    public List<ArtifactInfo> listArtifacts(String namespace) throws IOException, AccessException {
      return Collections.emptyList();
    }

    @Override
    public CloseableClassLoader createClassLoader(ArtifactInfo artifactInfo,
        @Nullable ClassLoader parentClassLoader)
        throws IOException, AccessException {
      return null;
    }

    @Override
    public CloseableClassLoader createClassLoader(String namespace, ArtifactInfo artifactInfo,
        @Nullable ClassLoader parentClassLoader)
        throws IOException, AccessException {
      return null;
    }
  }
}

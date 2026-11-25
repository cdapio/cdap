/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.gateway.handlers.FileFetcherHttpHandlerInternal;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.authorization.DefaultAccessEnforcer;
import io.cdap.cdap.security.authorization.InternalAccessEnforcer;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import io.cdap.common.http.HttpContentConsumer;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link FileFetcherHttpHandlerInternal}.
 */
public class FileFetcherHttpHandlerInternalTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Logger LOG = LoggerFactory.getLogger(FileFetcherHttpHandlerInternalTest.class);
  private static final AtomicReference<Principal> CALLER = new AtomicReference<>();
  private static final long TOKEN_TTL_MILLIS = TimeUnit.MINUTES.toMillis(5);
  private static NettyHttpService httpService;
  private static URL baseURL;
  private static NettyHttpService internalAuthHttpService;
  private static URL internalAuthBaseUrl;
  private static TokenManager tokenManager;
  private static AccessTokenCodec accessTokenCodec;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocalLocationModule(),
      new IOModule(),
      authModule(NoOpAccessController.class));

    FileFetcherHttpHandlerInternal handler = injector.getInstance(FileFetcherHttpHandlerInternal.class);

    httpService = NettyHttpService.builder("FileFetcherHttpHandlerInternalTest")
      .setHttpHandlers(handler)
      .setExceptionHandler(new HttpExceptionHandler())
      .build();

    httpService.start();

    InetSocketAddress addr = httpService.getBindAddress();
    baseURL = new URL(String.format("http://%s:%d", addr.getHostName(), addr.getPort()));

    CConfiguration internalAuthConf = CConfiguration.copy(cConf);
    internalAuthConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    Injector internalAuthInjector = Guice.createInjector(
        new ConfigModule(internalAuthConf),
        new LocalLocationModule(),
        new IOModule(),
        new CoreSecurityRuntimeModule().getInMemoryModules(),
        authModule(InternalAccessEnforcer.class));
    tokenManager = internalAuthInjector.getInstance(TokenManager.class);
    Services.startAndWait(tokenManager);
    accessTokenCodec = internalAuthInjector.getInstance(AccessTokenCodec.class);

    internalAuthHttpService = NettyHttpService.builder("FileFetcherHttpHandlerInternalAuthTest")
        .setHttpHandlers(internalAuthInjector.getInstance(FileFetcherHttpHandlerInternal.class))
        .setExceptionHandler(new HttpExceptionHandler())
        .build();
    internalAuthHttpService.start();

    InetSocketAddress internalAuthAddr = internalAuthHttpService.getBindAddress();
    internalAuthBaseUrl = new URL(String.format("http://%s:%d", internalAuthAddr.getHostName(),
        internalAuthAddr.getPort()));
  }

  @AfterClass
  public static void stop() throws Exception {
    httpService.stop();
    internalAuthHttpService.stop();
    Services.stopAndWait(tokenManager);
  }

  @Before
  public void resetCaller() {
    CALLER.set(principal(null));
  }

  @Test
  public void testSuccess() throws Exception {
    // Create a file.
    File srcFile = TEMP_FOLDER.newFile("src_file");
    try (FileOutputStream out = new FileOutputStream(srcFile)) {
      byte[] bytes = new byte[256 * 1024 + 7];
      new Random().nextBytes(bytes);
      out.write(bytes);
    }

    // Download the file.
    File targetFile = new File(TEMP_FOLDER.newFolder(), "dst_file");
    Location dst = Locations.toLocation(targetFile);
    HttpResponse httpResponse = download(srcFile, dst);
    Assert.assertEquals(HttpResponseStatus.OK.code(), httpResponse.getResponseCode());

    // Verify the source and destination files are identical.
    byte[] orgFileContent = Files.readAllBytes(srcFile.toPath());
    byte[] downloadedFileContent = Files.readAllBytes(targetFile.toPath());
    Assert.assertArrayEquals(orgFileContent, downloadedFileContent);
  }

  @Test
  public void testFileNotFound() throws Exception {
    // A non-existent file
    File srcFile = new File(TEMP_FOLDER.getRoot(), "file_dont_exist");

    // Download the file.
    File targetFile = new File(TEMP_FOLDER.newFolder(), "dst_file");
    Location dst = Locations.toLocation(targetFile);

    HttpResponse httpResponse = download(srcFile, dst);
    Assert.assertEquals(httpResponse.getResponseCode(), HttpResponseStatus.NOT_FOUND.code());
  }

  @Test
  public void testSystemServiceCanDownloadWithInternalAuth() throws Exception {
    File srcFile = TEMP_FOLDER.newFile();
    Files.write(srcFile.toPath(), "pod startup file".getBytes(StandardCharsets.UTF_8));
    CALLER.set(principal(
        signedCredential("cdap", UserIdentity.IdentifierType.INTERNAL, TOKEN_TTL_MILLIS)));

    HttpResponse response = downloadWithInternalAuth(srcFile);

    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Assert.assertArrayEquals(Files.readAllBytes(srcFile.toPath()), response.getResponseBody());
  }

  @Test
  public void testOtherCallersForbiddenWithInternalAuth() throws Exception {
    File srcFile = TEMP_FOLDER.newFile();
    Map<String, Credential> credentials = new LinkedHashMap<>();
    credentials.put("no credential", null);
    credentials.put("empty user placeholder",
        new Credential("CDAP-empty-user-credential", Credential.CredentialType.INTERNAL));
    credentials.put("external credential",
        new Credential("user-token", Credential.CredentialType.EXTERNAL));
    credentials.put("malformed internal token",
        new Credential("not-a-token", Credential.CredentialType.INTERNAL));
    credentials.put("external identity",
        signedCredential("cdap", UserIdentity.IdentifierType.EXTERNAL, TOKEN_TTL_MILLIS));
    credentials.put("expired token",
        signedCredential("cdap", UserIdentity.IdentifierType.INTERNAL, -TOKEN_TTL_MILLIS));
    credentials.put("program run token",
        signedCredential(Constants.Security.Authentication.RUNTIME_IDENTITY,
            UserIdentity.IdentifierType.INTERNAL, TOKEN_TTL_MILLIS));

    for (Map.Entry<String, Credential> testCase : credentials.entrySet()) {
      CALLER.set(principal(testCase.getValue()));
      Assert.assertEquals(testCase.getKey(), HttpResponseStatus.FORBIDDEN.code(),
          downloadWithInternalAuth(srcFile).getResponseCode());
    }
  }

  @Test
  public void testMissingFileForbiddenWithInternalAuth() throws Exception {
    CALLER.set(principal(null));

    HttpResponse response =
        downloadWithInternalAuth(new File(TEMP_FOLDER.getRoot(), "file_dont_exist"));

    Assert.assertEquals(HttpResponseStatus.FORBIDDEN.code(), response.getResponseCode());
  }

  private static Module authModule(Class<? extends AccessEnforcer> internalAccessEnforcer) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(AuthenticationContext.class).toInstance(CALLER::get);
        bind(AccessEnforcer.class)
            .annotatedWith(Names.named(DefaultAccessEnforcer.INTERNAL_ACCESS_ENFORCER))
            .to(internalAccessEnforcer);
      }
    };
  }

  private static Principal principal(Credential credential) {
    return new Principal("cdap", Principal.PrincipalType.USER, credential);
  }

  private static Credential signedCredential(String username, UserIdentity.IdentifierType type,
      long ttlMillis) throws IOException {
    long now = System.currentTimeMillis();
    UserIdentity identity =
        new UserIdentity(username, type, Collections.emptyList(), now, now + ttlMillis);
    byte[] token = accessTokenCodec.encode(tokenManager.signIdentifier(identity));
    return new Credential(Base64.getEncoder().encodeToString(token),
        Credential.CredentialType.INTERNAL);
  }

  private static HttpResponse downloadWithInternalAuth(File src) throws IOException {
    URL url = new URL(String.format("%s/v3Internal/location/%s", internalAuthBaseUrl,
        src.toURI().getPath()));
    return HttpRequests.execute(HttpRequest.get(url).build(), new DefaultHttpRequestConfig(false));
  }

  private HttpResponse download(File src, Location dst) throws IOException {
    // Make a request to download the source file.
    URL url = new URL(String.format("%s/v3Internal/location/%s", baseURL, src.toURI().getPath()));
    OutputStream outputStream = dst.getOutputStream();
    HttpRequest request = HttpRequest.builder(
      HttpMethod.GET, url).withContentConsumer(
      new HttpContentConsumer() {
        @Override
        public boolean onReceived(ByteBuffer chunk) {
          try {
            byte[] bytes = new byte[chunk.remaining()];
            chunk.get(bytes, 0, bytes.length);
            outputStream.write(bytes);
          } catch (IOException e) {
            LOG.error("Failed to write to {}", dst.toURI());
            return false;
          }
          return true;
        }

        @Override
        public void onFinished() {
          try {
            outputStream.close();
          } catch (Exception e) {
            LOG.error("Failed to close {}", dst.toURI());
          }
        }
      }).build();
    HttpResponse httpResponse = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    httpResponse.consumeContent();
    return httpResponse;
  }

}

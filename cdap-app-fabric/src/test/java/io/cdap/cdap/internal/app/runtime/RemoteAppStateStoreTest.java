/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.gateway.handlers.AppStateHandler;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.store.state.AppStateKey;
import io.cdap.cdap.internal.app.store.state.AppStateKeyValue;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.http.NettyHttpService;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.hamcrest.CoreMatchers;
import org.hamcrest.CustomTypeSafeMatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/**
 * Tests for {@link RemoteAppStateStore}
 */
public class RemoteAppStateStoreTest {

  private static final String NAMESPACE = "ns1";
  private static final String NOT_FOUND_APP = "non_existing_app";
  private static final String SUCCESS_APP = "app_that_works";
  private static final String ERROR_APP = "app_that_throws_error";
  private static final String TEST_VALUE = "test value";
  private static final String MISSING_KEY = "missing_key";

  private static NettyHttpService httpService;
  private static ApplicationLifecycleService applicationLifecycleService;
  private static CConfiguration cConf;
  private static RemoteClientFactory remoteClientFactory;
  private static Cancellable cancellable;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setup() throws Exception {
    cConf = CConfiguration.create();
    cConf.set("app.state.retry.policy.base.delay.ms", "10");
    cConf.set("app.state.retry.policy.max.delay.ms", "2000");
    cConf.set("app.state.retry.policy.max.retries", "2147483647");
    cConf.set("app.state.retry.policy.max.time.secs", "60");
    cConf.set("app.state.retry.policy.type", "exponential.backoff");
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    NamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    applicationLifecycleService = Mockito.mock(ApplicationLifecycleService.class);
    NamespaceMeta testNameSpace = new NamespaceMeta.Builder()
      .setName(NAMESPACE)
      .setDescription("This is the default namespace, which is automatically created, and is always available.")
      .build();
    namespaceAdmin.create(testNameSpace);
    remoteClientFactory = new RemoteClientFactory(discoveryService,
                                                  new DefaultInternalAuthenticator(new AuthenticationTestContext()));
    httpService = new CommonNettyHttpServiceBuilder(cConf, "appfabric", new NoOpMetricsCollectionService(), null)
      .setHttpHandlers(new AppStateHandler(applicationLifecycleService, namespaceAdmin)).build();
    httpService.start();
    cancellable = discoveryService
      .register(URIScheme.createDiscoverable(Constants.Service.APP_FABRIC_HTTP, httpService));
    setUpMockBehaviorForApplicationLifeCycleService();
  }

  private static void setUpMockBehaviorForApplicationLifeCycleService() throws ApplicationNotFoundException {
    //Throw ApplicationNotFoundException when ever NOT_FOUND_APP is used
    Mockito.doThrow(new ApplicationNotFoundException(new ApplicationId(NAMESPACE, NOT_FOUND_APP)))
      .when(applicationLifecycleService)
      .saveState(Mockito.argThat(new AppNameAppStateKeyValueMatcher("NOT_FOUND_APP", NOT_FOUND_APP)));
    Mockito.doThrow(new ApplicationNotFoundException(new ApplicationId(NAMESPACE, NOT_FOUND_APP)))
      .when(applicationLifecycleService)
      .getState(Mockito.argThat(new AppNameAppStateKeyMatcher("NOT_FOUND_APP", NOT_FOUND_APP)));
    Mockito.doThrow(new ApplicationNotFoundException(new ApplicationId(NAMESPACE, NOT_FOUND_APP)))
        .when(applicationLifecycleService)
        .deleteState(Mockito.argThat(new AppNameAppStateKeyMatcher("NOT_FOUND_APP", NOT_FOUND_APP)));

    //Throw RuntimeException whenever error app is being used
    Mockito.doThrow(new RuntimeException("test")).when(applicationLifecycleService)
      .saveState(Mockito.argThat(new AppNameAppStateKeyValueMatcher("ERROR_APP", ERROR_APP)));
    Mockito.doThrow(new RuntimeException("test")).when(applicationLifecycleService)
      .getState(Mockito.argThat(new AppNameAppStateKeyMatcher("ERROR_APP", ERROR_APP)));
    Mockito.doThrow(new RuntimeException("test")).when(applicationLifecycleService)
        .deleteState(Mockito.argThat(new AppNameAppStateKeyMatcher("ERROR_APP", ERROR_APP)));

    String encodedInvalidKey = Base64.getEncoder().encodeToString(MISSING_KEY.getBytes(StandardCharsets.UTF_8));
    // Different response for valid and invalid keys
    Mockito.when(
        applicationLifecycleService.getState(
          Mockito.argThat(new CustomTypeSafeMatcher<AppStateKey>("valid key match") {
            @Override
            protected boolean matchesSafely(AppStateKey item) {
              return item.getAppName().equals(SUCCESS_APP) && !item.getStateKey().equals(encodedInvalidKey);
            }
          })))
      .thenReturn(Optional.of(TEST_VALUE.getBytes(StandardCharsets.UTF_8)));

    Mockito.when(
        applicationLifecycleService.getState(
          Mockito.argThat(new CustomTypeSafeMatcher<AppStateKey>("invalid key match") {
            @Override
            protected boolean matchesSafely(AppStateKey item) {
              return item.getAppName().equals(SUCCESS_APP) && item.getStateKey().equals(encodedInvalidKey);
            }
          })))
      .thenReturn(Optional.empty());
  }

  /**
   * Simple AppStateKeyValue matcher that matches for appname
   */
  private static class AppNameAppStateKeyValueMatcher extends CustomTypeSafeMatcher<AppStateKeyValue> {
    private final String appName;

    public AppNameAppStateKeyValueMatcher(String description, String appName) {
      super(description);
      this.appName = appName;
    }

    @Override
    protected boolean matchesSafely(AppStateKeyValue item) {
      return item.getAppName().equals(appName);
    }
  }

  /**
   * Simple AppStateKey matcher that matches for appname
   */
  private static class AppNameAppStateKeyMatcher extends CustomTypeSafeMatcher<AppStateKey> {
    private final String appName;

    public AppNameAppStateKeyMatcher(String description, String appName) {
      super(description);
      this.appName = appName;
    }

    @Override
    protected boolean matchesSafely(AppStateKey item) {
      return item.getAppName().equals(appName);
    }
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    cancellable.cancel();
    httpService.stop();
  }

  @Test
  public void testSaveSuccess() throws IOException {
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory,
                                                                      NAMESPACE, SUCCESS_APP);
    remoteAppStateStore.saveState("valid-key", "some_value".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testSaveSuccessWithSpaceInKey() throws IOException {
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory,
                                                                      NAMESPACE, SUCCESS_APP);
    remoteAppStateStore.saveState("valid key with space", "some_value".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testSaveInvalidNamespace() throws IOException {
    expectedException.expectCause(CoreMatchers.isA(NotFoundException.class));
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory,
                                                                      "invalid", "some_app");
    remoteAppStateStore.saveState("some_key", "some_value".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testSaveInvalidApp() throws IOException {
    expectedException.expectCause(CoreMatchers.isA(NotFoundException.class));
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory, NAMESPACE,
                                                                      NOT_FOUND_APP);
    remoteAppStateStore.saveState("some_key", "some value".getBytes());
  }

  @Test
  public void testSaveFail() throws IOException {
    expectedException.expectCause(CoreMatchers.isA(RetryableException.class));
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory, NAMESPACE,
                                                                      ERROR_APP);
    remoteAppStateStore.saveState("some_key", "some value".getBytes());
  }

  @Test
  public void testGetWithValidKeys() throws IOException {
    getWithValidKeys("key");
  }

  @Test
  public void testGetSuccessWithSpaceInKey() throws IOException {
    getWithValidKeys("key with space");
  }

  private void getWithValidKeys(String key) throws IOException {
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory,
                                                                      NAMESPACE, SUCCESS_APP);
    Optional<byte[]> state = remoteAppStateStore.getState(key);
    Assert.assertEquals(TEST_VALUE, new String(state.get()));
  }

  @Test
  public void testGetInvalidNamespace() throws IOException {
    expectedException.expectCause(CoreMatchers.isA(NotFoundException.class));
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory,
                                                                      "invalid", "some_app");
    remoteAppStateStore.getState("some_key");
  }

  @Test
  public void testGetInvalidApp() throws IOException {
    expectedException.expectCause(CoreMatchers.isA(NotFoundException.class));
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory, NAMESPACE,
                                                                      NOT_FOUND_APP);
    remoteAppStateStore.getState("some_key");
  }

  @Test
  public void testGetInvalidKey() throws IOException {
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory, NAMESPACE,
                                                                      SUCCESS_APP);
    Optional<byte[]> state = remoteAppStateStore.getState(MISSING_KEY);
    Assert.assertEquals(Optional.empty(), state);
  }

  @Test
  public void testGetFail() throws IOException {
    expectedException.expectCause(CoreMatchers.isA(RetryableException.class));
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory, NAMESPACE,
                                                                      ERROR_APP);
    remoteAppStateStore.getState("some_key");
  }
  
  @Test
  public void testDeleteSuccess() throws IOException {
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory,
        NAMESPACE, SUCCESS_APP);
    remoteAppStateStore.deleteState("valid-key");
  }

  @Test
  public void testDeleteSuccessWithSpaceInKey() throws IOException {
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory,
        NAMESPACE, SUCCESS_APP);
    remoteAppStateStore.deleteState("valid key with space");
  }

  @Test
  public void testDeleteInvalidNamespace() throws IOException {
    expectedException.expectCause(CoreMatchers.isA(NotFoundException.class));
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory,
        "invalid", "some_app");
    remoteAppStateStore.deleteState("some_key");
  }

  @Test
  public void testDeleteInvalidApp() throws IOException {
    expectedException.expectCause(CoreMatchers.isA(NotFoundException.class));
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory, NAMESPACE,
        NOT_FOUND_APP);
    remoteAppStateStore.deleteState("some_key");
  }

  @Test
  public void testDeleteFail() throws IOException {
    expectedException.expectCause(CoreMatchers.isA(RetryableException.class));
    RemoteAppStateStore remoteAppStateStore = new RemoteAppStateStore(cConf, remoteClientFactory, NAMESPACE,
        ERROR_APP);
    remoteAppStateStore.deleteState("some_key");
  }
}

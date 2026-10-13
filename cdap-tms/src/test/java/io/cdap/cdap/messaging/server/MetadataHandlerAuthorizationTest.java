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

package io.cdap.cdap.messaging.server;

import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the authorization performed by {@link MetadataHandler}.
 *
 * <p>The handler runs inside a real {@link MessagingHttpService} on top of an in-memory messaging
 * service. The {@link AccessEnforcer} is a recording fake that grants permissions from a table, and
 * the {@link AuthenticationContext} returns whatever principal the test chose, so that every test
 * controls who calls and what the enforcer says. Status codes are read from raw HTTP responses.
 */
public class MetadataHandlerAuthorizationTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();
  private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() {
  }.getType();

  private static final NamespaceId ALPHA = new NamespaceId("alpha");
  private static final NamespaceId BETA = new NamespaceId("beta");
  private static final String TOPIC = "t1";

  private static final Principal END_USER = new Principal("alice", Principal.PrincipalType.USER,
      new Credential("user-token", Credential.CredentialType.EXTERNAL));
  private static final Principal INTERNAL_SERVICE = new Principal("system",
      Principal.PrincipalType.USER, new Credential("internal-token", Credential.CredentialType.INTERNAL));

  private final RecordingAccessEnforcer accessEnforcer = new RecordingAccessEnforcer();
  private final AtomicReference<Principal> currentPrincipal = new AtomicReference<>(END_USER);

  private MessagingHttpService httpService;
  private MessagingService messagingService;
  private Discoverable messagingServer;

  @After
  public void afterTest() {
    if (httpService != null) {
      httpService.stopAndWait();
    }
  }

  @Test
  public void testNoPermissionIsForbiddenAndChangesNothing() throws Exception {
    startService(false);
    for (NamespaceId namespace : Arrays.asList(ALPHA, NamespaceId.SYSTEM)) {
      createTopicDirectly(namespace, TOPIC);

      assertAllEndpoints(403, namespace);

      Assert.assertEquals(Collections.singletonList(namespace.topic(TOPIC)),
          messagingService.listTopics(namespace));
      Assert.assertEquals("100", getTtl(namespace.topic(TOPIC)));
    }
  }

  @Test
  public void testNoPermissionIsForbiddenEvenIfTheTopicDoesNotExist() throws Exception {
    startService(false);

    // A caller without permission must get the same answer whether or not the topic exists.
    assertAllEndpoints(403, ALPHA);

    Assert.assertTrue(messagingService.listTopics(ALPHA).isEmpty());
  }

  @Test
  public void testMalformedBodyIsForbiddenBeforeItIsParsed() throws Exception {
    startService(false);

    assertStatus(403, send(HttpMethod.PUT, ALPHA.getNamespace(), "/topics/" + TOPIC, "not json"));
    Assert.assertTrue(messagingService.listTopics(ALPHA).isEmpty());

    // A caller with permission still gets the validation error.
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE);
    assertStatus(400, send(HttpMethod.PUT, ALPHA.getNamespace(), "/topics/" + TOPIC, "not json"));
  }

  @Test
  public void testInvalidTopicNameIsForbiddenBeforeItIsValidated() throws Exception {
    startService(false);

    assertStatus(403, send(HttpMethod.PUT, ALPHA.getNamespace(), "/topics/bad.topic", null));

    // A caller with permission still gets the validation error.
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE);
    assertStatus(400, send(HttpMethod.PUT, ALPHA.getNamespace(), "/topics/bad.topic", null));
  }

  @Test
  public void testInvalidNamespaceNameIsRejectedWithoutAskingTheEnforcer() throws Exception {
    startService(false);

    // There is no namespace entity to authorize, so the request fails before the enforcer is asked.
    assertStatus(400, send(HttpMethod.PUT, "bad-namespace", "/topics/" + TOPIC, null));

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
  }

  @Test
  public void testUpdatePermissionAllowsCreateUpdateAndDelete() throws Exception {
    startService(false);
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE);
    TopicId topicId = ALPHA.topic(TOPIC);

    assertStatus(200, send(Endpoint.CREATE_TOPIC, ALPHA));
    Assert.assertEquals("100", getTtl(topicId));

    assertStatus(200, send(Endpoint.UPDATE_TOPIC, ALPHA));
    Assert.assertEquals("200", getTtl(topicId));

    assertStatus(200, send(Endpoint.DELETE_TOPIC, ALPHA));
    Assert.assertTrue(messagingService.listTopics(ALPHA).isEmpty());
  }

  @Test
  public void testGetPermissionAllowsOnlyReads() throws Exception {
    startService(false);
    createTopicDirectly(ALPHA, TOPIC);
    accessEnforcer.grant(ALPHA, StandardPermission.GET);

    assertStatus(200, send(Endpoint.GET_TOPIC, ALPHA));
    assertStatus(200, send(Endpoint.LIST_TOPICS, ALPHA));
    assertStatus(403, send(Endpoint.CREATE_TOPIC, ALPHA));
    assertStatus(403, send(Endpoint.UPDATE_TOPIC, ALPHA));
    assertStatus(403, send(Endpoint.DELETE_TOPIC, ALPHA));

    Assert.assertEquals(Collections.singletonList(ALPHA.topic(TOPIC)), messagingService.listTopics(ALPHA));
    Assert.assertEquals("100", getTtl(ALPHA.topic(TOPIC)));
  }

  @Test
  public void testPermissionsOnOneNamespaceDoNotApplyToAnother() throws Exception {
    startService(false);
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);
    createTopicDirectly(BETA, TOPIC);

    assertAllEndpoints(403, BETA);

    Assert.assertEquals(Collections.singletonList(BETA.topic(TOPIC)), messagingService.listTopics(BETA));
    Assert.assertEquals("100", getTtl(BETA.topic(TOPIC)));
    assertStatus(200, send(Endpoint.CREATE_TOPIC, ALPHA));
  }

  @Test
  public void testListedTopicsKeepTheirOrder() throws Exception {
    startService(false);
    createTopicDirectly(ALPHA, "t1");
    createTopicDirectly(ALPHA, "t2");
    accessEnforcer.grant(ALPHA, StandardPermission.GET);

    HttpResponse response = send(Endpoint.LIST_TOPICS, ALPHA);

    assertStatus(200, response);
    Assert.assertEquals(Arrays.asList("t1", "t2"),
        GSON.fromJson(response.getResponseBodyAsString(), STRING_LIST_TYPE));
  }

  @Test
  public void testAuthorizedCallersStillGetTopicErrors() throws Exception {
    startService(false);
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);

    assertStatus(404, send(Endpoint.GET_TOPIC, ALPHA));
    assertStatus(404, send(Endpoint.UPDATE_TOPIC, ALPHA));
    assertStatus(404, send(Endpoint.DELETE_TOPIC, ALPHA));
    assertStatus(200, send(Endpoint.CREATE_TOPIC, ALPHA));
    assertStatus(409, send(Endpoint.CREATE_TOPIC, ALPHA));
  }

  @Test
  public void testEachEndpointChecksTheNamespaceOnceAsTheCaller() throws Exception {
    startService(false);
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);

    for (Endpoint endpoint : Endpoint.values()) {
      accessEnforcer.clearChecks();

      assertStatus(200, send(endpoint, ALPHA));

      assertSingleCheck(endpoint, ALPHA, END_USER);
    }
  }

  @Test
  public void testEnforcerFailureDeniesTheRequest() throws Exception {
    startService(false);
    createTopicDirectly(ALPHA, TOPIC);
    accessEnforcer.failWith(new IllegalStateException("The authorization backend is down"));

    assertAllEndpoints(500, ALPHA);

    Assert.assertEquals(Collections.singletonList(ALPHA.topic(TOPIC)), messagingService.listTopics(ALPHA));
    Assert.assertEquals("100", getTtl(ALPHA.topic(TOPIC)));
  }

  @Test
  public void testSystemNamespaceIsLeftToTheEnforcerWhenInternalAuthIsOff() throws Exception {
    startService(false);
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    assertAllEndpoints(200, NamespaceId.SYSTEM);
  }

  @Test
  public void testSystemNamespaceRejectsCredentialsThatAreNotInternal() throws Exception {
    startService(true);
    // The enforcer would approve everything, so only the credential type can stop these requests.
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);
    for (Credential.CredentialType type : Credential.CredentialType.values()) {
      if (type == Credential.CredentialType.INTERNAL) {
        continue;
      }
      // Even a principal that is called "system" is not an internal service without the credential.
      currentPrincipal.set(new Principal("system", Principal.PrincipalType.USER, new Credential("token", type)));

      assertAllEndpoints(403, NamespaceId.SYSTEM);
    }

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(messagingService.listTopics(NamespaceId.SYSTEM).isEmpty());
  }

  @Test
  public void testSystemNamespaceRejectsPrincipalWithoutCredential() throws Exception {
    startService(true);
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);
    currentPrincipal.set(new Principal("system", Principal.PrincipalType.USER));

    assertAllEndpoints(403, NamespaceId.SYSTEM);

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(messagingService.listTopics(NamespaceId.SYSTEM).isEmpty());
  }

  @Test
  public void testSystemNamespaceRejectsEncodedSpellingOfItsName() throws Exception {
    startService(true);
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    // "%73" is "s": the namespace the handler authorizes is the decoded one, which is "system".
    assertStatus(403, send(HttpMethod.PUT, "%73ystem", "/topics/" + TOPIC, null));

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(messagingService.listTopics(NamespaceId.SYSTEM).isEmpty());
  }

  @Test
  public void testSystemNamespaceAcceptsInternalCredential() throws Exception {
    startService(true);
    currentPrincipal.set(INTERNAL_SERVICE);
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    for (Endpoint endpoint : Endpoint.values()) {
      accessEnforcer.clearChecks();

      assertStatus(200, send(endpoint, NamespaceId.SYSTEM));

      assertSingleCheck(endpoint, NamespaceId.SYSTEM, INTERNAL_SERVICE);
    }
  }

  @Test
  public void testInternalCredentialStillNeedsTheApprovalOfTheEnforcer() throws Exception {
    startService(true);
    currentPrincipal.set(INTERNAL_SERVICE);

    assertAllEndpoints(403, NamespaceId.SYSTEM);

    Assert.assertEquals(Endpoint.values().length, accessEnforcer.checks().size());
    Assert.assertTrue(messagingService.listTopics(NamespaceId.SYSTEM).isEmpty());
  }

  @Test
  public void testInternalCredentialCanManageUserNamespaces() throws Exception {
    startService(true);
    currentPrincipal.set(INTERNAL_SERVICE);
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);

    assertAllEndpoints(200, ALPHA);
  }

  @Test
  public void testUserNamespaceStillNeedsPermissionWhenInternalAuthIsOn() throws Exception {
    startService(true);

    assertAllEndpoints(403, ALPHA);

    Assert.assertTrue(messagingService.listTopics(ALPHA).isEmpty());
  }

  @Test
  public void testUserWithPermissionCanManageUserNamespaceWhenInternalAuthIsOn() throws Exception {
    startService(true);
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);

    assertAllEndpoints(200, ALPHA);
  }

  /**
   * Starts a {@link MessagingHttpService} on an in-memory messaging service. Everything the handler
   * asks about the caller and about permissions is answered by the fakes of this test.
   */
  private void startService(boolean internalAuthEnabled) throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS, InetAddress.getLocalHost().getHostName());
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, internalAuthEnabled);

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        RemoteAuthenticatorModules.getNoOpModule(),
        new InMemoryDiscoveryModule(),
        Modules.override(
            new AuthenticationContextModules().getNoOpModule(),
            new AuthorizationEnforcementModule().getNoOpModules()
        ).with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(AuthenticationContext.class).toInstance(currentPrincipal::get);
            bind(AccessEnforcer.class).toInstance(accessEnforcer);
          }
        }),
        new MessagingServerRuntimeModule().getInMemoryModules(),
        new NoOpAuditLogModule(),
        new UserCredentialAeadEncryptionModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).toInstance(new NoOpMetricsCollectionService());
          }
        }
    );

    messagingService = injector.getInstance(MessagingService.class);
    httpService = injector.getInstance(MessagingHttpService.class);
    httpService.startAndWait();
    messagingServer = Iterables.getOnlyElement(
        injector.getInstance(DiscoveryServiceClient.class).discover(Constants.Service.MESSAGING_SERVICE));
  }

  /**
   * Creates a topic straight on the messaging service, bypassing the HTTP handler and its checks.
   */
  private void createTopicDirectly(NamespaceId namespace, String topic) throws Exception {
    messagingService.createTopic(new DefaultTopicMetadata(namespace.topic(topic), "ttl", "100"));
  }

  private String getTtl(TopicId topicId) throws Exception {
    return messagingService.getTopicMetadataProperties(topicId).get("ttl");
  }

  private HttpResponse send(Endpoint endpoint, NamespaceId namespace) throws IOException {
    return send(endpoint.method, namespace.getNamespace(), String.format(endpoint.pathFormat, TOPIC),
        endpoint.body);
  }

  private HttpResponse send(HttpMethod method, String namespace, String path, @Nullable String body)
      throws IOException {
    URL url = URIScheme.createURI(messagingServer, "%s", "/v1/namespaces/" + namespace + path).toURL();
    HttpRequest.Builder request = HttpRequest.builder(method, url);
    if (body != null) {
      request.withBody(body);
    }
    return HttpRequests.execute(request.build());
  }

  private void assertStatus(int expectedStatus, HttpResponse response) {
    Assert.assertEquals(response.getResponseBodyAsString(), expectedStatus, response.getResponseCode());
  }

  /**
   * Calls every endpoint once and expects the same status each time. The endpoints are called in the
   * order of {@link Endpoint}, so a caller who is allowed everything creates, updates, reads, lists
   * and deletes the topic without ever hitting an error caused by an earlier call.
   */
  private void assertAllEndpoints(int expectedStatus, NamespaceId namespace) throws IOException {
    for (Endpoint endpoint : Endpoint.values()) {
      HttpResponse response = send(endpoint, namespace);
      Assert.assertEquals(endpoint + " in " + namespace + ": " + response.getResponseBodyAsString(),
          expectedStatus, response.getResponseCode());
    }
  }

  private void assertSingleCheck(Endpoint endpoint, NamespaceId namespace, Principal principal) {
    List<Check> checks = accessEnforcer.checks();
    Assert.assertEquals(endpoint.toString(), 1, checks.size());
    Check check = checks.get(0);
    Assert.assertEquals(endpoint.toString(), namespace, check.entity);
    Assert.assertSame(endpoint.toString(), principal, check.principal);
    Assert.assertEquals(endpoint.toString(), Collections.singleton(endpoint.requiredPermission), check.permissions);
  }

  /**
   * The endpoints of {@link MetadataHandler}, in an order in which each one succeeds after the ones
   * before it: the topic is created, updated, read, listed and finally deleted.
   */
  private enum Endpoint {
    CREATE_TOPIC(HttpMethod.PUT, "/topics/%s", "{\"ttl\":\"100\"}", StandardPermission.UPDATE),
    UPDATE_TOPIC(HttpMethod.PUT, "/topics/%s/properties", "{\"ttl\":\"200\"}", StandardPermission.UPDATE),
    GET_TOPIC(HttpMethod.GET, "/topics/%s", null, StandardPermission.GET),
    LIST_TOPICS(HttpMethod.GET, "/topics", null, StandardPermission.GET),
    DELETE_TOPIC(HttpMethod.DELETE, "/topics/%s", null, StandardPermission.UPDATE);

    private final HttpMethod method;
    private final String pathFormat;
    @Nullable
    private final String body;
    private final StandardPermission requiredPermission;

    Endpoint(HttpMethod method, String pathFormat, @Nullable String body,
        StandardPermission requiredPermission) {
      this.method = method;
      this.pathFormat = pathFormat;
      this.body = body;
      this.requiredPermission = requiredPermission;
    }
  }

  /**
   * One call to {@link AccessEnforcer#enforce}.
   */
  private static final class Check {

    private final EntityId entity;
    private final Principal principal;
    private final Set<Permission> permissions;

    private Check(EntityId entity, Principal principal, Set<? extends Permission> permissions) {
      this.entity = entity;
      this.principal = principal;
      this.permissions = new HashSet<>(permissions);
    }
  }

  /**
   * An {@link AccessEnforcer} that approves what it was told to grant, records every check, and can
   * be told to fail. Anything other than {@link #enforce} is unexpected and fails loudly.
   */
  private static final class RecordingAccessEnforcer implements AccessEnforcer {

    private final Map<EntityId, Set<Permission>> grants = new ConcurrentHashMap<>();
    private final List<Check> checks = new CopyOnWriteArrayList<>();
    private volatile RuntimeException failure;

    void grant(EntityId entity, Permission... permissions) {
      grants.computeIfAbsent(entity, key -> ConcurrentHashMap.newKeySet()).addAll(Arrays.asList(permissions));
    }

    void failWith(RuntimeException failure) {
      this.failure = failure;
    }

    List<Check> checks() {
      return new ArrayList<>(checks);
    }

    void clearChecks() {
      checks.clear();
    }

    @Override
    public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
        throws UnauthorizedException {
      checks.add(new Check(entity, principal, permissions));
      if (failure != null) {
        throw failure;
      }
      if (!grants.getOrDefault(entity, Collections.emptySet()).containsAll(permissions)) {
        throw new UnauthorizedException(principal, permissions, entity);
      }
    }

    @Override
    public void enforceOnParent(io.cdap.cdap.proto.element.EntityType entityType, EntityId parentId,
        Principal principal, Permission permission) {
      throw new UnsupportedOperationException("enforceOnParent is not expected");
    }

    @Override
    public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal) {
      throw new UnsupportedOperationException("isVisible is not expected");
    }
  }
}

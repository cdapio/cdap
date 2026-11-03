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
import com.google.common.collect.Iterators;
import com.google.common.net.HttpHeaders;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.internal.io.ExposedByteArrayOutputStream;
import io.cdap.cdap.messaging.DefaultMessageFetchRequest;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.Schemas;
import io.cdap.cdap.messaging.client.AbstractClientMessagingService;
import io.cdap.cdap.messaging.client.DefaultClientMessagingService;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.messaging.spi.RollbackDetail;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.InternalAccessEnforcer;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.tephra.TxConstants;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the authorization performed by {@link FetchHandler} and {@link StoreHandler}.
 *
 * <p>The handlers run inside a real {@link MessagingHttpService} on top of an in-memory messaging
 * service. The {@link AccessEnforcer} is a recording fake that grants permissions from a table, and
 * the {@link AuthenticationContext} returns whatever principal the test chose, so that every test
 * controls who calls and what the enforcer says. Status codes are read from raw HTTP responses.
 */
public class StoreAndFetchHandlerAuthorizationTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final NamespaceId ALPHA = new NamespaceId("alpha");
  private static final NamespaceId BETA = new NamespaceId("beta");
  private static final String TOPIC = "t1";

  private static final Principal END_USER = new Principal("alice", Principal.PrincipalType.USER,
      new Credential("user-token", Credential.CredentialType.EXTERNAL));
  private static final Principal INTERNAL_SERVICE = new Principal("system",
      Principal.PrincipalType.USER,
      new Credential("internal-token", Credential.CredentialType.INTERNAL));
  private static final Principal EMPTY_WORKER_PRINCIPAL = new Principal(
      Constants.Security.Authentication.EMPTY_WORKER_USER_ID,
      Principal.PrincipalType.USER,
      new Credential(Constants.Security.Authentication.EMPTY_WORKER_CREDENTIAL,
          Credential.CredentialType.INTERNAL));
  private static final Principal EMPTY_USER_PRINCIPAL = new Principal(
      Constants.Security.Authentication.EMPTY_USER_ID,
      Principal.PrincipalType.USER,
      new Credential(Constants.Security.Authentication.EMPTY_USER_CREDENTIAL,
          Credential.CredentialType.INTERNAL));

  private final RecordingAccessEnforcer accessEnforcer = new RecordingAccessEnforcer();
  private final AtomicReference<Principal> currentPrincipal = new AtomicReference<>(END_USER);

  private MessagingHttpService httpService;
  private MessagingService messagingService;
  private Discoverable messagingServer;
  private TokenManager tokenManager;
  private File tempDir;

  @After
  public void afterTest() {
    SecurityRequestContext.reset();
    if (httpService != null) {
      httpService.stopAndWait();
    }
    if (tokenManager != null) {
      tokenManager.stopAndWait();
      tokenManager = null;
    }
  }

  @Test
  public void testNoPermissionIsForbiddenAndChangesNothing() throws Exception {
    startService(false);
    for (NamespaceId namespace : Arrays.asList(ALPHA, NamespaceId.SYSTEM)) {
      TopicId topicId = namespace.topic(TOPIC);
      createTopicDirectly(topicId);

      assertAllEndpoints(403, namespace);

      Assert.assertTrue(fetchDirectly(topicId).isEmpty());
      assertNoTempFilesCreated();
    }
  }

  @Test
  public void testNoPermissionIsForbiddenEvenIfTheTopicDoesNotExist() throws Exception {
    startService(false);

    assertAllEndpoints(403, ALPHA);
    assertNoTempFilesCreated();
  }

  @Test
  public void testMissingContentTypeIsForbiddenBeforeItIsChecked() throws Exception {
    startService(false);
    createTopicDirectly(ALPHA.topic(TOPIC));

    for (Endpoint endpoint : Arrays.asList(Endpoint.PUBLISH, Endpoint.STORE, Endpoint.POLL)) {
      assertStatus(403, send(ALPHA.getNamespace(), TOPIC, endpoint.action, null, endpoint.body()));
    }
    assertNoTempFilesCreated();

    // A caller with permission still gets the content-type validation error (400).
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);
    for (Endpoint endpoint : Arrays.asList(Endpoint.PUBLISH, Endpoint.STORE, Endpoint.POLL)) {
      assertStatus(400, send(ALPHA.getNamespace(), TOPIC, endpoint.action, null, endpoint.body()));
    }
  }

  @Test
  public void testInvalidTopicNameIsForbiddenBeforeItIsValidated() throws Exception {
    startService(false);

    for (Endpoint endpoint : Endpoint.values()) {
      assertStatus(403, send(ALPHA.getNamespace(), "bad.topic", endpoint.action,
          "avro/binary", endpoint.body()));
    }
    assertNoTempFilesCreated();

    // A caller with permission still gets the topic validation error (400 in MessagingHttpService
    // via its CDAP-7688 IllegalArgumentException handler; 500 in AppFabricServer).
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);
    for (Endpoint endpoint : Endpoint.values()) {
      assertStatus(400, send(ALPHA.getNamespace(), "bad.topic", endpoint.action,
          "avro/binary", endpoint.body()));
    }
  }

  @Test
  public void testInvalidNamespaceNameIsRejectedWithoutAskingTheEnforcer() throws Exception {
    startService(false);

    // Fails before the enforcer is asked (400 in MessagingHttpService; 500 in AppFabricServer).
    for (Endpoint endpoint : Endpoint.values()) {
      assertStatus(400, send("bad-namespace", TOPIC, endpoint.action,
          "avro/binary", endpoint.body()));
    }

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testUpdatePermissionAllowsPublishStoreAndRollbackOnly() throws Exception {
    startService(false);
    TopicId topicId = ALPHA.topic(TOPIC);
    createTopicDirectly(topicId);
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE);

    assertStatus(200, send(Endpoint.PUBLISH, ALPHA));
    assertStatus(200, send(Endpoint.STORE, ALPHA));
    assertStatus(200, send(Endpoint.ROLLBACK, ALPHA));
    assertStatus(403, send(Endpoint.POLL, ALPHA));

    Assert.assertEquals(Collections.singletonList("msg"), fetchDirectly(topicId));
  }

  @Test
  public void testGetPermissionAllowsPollOnly() throws Exception {
    startService(false);
    TopicId topicId = ALPHA.topic(TOPIC);
    createTopicDirectly(topicId);
    accessEnforcer.grant(ALPHA, StandardPermission.GET);

    assertStatus(200, send(Endpoint.POLL, ALPHA));
    assertStatus(403, send(Endpoint.PUBLISH, ALPHA));
    assertStatus(403, send(Endpoint.STORE, ALPHA));
    assertStatus(403, send(Endpoint.ROLLBACK, ALPHA));

    Assert.assertTrue(fetchDirectly(topicId).isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testPermissionsOnOneNamespaceDoNotApplyToAnother() throws Exception {
    startService(false);
    createTopicDirectly(ALPHA.topic(TOPIC));
    createTopicDirectly(BETA.topic(TOPIC));
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);

    assertAllEndpoints(403, BETA);
    Assert.assertTrue(fetchDirectly(BETA.topic(TOPIC)).isEmpty());

    assertAllEndpoints(200, ALPHA);
    Assert.assertEquals(Collections.singletonList("msg"), fetchDirectly(ALPHA.topic(TOPIC)));
  }

  @Test
  public void testEachEndpointChecksTheNamespaceOnceAsTheCaller() throws Exception {
    startService(false);
    createTopicDirectly(ALPHA.topic(TOPIC));
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
    createTopicDirectly(ALPHA.topic(TOPIC));
    accessEnforcer.failWith(new IllegalStateException("The authorization backend is down"));

    assertAllEndpoints(500, ALPHA);
    Assert.assertTrue(fetchDirectly(ALPHA.topic(TOPIC)).isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testSystemNamespaceIsLeftToTheEnforcerWhenInternalAuthIsOff() throws Exception {
    startService(false);
    for (String topic : Arrays.asList(TOPIC, "preview", "metrics0", "programstatusevent")) {
      createTopicDirectly(NamespaceId.SYSTEM.topic(topic));
    }
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    for (String topic : Arrays.asList(TOPIC, "preview", "metrics0", "programstatusevent")) {
      assertAllEndpoints(200, NamespaceId.SYSTEM, topic);
    }
  }

  @Test
  public void testSystemNamespaceRejectsCredentialsThatAreNotInternal() throws Exception {
    startService(true);
    for (String topic : Arrays.asList(TOPIC, "metrics0", "programstatusevent", "previewlog0")) {
      createTopicDirectly(NamespaceId.SYSTEM.topic(topic));
    }
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    for (Credential.CredentialType type : Credential.CredentialType.values()) {
      if (type == Credential.CredentialType.INTERNAL) {
        continue;
      }
      for (String value : Arrays.asList("token", "empty-worker-user-credential")) {
        currentPrincipal.set(
            new Principal("system", Principal.PrincipalType.USER, new Credential(value, type)));

        for (String topic : Arrays.asList(TOPIC, "metrics0", "programstatusevent", "previewlog0")) {
          assertAllEndpoints(403, NamespaceId.SYSTEM, topic);
        }
      }
    }

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    for (String topic : Arrays.asList(TOPIC, "metrics0", "programstatusevent", "previewlog0")) {
      Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(topic)).isEmpty());
    }
    assertNoTempFilesCreated();
  }

  @Test
  public void testUserCredentialCanPublishToPreviewTopicOnlyInSystemNamespace() throws Exception {
    startService(true);
    TopicId previewTopicId = NamespaceId.SYSTEM.topic("preview");
    createTopicDirectly(previewTopicId);
    for (String otherTopic : Arrays.asList("metrics0", "programstatusevent", "previewlog0")) {
      createTopicDirectly(NamespaceId.SYSTEM.topic(otherTopic));
    }
    currentPrincipal.set(END_USER);
    // The enforcer grants nothing; preview publish must succeed without calling the enforcer.

    assertStatus(200, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, "preview"));
    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertEquals(Collections.singletonList("msg"), fetchDirectly(previewTopicId));

    // Even if the enforcer granted SYSTEM permissions, non-internal credentials cannot publish to
    // other system topics and cannot store, rollback, poll, or administer system:preview.
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);
    for (String otherTopic : Arrays.asList("metrics0", "programstatusevent", "previewlog0")) {
      HttpResponse response = send(Endpoint.PUBLISH, NamespaceId.SYSTEM, otherTopic);
      assertStatus(403, response);
      Assert.assertTrue(response.getResponseBodyAsString().contains(
          "Only internal services can access topics of the system namespace."));
      Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(otherTopic)).isEmpty());
    }
    for (Endpoint endpoint : Arrays.asList(Endpoint.STORE, Endpoint.ROLLBACK, Endpoint.POLL)) {
      HttpResponse response = send(endpoint, NamespaceId.SYSTEM, "preview");
      assertStatus(403, response);
      Assert.assertTrue(response.getResponseBodyAsString().contains(
          "Only internal services can access topics of the system namespace."));
    }
    assertStatus(403, sendTopicAdmin(HttpMethod.GET, "system", "preview"));
    assertStatus(403, sendTopicAdmin(HttpMethod.PUT, "system", "preview"));
    assertStatus(403, sendTopicAdmin(HttpMethod.DELETE, "system", "preview"));
    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testSystemNamespaceRejectsPrincipalWithoutCredential() throws Exception {
    startService(true);
    createTopicDirectly(NamespaceId.SYSTEM.topic(TOPIC));
    createTopicDirectly(NamespaceId.SYSTEM.topic("preview"));
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    List<Principal> invalidPrincipals = Arrays.asList(
        new Principal("system", Principal.PrincipalType.USER),
        new Principal("alice", Principal.PrincipalType.USER,
            new Credential("", Credential.CredentialType.EXTERNAL)),
        new Principal("alice", Principal.PrincipalType.USER,
            new Credential(null, Credential.CredentialType.EXTERNAL)),
        new Principal("alice", Principal.PrincipalType.USER,
            new Credential("user-token", null)));

    for (Principal principal : invalidPrincipals) {
      currentPrincipal.set(principal);
      assertAllEndpoints(403, NamespaceId.SYSTEM, TOPIC);
      assertAllEndpoints(403, NamespaceId.SYSTEM, "preview");
    }

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(TOPIC)).isEmpty());
    Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic("preview")).isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testSystemNamespaceRejectsNullPrincipal() throws Exception {
    startService(true);
    createTopicDirectly(NamespaceId.SYSTEM.topic(TOPIC));
    createTopicDirectly(NamespaceId.SYSTEM.topic("preview"));
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);
    currentPrincipal.set(null);

    assertAllEndpoints(403, NamespaceId.SYSTEM, TOPIC);
    assertAllEndpoints(403, NamespaceId.SYSTEM, "preview");

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(TOPIC)).isEmpty());
    Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic("preview")).isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testSystemNamespaceRejectsEncodedSpellingOfItsName() throws Exception {
    startService(true);
    createTopicDirectly(NamespaceId.SYSTEM.topic(TOPIC));
    createTopicDirectly(NamespaceId.SYSTEM.topic("programstatusevent"));
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    for (Endpoint endpoint : Endpoint.values()) {
      assertStatus(403, send("%73ystem", TOPIC, endpoint.action, "avro/binary", endpoint.body()));
    }
    currentPrincipal.set(EMPTY_WORKER_PRINCIPAL);
    assertStatus(403, send("%73ystem", "%70rogramstatusevent",
        Endpoint.PUBLISH.action, "avro/binary", Endpoint.PUBLISH.body()));

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(TOPIC)).isEmpty());
    Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic("programstatusevent")).isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testSystemNamespaceAcceptsInternalCredential() throws Exception {
    startService(true);
    createTopicDirectly(NamespaceId.SYSTEM.topic(TOPIC));
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
    createTopicDirectly(NamespaceId.SYSTEM.topic(TOPIC));
    currentPrincipal.set(INTERNAL_SERVICE);

    assertAllEndpoints(403, NamespaceId.SYSTEM);

    Assert.assertEquals(Endpoint.values().length, accessEnforcer.checks().size());
    Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(TOPIC)).isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testWorkerPlaceholderCanPublishToAllowedSystemTopicsOnly()
      throws Exception {
    startService(true);
    currentPrincipal.set(EMPTY_WORKER_PRINCIPAL);
    // The enforcer grants nothing and would fail if called for publish on system.

    List<String> allowedTopics = Arrays.asList(
        "metrics0", "metrics9", "previewlog0", "preview", "auditlogevent0", "auditlogevent2");
    for (String topic : allowedTopics) {
      TopicId topicId = NamespaceId.SYSTEM.topic(topic);
      createTopicDirectly(topicId);

      assertStatus(200, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, topic));
      Assert.assertTrue(accessEnforcer.checks().isEmpty());
      Assert.assertEquals(Collections.singletonList("msg"), fetchDirectly(topicId));
    }

    // Non-worker system topics are rejected with 403 before calling the enforcer.
    List<String> disallowedTopics = Arrays.asList(
        "programstatusevent", "metadata", "metrics10", "previewlog1", "auditlogevent3", TOPIC);
    for (String topic : disallowedTopics) {
      TopicId topicId = NamespaceId.SYSTEM.topic(topic);
      createTopicDirectly(topicId);

      HttpResponse response = send(Endpoint.PUBLISH, NamespaceId.SYSTEM, topic);
      assertStatus(403, response);
      Assert.assertTrue(response.getResponseBodyAsString().contains(
          "Workers can only publish to their own topics of the system namespace."));
      Assert.assertTrue(accessEnforcer.checks().isEmpty());
      Assert.assertTrue(fetchDirectly(topicId).isEmpty());
    }
    // Invalid topic names are also forbidden (403) before topic name validation (400).
    assertStatus(403,
        send("system", "bad.topic", Endpoint.PUBLISH.action, "avro/binary",
            Endpoint.PUBLISH.body()));
    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    assertNoTempFilesCreated();

    // Store, rollback, poll, and topic admin on an allowlisted system topic are rejected with 403
    // before calling the enforcer because the worker placeholder is not a signed internal token.
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);
    for (Endpoint endpoint : Arrays.asList(Endpoint.STORE, Endpoint.ROLLBACK, Endpoint.POLL)) {
      assertStatus(403, send(endpoint, NamespaceId.SYSTEM, "metrics0"));
    }
    assertStatus(403, sendTopicAdmin(HttpMethod.GET, "system", "metrics0"));
    assertStatus(403, sendTopicAdmin(HttpMethod.PUT, "system", "metrics0"));
    assertStatus(403, sendTopicAdmin(HttpMethod.DELETE, "system", "metrics0"));
    Assert.assertTrue(accessEnforcer.checks().isEmpty());
  }

  @Test
  public void testWorkerAllowlistAndPreviewTopicFollowConfigOverrides() throws Exception {
    startService(true, conf -> {
      conf.setInt(Constants.Metrics.MESSAGING_TOPIC_NUM, 3);
      conf.setInt(Constants.AuditLogging.AUDIT_LOG_EVENT_NUM_PARTITIONS, 1);
      conf.set(Constants.Preview.MESSAGING_TOPIC, "custompreview");
    });
    for (String topic : Arrays.asList(
        "metrics0", "metrics2", "metrics3", "auditlogevent0", "auditlogevent1",
        "custompreview", "preview", "previewlog0")) {
      createTopicDirectly(NamespaceId.SYSTEM.topic(topic));
    }

    currentPrincipal.set(EMPTY_WORKER_PRINCIPAL);
    for (String allowed : Arrays.asList(
        "metrics0", "metrics2", "auditlogevent0", "custompreview", "previewlog0")) {
      assertStatus(200, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, allowed));
      Assert.assertEquals(
          Collections.singletonList("msg"), fetchDirectly(NamespaceId.SYSTEM.topic(allowed)));
    }
    for (String rejected : Arrays.asList("metrics3", "auditlogevent1", "preview")) {
      assertStatus(403, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, rejected));
      Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(rejected)).isEmpty());
    }

    // Non-internal user credential follows the configured preview topic as well.
    currentPrincipal.set(END_USER);
    assertStatus(200, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, "custompreview"));
    assertStatus(403, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, "preview"));
    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testEmptyOrUnsetPreviewTopicAndEmptyTopicPrefixes() throws Exception {
    // 1. Empty preview topic and empty metrics/audit prefixes: only previewlog0 remains in the
    // worker allowlist, and non-internal user credentials cannot publish to any system topic.
    startService(true, conf -> {
      conf.set(Constants.Preview.MESSAGING_TOPIC, "");
      conf.set(Constants.Metrics.TOPIC_PREFIX, "");
      conf.set(Constants.AuditLogging.AUDIT_LOG_EVENT_TOPIC, "");
    });
    for (String topic : Arrays.asList("preview", "metrics0", "auditlogevent0", "previewlog0")) {
      createTopicDirectly(NamespaceId.SYSTEM.topic(topic));
    }

    currentPrincipal.set(EMPTY_WORKER_PRINCIPAL);
    assertStatus(200, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, "previewlog0"));
    Assert.assertEquals(Collections.singletonList("msg"),
        fetchDirectly(NamespaceId.SYSTEM.topic("previewlog0")));
    for (String rejected : Arrays.asList("preview", "metrics0", "auditlogevent0")) {
      assertStatus(403, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, rejected));
      Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(rejected)).isEmpty());
    }

    currentPrincipal.set(END_USER);
    for (String rejected : Arrays.asList("preview", "previewlog0")) {
      assertStatus(403, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, rejected));
    }
    Assert.assertTrue(accessEnforcer.checks().isEmpty());

    // 2. Unset (null) preview topic and unset (null) metrics/audit prefixes.
    httpService.stopAndWait();
    startService(true, conf -> {
      conf.unset(Constants.Preview.MESSAGING_TOPIC);
      conf.unset(Constants.Metrics.TOPIC_PREFIX);
      conf.unset(Constants.AuditLogging.AUDIT_LOG_EVENT_TOPIC);
    });
    for (String topic : Arrays.asList("preview", "metrics0", "auditlogevent0", "previewlog0")) {
      createTopicDirectly(NamespaceId.SYSTEM.topic(topic));
    }

    currentPrincipal.set(EMPTY_WORKER_PRINCIPAL);
    assertStatus(200, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, "previewlog0"));
    for (String rejected : Arrays.asList("preview", "metrics0", "auditlogevent0")) {
      assertStatus(403, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, rejected));
    }
    currentPrincipal.set(END_USER);
    assertStatus(403, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, "preview"));
    Assert.assertTrue(accessEnforcer.checks().isEmpty());
  }

  @Test
  public void testInternalCredentialPublishingToPreviewTopicStillRequiresEnforcer()
      throws Exception {
    startService(true);
    TopicId previewTopic = NamespaceId.SYSTEM.topic("preview");
    createTopicDirectly(previewTopic);
    currentPrincipal.set(INTERNAL_SERVICE);

    // Without enforcer approval, even a valid internal credential publishing to system:preview
    // must be denied after consulting the enforcer.
    assertStatus(403, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, "preview"));
    assertSingleCheck(Endpoint.PUBLISH, NamespaceId.SYSTEM, INTERNAL_SERVICE);
    Assert.assertTrue(fetchDirectly(previewTopic).isEmpty());

    // With enforcer approval, publishing succeeds and consults the enforcer once.
    accessEnforcer.clearChecks();
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE);
    assertStatus(200, send(Endpoint.PUBLISH, NamespaceId.SYSTEM, "preview"));
    assertSingleCheck(Endpoint.PUBLISH, NamespaceId.SYSTEM, INTERNAL_SERVICE);
    Assert.assertEquals(Collections.singletonList("msg"), fetchDirectly(previewTopic));
  }

  @Test
  public void testMixedCaseSystemNamespaceIsTreatedAsSystemNamespace() throws Exception {
    startService(true);
    List<NamespaceId> mixedCaseSystemNamespaces = Arrays.asList(
        new NamespaceId("SYSTEM"), new NamespaceId("System"), new NamespaceId("sYsTeM"));
    for (NamespaceId mixedNs : mixedCaseSystemNamespaces) {
      createTopicDirectly(mixedNs.topic(TOPIC));
      createTopicDirectly(mixedNs.topic("preview"));
      createTopicDirectly(mixedNs.topic("metrics0"));
      accessEnforcer.grant(mixedNs, StandardPermission.UPDATE, StandardPermission.GET);
    }

    // Non-internal credentials are rejected on non-preview topics and on non-publish endpoints
    // across all mixed-case spellings without calling the enforcer.
    currentPrincipal.set(END_USER);
    for (NamespaceId mixedNs : mixedCaseSystemNamespaces) {
      assertAllEndpoints(403, mixedNs, TOPIC);
      assertAllEndpoints(403, mixedNs, "metrics0");
      for (Endpoint endpoint : Arrays.asList(Endpoint.STORE, Endpoint.ROLLBACK, Endpoint.POLL)) {
        assertStatus(403, send(endpoint, mixedNs, "preview"));
      }
      assertStatus(200, send(Endpoint.PUBLISH, mixedNs, "preview"));
    }
    Assert.assertTrue(accessEnforcer.checks().isEmpty());

    // Worker placeholder can publish only to worker-allowlisted topics on mixed-case system.
    currentPrincipal.set(EMPTY_WORKER_PRINCIPAL);
    for (NamespaceId mixedNs : mixedCaseSystemNamespaces) {
      assertStatus(200, send(Endpoint.PUBLISH, mixedNs, "metrics0"));
      assertStatus(403, send(Endpoint.PUBLISH, mixedNs, TOPIC));
      for (Endpoint endpoint : Arrays.asList(Endpoint.STORE, Endpoint.ROLLBACK, Endpoint.POLL)) {
        assertStatus(403, send(endpoint, mixedNs, "metrics0"));
      }
    }
    Assert.assertTrue(accessEnforcer.checks().isEmpty());

    // Valid internal credentials still go through the enforcer for every endpoint.
    currentPrincipal.set(INTERNAL_SERVICE);
    for (NamespaceId mixedNs : mixedCaseSystemNamespaces) {
      for (Endpoint endpoint : Endpoint.values()) {
        accessEnforcer.clearChecks();
        assertStatus(200, send(endpoint, mixedNs, TOPIC));
        assertSingleCheck(endpoint, mixedNs, INTERNAL_SERVICE);
      }
    }
  }

  @Test
  public void testPlaceholderInternalCredentialsAreRejectedWith403BeforeEnforcer()
      throws Exception {
    startService(true);
    TopicId userTopic = ALPHA.topic(TOPIC);
    TopicId systemTopic = NamespaceId.SYSTEM.topic(TOPIC);
    TopicId previewTopic = NamespaceId.SYSTEM.topic("preview");
    createTopicDirectly(userTopic);
    createTopicDirectly(systemTopic);
    createTopicDirectly(previewTopic);
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    // 1. Worker placeholder on a user namespace is rejected with 403 before calling the enforcer.
    currentPrincipal.set(EMPTY_WORKER_PRINCIPAL);
    assertAllEndpoints(403, ALPHA);

    // 2. Empty user placeholder (from requests with no auth header) is rejected with 403 on both
    // system (including preview and worker topics) and user namespaces before calling the enforcer.
    currentPrincipal.set(EMPTY_USER_PRINCIPAL);
    for (Endpoint endpoint : Endpoint.values()) {
      HttpResponse sysResponse = send(endpoint, NamespaceId.SYSTEM, TOPIC);
      assertStatus(403, sysResponse);
      Assert.assertTrue(sysResponse.getResponseBodyAsString().contains(
          "Only internal services can access topics of the system namespace."));
      HttpResponse previewResponse = send(endpoint, NamespaceId.SYSTEM, "preview");
      assertStatus(403, previewResponse);
      Assert.assertTrue(previewResponse.getResponseBodyAsString().contains(
          "Only internal services can access topics of the system namespace."));
    }
    assertAllEndpoints(403, ALPHA, TOPIC);

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(fetchDirectly(userTopic).isEmpty());
    Assert.assertTrue(fetchDirectly(systemTopic).isEmpty());
    Assert.assertTrue(fetchDirectly(previewTopic).isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testUnauthenticatedHttpRequestWithoutHeadersIsRejectedWith403() throws Exception {
    startServiceWithMasterInternalAuth();
    TopicId userTopic = ALPHA.topic(TOPIC);
    TopicId systemTopic = NamespaceId.SYSTEM.topic(TOPIC);
    TopicId previewTopic = NamespaceId.SYSTEM.topic("preview");
    TopicId metricsTopic = NamespaceId.SYSTEM.topic("metrics0");
    createTopicDirectly(userTopic);
    createTopicDirectly(systemTopic);
    createTopicDirectly(previewTopic);
    createTopicDirectly(metricsTopic);
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    // Direct HTTP requests with no authentication headers get CDAP-empty-user-credential from
    // AuthenticationChannelHandler and must be rejected with 403 before InternalAccessEnforcer.
    assertAllEndpoints(403, NamespaceId.SYSTEM, TOPIC);
    assertAllEndpoints(403, NamespaceId.SYSTEM, "preview");
    assertAllEndpoints(403, NamespaceId.SYSTEM, "metrics0");
    assertAllEndpoints(403, ALPHA, TOPIC);

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(fetchDirectly(userTopic).isEmpty());
    Assert.assertTrue(fetchDirectly(systemTopic).isEmpty());
    Assert.assertTrue(fetchDirectly(previewTopic).isEmpty());
    Assert.assertTrue(fetchDirectly(metricsTopic).isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testWorkerPlaceholderStillRequiresEnforcerWhenInternalAuthIsOff() throws Exception {
    startService(false);
    TopicId systemTopic = NamespaceId.SYSTEM.topic(TOPIC);
    createTopicDirectly(systemTopic);
    currentPrincipal.set(EMPTY_WORKER_PRINCIPAL);

    assertAllEndpoints(403, NamespaceId.SYSTEM);
    Assert.assertEquals(Endpoint.values().length, accessEnforcer.checks().size());
    Assert.assertTrue(fetchDirectly(systemTopic).isEmpty());
  }

  @Test
  public void testAuthorizedRoundTripPublishAndPoll() throws Exception {
    startService(true);
    TopicId topicId = ALPHA.topic(TOPIC);
    createTopicDirectly(topicId);
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);

    assertStatus(200, send(Endpoint.PUBLISH, ALPHA));

    HttpResponse pollResponse = send(Endpoint.POLL, ALPHA);
    assertStatus(200, pollResponse);
    Assert.assertEquals(Collections.singletonList("msg"),
        decodePollResponse(pollResponse.getResponseBody()));
  }

  @Test
  public void testDefaultClientMessagingServiceUsesInternalIdentityWhenUserContextIsSet()
      throws Exception {
    final DefaultClientMessagingService client = startServiceWithMasterInternalAuth();
    final TopicId systemTopic = NamespaceId.SYSTEM.topic("programstatusevent");
    createTopicDirectly(systemTopic);

    // Simulate a /v3 HTTP handler thread in AppFabric where AuthenticationChannelHandler has
    // populated SecurityRequestContext with an end-user's EXTERNAL credential.
    SecurityRequestContext.setUserId(END_USER.getName());
    SecurityRequestContext.setUserCredential(END_USER.getFullCredential());
    SecurityRequestContext.setUserIp("10.1.2.3");

    // A direct AbstractClientMessagingService subclass that does not clear SecurityRequestContext
    // propagates END_USER's EXTERNAL credential and is rejected on NamespaceId.SYSTEM.
    RemoteClientFactory remoteClientFactory =
        injectorRef.get().getInstance(RemoteClientFactory.class);
    AbstractClientMessagingService rawClient =
        new DirectClientMessagingService(remoteClientFactory);
    try {
      rawClient.publish(StoreRequestBuilder.of(systemTopic).addPayload("blocked").build());
      Assert.fail("Expected UnauthorizedException from direct AbstractClientMessagingService");
    } catch (UnauthorizedException expected) {
      // expected
    }
    try {
      rawClient.storePayload(
          StoreRequestBuilder.of(systemTopic).setTransaction(99L).addPayload("blocked").build());
      Assert.fail("Expected UnauthorizedException from direct AbstractClientMessagingService");
    } catch (UnauthorizedException expected) {
      // expected
    }
    try {
      rawClient.fetch(new DefaultMessageFetchRequest.Builder().setTopicId(systemTopic).build());
      Assert.fail("Expected IOException on 403 from direct AbstractClientMessagingService.fetch");
    } catch (IOException expected) {
      Assert.assertTrue(expected.getMessage().contains(
          "Only internal services can access topics of the system namespace."));
    }

    // 1. Non-transactional publish to system:programstatusevent
    client.publish(StoreRequestBuilder.of(systemTopic).addPayload("status-event").build());
    assertUserContextPreserved(END_USER.getName(), END_USER.getFullCredential(), "10.1.2.3");

    // 2. Transactional publish + rollback on system:programstatusevent
    RollbackDetail rollbackDetail = client.publish(
        StoreRequestBuilder.of(systemTopic).setTransaction(1L).addPayload("tx-event").build());
    Assert.assertNotNull(rollbackDetail);
    assertUserContextPreserved(END_USER.getName(), END_USER.getFullCredential(), "10.1.2.3");

    client.rollback(systemTopic, rollbackDetail);
    assertUserContextPreserved(END_USER.getName(), END_USER.getFullCredential(), "10.1.2.3");

    // 3. Transactional storePayload on system:programstatusevent
    client.storePayload(
        StoreRequestBuilder.of(systemTopic).setTransaction(2L).addPayload("stored-event").build());
    assertUserContextPreserved(END_USER.getName(), END_USER.getFullCredential(), "10.1.2.3");

    // 4. Fetch (openConnection) on system:programstatusevent
    List<String> fetched = new ArrayList<>();
    try (CloseableIterator<RawMessage> iterator = client.fetch(
        new DefaultMessageFetchRequest.Builder().setTopicId(systemTopic).build())) {
      while (iterator.hasNext()) {
        fetched.add(Bytes.toString(iterator.next().getPayload()));
      }
    }
    Assert.assertEquals(Arrays.asList("status-event", "tx-event"), fetched);
    assertUserContextPreserved(END_USER.getName(), END_USER.getFullCredential(), "10.1.2.3");

    // Every check must have reached the server with a signed INTERNAL credential (validated by
    // InternalAccessEnforcer) rather than the caller thread's EXTERNAL user credential.
    List<Check> checks = accessEnforcer.checks();
    Assert.assertEquals(5, checks.size());
    for (Check check : checks) {
      Assert.assertEquals(NamespaceId.SYSTEM, check.entity);
      Assert.assertNotNull(check.principal.getFullCredential());
      Assert.assertEquals(Credential.CredentialType.INTERNAL,
          check.principal.getFullCredential().getType());
    }

    // 5. Failing calls (TopicNotFoundException) also restore the caller's SecurityRequestContext.
    TopicId missingTopic = NamespaceId.SYSTEM.topic("missing");
    try {
      client.publish(StoreRequestBuilder.of(missingTopic).addPayload("x").build());
      Assert.fail("Expected TopicNotFoundException");
    } catch (TopicNotFoundException expected) {
      // expected
    }
    assertUserContextPreserved(END_USER.getName(), END_USER.getFullCredential(), "10.1.2.3");

    try {
      client.fetch(new DefaultMessageFetchRequest.Builder().setTopicId(missingTopic).build());
      Assert.fail("Expected TopicNotFoundException");
    } catch (TopicNotFoundException expected) {
      // expected
    }
    assertUserContextPreserved(END_USER.getName(), END_USER.getFullCredential(), "10.1.2.3");

    // 6. When SecurityRequestContext is unset before calling the client, it remains unset after.
    SecurityRequestContext.resetUserDetails();
    client.publish(StoreRequestBuilder.of(systemTopic).addPayload("bg-event").build());
    assertUserContextPreserved(null, null, null);
  }

  @Test
  public void testDefaultClientMessagingServiceWithWorkerAuthContextUsesWorkerIdentityWhenUserContextIsSet()
      throws Exception {
    // When messaging.service.enabled=true (Cloud SQL), PreviewRunnerMessagingClientModule and
    // TaskWorkerMessagingClientModule bind MessagingService to DefaultClientMessagingService
    // paired with AuthenticationContextModules().getMasterWorkerModule() (WorkerAuthenticationContext).
    final DefaultClientMessagingService client =
        startServiceWithInternalAuth(new AuthenticationContextModules().getMasterWorkerModule());
    final TopicId previewTopic = NamespaceId.SYSTEM.topic("preview");
    final TopicId metricsTopic = NamespaceId.SYSTEM.topic("metrics0");
    final TopicId statusTopic = NamespaceId.SYSTEM.topic("programstatusevent");
    createTopicDirectly(previewTopic);
    createTopicDirectly(metricsTopic);
    createTopicDirectly(statusTopic);

    SecurityRequestContext.setUserId(END_USER.getName());
    SecurityRequestContext.setUserCredential(END_USER.getFullCredential());
    SecurityRequestContext.setUserIp("10.1.2.3");

    // A direct AbstractClientMessagingService subclass does NOT clear SecurityRequestContext, so
    // publishing to system:metrics0 when END_USER (EXTERNAL) is in SecurityRequestContext fails
    // with 403 (since EXTERNAL credentials are only allowed on system:preview).
    AbstractClientMessagingService rawWorkerClient =
        new DirectClientMessagingService(injectorRef.get().getInstance(RemoteClientFactory.class));
    try {
      rawWorkerClient.publish(StoreRequestBuilder.of(metricsTopic).addPayload("m-blocked").build());
      Assert.fail("Expected UnauthorizedException from direct AbstractClientMessagingService");
    } catch (UnauthorizedException expected) {
      // expected
    }
    assertUserContextPreserved(END_USER.getName(), END_USER.getFullCredential(), "10.1.2.3");

    // DefaultClientMessagingService clears SecurityRequestContext so WorkerAuthenticationContext
    // authenticates as empty-worker-user-credential, allowing writes to worker-allowlisted system
    // topics (preview, metrics0) while still denying non-allowlisted topics (programstatusevent).
    client.publish(StoreRequestBuilder.of(previewTopic).addPayload("p-msg").build());
    assertUserContextPreserved(END_USER.getName(), END_USER.getFullCredential(), "10.1.2.3");

    client.publish(StoreRequestBuilder.of(metricsTopic).addPayload("m-msg").build());
    assertUserContextPreserved(END_USER.getName(), END_USER.getFullCredential(), "10.1.2.3");

    Assert.assertEquals(Collections.singletonList("p-msg"), fetchDirectly(previewTopic));
    Assert.assertEquals(Collections.singletonList("m-msg"), fetchDirectly(metricsTopic));

    try {
      client.publish(StoreRequestBuilder.of(statusTopic).addPayload("s-blocked").build());
      Assert.fail("Expected UnauthorizedException on non-allowlisted system topic");
    } catch (UnauthorizedException expected) {
      // expected
    }
    assertUserContextPreserved(END_USER.getName(), END_USER.getFullCredential(), "10.1.2.3");
  }

  private static final class DirectClientMessagingService extends AbstractClientMessagingService {

    DirectClientMessagingService(RemoteClientFactory remoteClientFactory) {
      super(remoteClientFactory.createRemoteClient(
          Constants.Service.MESSAGING_SERVICE, HTTP_REQUEST_CONFIG, "/v1/namespaces/"), false);
    }
  }

  private static void assertUserContextPreserved(@Nullable String expectedUserId,
      @Nullable Credential expectedCredential, @Nullable String expectedUserIp) {
    Assert.assertEquals(expectedUserId, SecurityRequestContext.getUserId());
    Assert.assertEquals(expectedCredential, SecurityRequestContext.getUserCredential());
    Assert.assertEquals(expectedUserIp, SecurityRequestContext.getUserIp());
  }

  private DefaultClientMessagingService startServiceWithMasterInternalAuth() throws IOException {
    return startServiceWithInternalAuth(new AuthenticationContextModules().getMasterModule());
  }

  private DefaultClientMessagingService startServiceWithInternalAuth(Module authContextModule)
      throws IOException {
    CConfiguration cConf = CConfiguration.create();
    File localDataDir = TEMP_FOLDER.newFolder();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    cConf.set(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS,
        InetAddress.getLocalHost().getHostName());
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    cConf.setLong(TxConstants.Manager.CFG_TX_MAX_LIFETIME, 10000000000L);
    tempDir = new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR));

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new IOModule(),
        RemoteAuthenticatorModules.getDefaultModule(),
        new InMemoryDiscoveryModule(),
        new CoreSecurityRuntimeModule().getInMemoryModules(),
        authContextModule,
        Modules.override(new AuthorizationEnforcementModule().getNoOpModules())
            .with(new AbstractModule() {
              @Override
              protected void configure() {
                bind(AccessEnforcer.class).toProvider(() -> {
                  InternalAccessEnforcer internalEnforcer =
                      injectorRef.get().getInstance(InternalAccessEnforcer.class);
                  return new AccessEnforcer() {
                    @Override
                    public void enforce(EntityId entity, Principal principal,
                        Set<? extends Permission> permissions) {
                      internalEnforcer.enforce(entity, principal, permissions);
                      accessEnforcer.enforce(entity, principal, permissions);
                    }

                    @Override
                    public void enforceOnParent(EntityType entityType, EntityId parentId,
                        Principal principal, Permission permission) {
                      internalEnforcer.enforceOnParent(entityType, parentId, principal, permission);
                    }

                    @Override
                    public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds,
                        Principal principal) {
                      return internalEnforcer.isVisible(entityIds, principal);
                    }
                  };
                });
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
    injectorRef.set(injector);

    tokenManager = injector.getInstance(TokenManager.class);
    tokenManager.startAndWait();
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    messagingService = injector.getInstance(MessagingService.class);
    httpService = injector.getInstance(MessagingHttpService.class);
    httpService.startAndWait();
    messagingServer = Iterables.getOnlyElement(
        injector.getInstance(DiscoveryServiceClient.class)
            .discover(Constants.Service.MESSAGING_SERVICE));
    return injector.getInstance(DefaultClientMessagingService.class);
  }

  private final AtomicReference<Injector> injectorRef = new AtomicReference<>();

  private void startService(boolean internalAuthEnabled) throws IOException {
    startService(internalAuthEnabled, conf -> { });
  }

  private void startService(boolean internalAuthEnabled,
      Consumer<CConfiguration> configurer) throws IOException {
    CConfiguration cConf = CConfiguration.create();
    File localDataDir = TEMP_FOLDER.newFolder();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    cConf.set(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS,
        InetAddress.getLocalHost().getHostName());
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, internalAuthEnabled);
    configurer.accept(cConf);
    tempDir = new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR));

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
        injector.getInstance(DiscoveryServiceClient.class)
            .discover(Constants.Service.MESSAGING_SERVICE));
  }

  private void createTopicDirectly(TopicId topicId) throws Exception {
    messagingService.createTopic(new DefaultTopicMetadata(topicId, "ttl", "100"));
  }

  private List<String> fetchDirectly(TopicId topicId) throws Exception {
    List<RawMessage> messages = new ArrayList<>();
    try (CloseableIterator<RawMessage> iterator = messagingService.fetch(
        new DefaultMessageFetchRequest.Builder().setTopicId(topicId).build())) {
      Iterators.addAll(messages, iterator);
    }
    return messages.stream()
        .map(RawMessage::getPayload)
        .map(Bytes::toString)
        .collect(Collectors.toList());
  }

  private void assertNoTempFilesCreated() {
    String[] files = tempDir.list();
    Assert.assertNotNull(files);
    Assert.assertEquals(Arrays.toString(files), 0, files.length);
  }

  private HttpResponse send(Endpoint endpoint, NamespaceId namespace) throws IOException {
    return send(endpoint, namespace, TOPIC);
  }

  private HttpResponse send(Endpoint endpoint, NamespaceId namespace, String topic)
      throws IOException {
    return send(namespace.getNamespace(), topic, endpoint.action, "avro/binary", endpoint.body());
  }

  private HttpResponse send(String namespace, String topic, String action,
      @Nullable String contentType, byte[] body) throws IOException {
    URL url = URIScheme.createURI(messagingServer, "%s",
        "/v1/namespaces/" + namespace + "/topics/" + topic + "/" + action).toURL();
    HttpRequest.Builder request = HttpRequest.builder(HttpMethod.POST, url)
        .withBody(ByteBuffer.wrap(body));
    if (contentType != null) {
      request.addHeader(HttpHeaders.CONTENT_TYPE, contentType);
    }
    return HttpRequests.execute(request.build());
  }

  private HttpResponse sendTopicAdmin(HttpMethod method, String namespace, String topic)
      throws IOException {
    URL url = URIScheme.createURI(messagingServer, "%s",
        "/v1/namespaces/" + namespace + "/topics/" + topic).toURL();
    HttpRequest.Builder request = HttpRequest.builder(method, url);
    if (method == HttpMethod.PUT) {
      request.withBody("{}");
    }
    return HttpRequests.execute(request.build());
  }

  private void assertStatus(int expectedStatus, HttpResponse response) {
    Assert.assertEquals(response.getResponseBodyAsString(),
        expectedStatus, response.getResponseCode());
  }

  private void assertAllEndpoints(int expectedStatus, NamespaceId namespace) throws IOException {
    assertAllEndpoints(expectedStatus, namespace, TOPIC);
  }

  private void assertAllEndpoints(int expectedStatus, NamespaceId namespace, String topic)
      throws IOException {
    for (Endpoint endpoint : Endpoint.values()) {
      HttpResponse response = send(endpoint, namespace, topic);
      Assert.assertEquals(
          endpoint + " in " + namespace + "." + topic + ": " + response.getResponseBodyAsString(),
          expectedStatus, response.getResponseCode());
    }
  }

  private void assertSingleCheck(Endpoint endpoint, NamespaceId namespace, Principal principal) {
    List<Check> checks = accessEnforcer.checks();
    Assert.assertEquals(endpoint.toString(), 1, checks.size());
    Check check = checks.get(0);
    Assert.assertEquals(endpoint.toString(), namespace, check.entity);
    Assert.assertSame(endpoint.toString(), principal, check.principal);
    Assert.assertEquals(endpoint.toString(),
        Collections.singleton(endpoint.requiredPermission), check.permissions);
  }

  private static byte[] encodePublishRequest(@Nullable Long txWritePointer, String... payloads)
      throws IOException {
    GenericRecord record = new GenericData.Record(Schemas.V1.PublishRequest.SCHEMA);
    if (txWritePointer != null) {
      record.put("transactionWritePointer", txWritePointer);
    }
    List<ByteBuffer> messages = new ArrayList<>();
    for (String payload : payloads) {
      messages.add(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)));
    }
    record.put("messages", messages);

    ExposedByteArrayOutputStream os = new ExposedByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(os, null);
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(Schemas.V1.PublishRequest.SCHEMA);
    writer.write(record, encoder);
    encoder.flush();
    return os.toByteArray();
  }

  private static byte[] encodeRollbackRequest() throws IOException {
    Schema schema = Schemas.V1.PublishResponse.SCHEMA;
    GenericRecord record = new GenericData.Record(schema);
    record.put("transactionWritePointer", 1L);

    GenericRecord rollbackRange = new GenericData.Record(schema.getField("rollbackRange").schema());
    rollbackRange.put("startTimestamp", 1L);
    rollbackRange.put("startSequenceId", 0);
    rollbackRange.put("endTimestamp", 2L);
    rollbackRange.put("endSequenceId", 1);
    record.put("rollbackRange", rollbackRange);

    ExposedByteArrayOutputStream os = new ExposedByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(os, null);
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    return os.toByteArray();
  }

  private static byte[] encodeConsumeRequest() throws IOException {
    GenericRecord record = new GenericData.Record(Schemas.V1.ConsumeRequest.SCHEMA);
    record.put("inclusive", false);
    record.put("limit", 10);

    ExposedByteArrayOutputStream os = new ExposedByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(os, null);
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(Schemas.V1.ConsumeRequest.SCHEMA);
    writer.write(record, encoder);
    encoder.flush();
    return os.toByteArray();
  }

  private static List<String> decodePollResponse(byte[] body) throws IOException {
    Decoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(body), null);
    DatumReader<GenericRecord> reader =
        new GenericDatumReader<>(Schemas.V1.ConsumeResponse.SCHEMA.getElementType());
    GenericRecord record =
        new GenericData.Record(Schemas.V1.ConsumeResponse.SCHEMA.getElementType());
    List<String> payloads = new ArrayList<>();
    for (long count = decoder.readArrayStart(); count > 0; count = decoder.arrayNext()) {
      for (long i = 0; i < count; i++) {
        record = reader.read(record, decoder);
        payloads.add(Bytes.toString((ByteBuffer) record.get("payload")));
      }
    }
    return payloads;
  }

  private enum Endpoint {
    PUBLISH("publish", StandardPermission.UPDATE) {
      @Override
      byte[] body() throws IOException {
        return encodePublishRequest(null, "msg");
      }
    },
    STORE("store", StandardPermission.UPDATE) {
      @Override
      byte[] body() throws IOException {
        return encodePublishRequest(1L, "tx-msg");
      }
    },
    ROLLBACK("rollback", StandardPermission.UPDATE) {
      @Override
      byte[] body() throws IOException {
        return encodeRollbackRequest();
      }
    },
    POLL("poll", StandardPermission.GET) {
      @Override
      byte[] body() throws IOException {
        return encodeConsumeRequest();
      }
    };

    private final String action;
    private final StandardPermission requiredPermission;

    Endpoint(String action, StandardPermission requiredPermission) {
      this.action = action;
      this.requiredPermission = requiredPermission;
    }

    abstract byte[] body() throws IOException;
  }

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

  private static final class RecordingAccessEnforcer implements AccessEnforcer {

    private final Map<EntityId, Set<Permission>> grants = new ConcurrentHashMap<>();
    private final List<Check> checks = new CopyOnWriteArrayList<>();
    private volatile RuntimeException failure;

    void grant(EntityId entity, Permission... permissions) {
      grants.computeIfAbsent(entity, key -> ConcurrentHashMap.newKeySet())
          .addAll(Arrays.asList(permissions));
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
    public void enforceOnParent(EntityType entityType, EntityId parentId,
        Principal principal, Permission permission) {
      throw new UnsupportedOperationException("enforceOnParent is not expected");
    }

    @Override
    public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds,
        Principal principal) {
      throw new UnsupportedOperationException("isVisible is not expected");
    }
  }
}

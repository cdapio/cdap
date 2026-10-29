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
import com.google.inject.util.Modules;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
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
import io.cdap.cdap.internal.io.ExposedByteArrayOutputStream;
import io.cdap.cdap.messaging.DefaultMessageFetchRequest;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.Schemas;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.proto.element.EntityType;
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
  private static final Principal EMPTY_WORKER_PRINCIPAL = new Principal("empty-worker-user-id",
      Principal.PrincipalType.USER,
      new Credential("empty-worker-user-credential", Credential.CredentialType.INTERNAL));

  private final RecordingAccessEnforcer accessEnforcer = new RecordingAccessEnforcer();
  private final AtomicReference<Principal> currentPrincipal = new AtomicReference<>(END_USER);

  private MessagingHttpService httpService;
  private MessagingService messagingService;
  private Discoverable messagingServer;
  private File tempDir;

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

    // A caller with permission still gets the topic validation error (400).
    accessEnforcer.grant(ALPHA, StandardPermission.UPDATE, StandardPermission.GET);
    for (Endpoint endpoint : Endpoint.values()) {
      assertStatus(400, send(ALPHA.getNamespace(), "bad.topic", endpoint.action,
          "avro/binary", endpoint.body()));
    }
  }

  @Test
  public void testInvalidNamespaceNameIsRejectedWithoutAskingTheEnforcer() throws Exception {
    startService(false);

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
    createTopicDirectly(NamespaceId.SYSTEM.topic(TOPIC));
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    assertAllEndpoints(200, NamespaceId.SYSTEM);
  }

  @Test
  public void testSystemNamespaceRejectsCredentialsThatAreNotInternal() throws Exception {
    startService(true);
    createTopicDirectly(NamespaceId.SYSTEM.topic(TOPIC));
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    for (Credential.CredentialType type : Credential.CredentialType.values()) {
      if (type == Credential.CredentialType.INTERNAL) {
        continue;
      }
      for (String value : Arrays.asList("token", "empty-worker-user-credential")) {
        currentPrincipal.set(
            new Principal("system", Principal.PrincipalType.USER, new Credential(value, type)));

        assertAllEndpoints(403, NamespaceId.SYSTEM);
      }
    }

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(TOPIC)).isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testSystemNamespaceRejectsPrincipalWithoutCredential() throws Exception {
    startService(true);
    createTopicDirectly(NamespaceId.SYSTEM.topic(TOPIC));
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);
    currentPrincipal.set(new Principal("system", Principal.PrincipalType.USER));

    assertAllEndpoints(403, NamespaceId.SYSTEM);

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(TOPIC)).isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testSystemNamespaceRejectsNullPrincipal() throws Exception {
    startService(true);
    createTopicDirectly(NamespaceId.SYSTEM.topic(TOPIC));
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);
    currentPrincipal.set(null);

    assertAllEndpoints(403, NamespaceId.SYSTEM);

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(TOPIC)).isEmpty());
    assertNoTempFilesCreated();
  }

  @Test
  public void testSystemNamespaceRejectsEncodedSpellingOfItsName() throws Exception {
    startService(true);
    createTopicDirectly(NamespaceId.SYSTEM.topic(TOPIC));
    accessEnforcer.grant(NamespaceId.SYSTEM, StandardPermission.UPDATE, StandardPermission.GET);

    for (Endpoint endpoint : Endpoint.values()) {
      assertStatus(403, send("%73ystem", TOPIC, endpoint.action, "avro/binary", endpoint.body()));
    }

    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertTrue(fetchDirectly(NamespaceId.SYSTEM.topic(TOPIC)).isEmpty());
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
  public void testWorkerPlaceholderCanPublishToSystemNamespaceWithoutCallingEnforcer()
      throws Exception {
    startService(true);
    TopicId systemTopic = NamespaceId.SYSTEM.topic(TOPIC);
    createTopicDirectly(systemTopic);
    currentPrincipal.set(EMPTY_WORKER_PRINCIPAL);
    // The enforcer grants nothing and would fail if called for publish on system.

    assertStatus(200, send(Endpoint.PUBLISH, NamespaceId.SYSTEM));
    Assert.assertTrue(accessEnforcer.checks().isEmpty());
    Assert.assertEquals(Collections.singletonList("msg"), fetchDirectly(systemTopic));

    // Store, rollback, and poll on the system namespace still go through the enforcer and are denied.
    for (Endpoint endpoint : Arrays.asList(Endpoint.STORE, Endpoint.ROLLBACK, Endpoint.POLL)) {
      accessEnforcer.clearChecks();
      assertStatus(403, send(endpoint, NamespaceId.SYSTEM));
      assertSingleCheck(endpoint, NamespaceId.SYSTEM, EMPTY_WORKER_PRINCIPAL);
    }
  }

  @Test
  public void testWorkerPlaceholderStillRequiresEnforcerOnUserNamespace() throws Exception {
    startService(true);
    TopicId userTopic = ALPHA.topic(TOPIC);
    createTopicDirectly(userTopic);
    currentPrincipal.set(EMPTY_WORKER_PRINCIPAL);

    // Without enforcer grant, even publish on a user namespace is denied.
    assertAllEndpoints(403, ALPHA);
    Assert.assertEquals(Endpoint.values().length, accessEnforcer.checks().size());
    Assert.assertTrue(fetchDirectly(userTopic).isEmpty());
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

  private void startService(boolean internalAuthEnabled) throws IOException {
    CConfiguration cConf = CConfiguration.create();
    File localDataDir = TEMP_FOLDER.newFolder();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    cConf.set(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS,
        InetAddress.getLocalHost().getHostName());
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, internalAuthEnabled);
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
    return send(namespace.getNamespace(), TOPIC, endpoint.action, "avro/binary", endpoint.body());
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

  private void assertStatus(int expectedStatus, HttpResponse response) {
    Assert.assertEquals(response.getResponseBodyAsString(),
        expectedStatus, response.getResponseCode());
  }

  private void assertAllEndpoints(int expectedStatus, NamespaceId namespace) throws IOException {
    for (Endpoint endpoint : Endpoint.values()) {
      HttpResponse response = send(endpoint, namespace);
      Assert.assertEquals(
          endpoint + " in " + namespace + ": " + response.getResponseBodyAsString(),
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

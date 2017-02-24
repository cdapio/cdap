/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.stream;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.data.file.FileWriter;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.audit.InMemoryAuditPublisher;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public abstract class StreamAdminTest {
  private static final Principal USER = new Principal(System.getProperty("user.name"), Principal.PrincipalType.USER);

  protected static CConfiguration cConf = CConfiguration.create();
  protected static final NamespaceId FOO_NAMESPACE = new NamespaceId("fooNamespace");
  protected static final NamespaceId OTHER_NAMESPACE = new NamespaceId("otherNamespace");

  protected abstract StreamAdmin getStreamAdmin();

  protected abstract StreamFileWriterFactory getFileWriterFactory();

  protected abstract InMemoryAuditPublisher getInMemoryAuditPublisher();

  protected abstract Authorizer getAuthorizer();

  protected abstract OwnerAdmin getOwnerAdmin();

  protected static void setupNamespaces(NamespacedLocationFactory namespacedLocationFactory) throws IOException {
    namespacedLocationFactory.get(FOO_NAMESPACE.toId()).mkdirs();
    namespacedLocationFactory.get(OTHER_NAMESPACE.toId()).mkdirs();
  }

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  protected static void addCConfProperties(CConfiguration cConf) throws IOException {
    File rootLocationFactoryPath = TEMPORARY_FOLDER.newFolder();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean(Constants.Security.Authorization.CACHE_ENABLED, false);
    LocationFactory locationFactory = new LocalLocationFactory(rootLocationFactoryPath);
    Location authorizerJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authorizerJar.toURI().getPath());
  }

  @Before
  public void beforeTest() throws Exception {
    revokeAndAssertSuccess(FOO_NAMESPACE, USER, EnumSet.allOf(Action.class));
    revokeAndAssertSuccess(OTHER_NAMESPACE, USER, EnumSet.allOf(Action.class));
  }

  @Test
  public void testCreateExist() throws Exception {
    SecurityRequestContext.setUserId(USER.getName());
    StreamAdmin streamAdmin = getStreamAdmin();

    String streamName = "streamName";
    StreamId streamId = FOO_NAMESPACE.stream(streamName);
    StreamId otherStreamId = OTHER_NAMESPACE.stream(streamName);

    Assert.assertFalse(streamAdmin.exists(streamId));
    Assert.assertFalse(streamAdmin.exists(otherStreamId));

    try {
      streamAdmin.create(streamId);
      Assert.fail("User should not be able to create a stream in this namespace.");
    } catch (UnauthorizedException e) {
      // expected
    }

    // grant write access for user to foo_namespace
    grantAndAssertSuccess(streamId.getParent(), USER, ImmutableSet.of(Action.WRITE));
    streamAdmin.create(streamId);

    // Even though both streams have the same name, {@code otherStreamId} does not exist because it is in a different
    // namespace than the one created above.
    Assert.assertTrue(streamAdmin.exists(streamId));
    Assert.assertFalse(streamAdmin.exists(otherStreamId));

    try {
      streamAdmin.create(otherStreamId);
      Assert.fail("User should not be able to create a stream in this namespace.");
    } catch (UnauthorizedException e) {
      // expected
    }

    // grant write access for user to other_namespace
    grantAndAssertSuccess(otherStreamId.getParent(), USER, ImmutableSet.of(Action.WRITE));
    streamAdmin.create(otherStreamId);
    Assert.assertTrue(streamAdmin.exists(otherStreamId));

    // the user should be able to drop the stream, they had created
    streamAdmin.drop(otherStreamId);
    Assert.assertFalse(streamAdmin.exists(otherStreamId));

    // revoke the permission for the user on the stream in foo_namespace
    revokeAndAssertSuccess(streamId, USER, EnumSet.allOf(Action.class));

    try {
      streamAdmin.drop(streamId);
      Assert.fail("User should not be able to delete a stream in this namespace.");
    } catch (UnauthorizedException e) {
      // expected
    }

    // grant WRITE permission to the user but they still should not be able to drop the stream
    grantAndAssertSuccess(streamId, USER, ImmutableSet.of(Action.WRITE));

    try {
      streamAdmin.drop(streamId);
      Assert.fail("User should not be able to delete a stream with only Write Action access.");
    } catch (UnauthorizedException e) {
      // expected
    }

    // grant admin access and the user should then be able to drop the stream
    grantAndAssertSuccess(streamId, USER, ImmutableSet.of(Action.ADMIN));
    streamAdmin.drop(streamId);
    Assert.assertFalse(streamAdmin.exists(streamId));
  }

  @Test
  public void testConfigAndTruncate() throws Exception {
    StreamAdmin streamAdmin = getStreamAdmin();
    grantAndAssertSuccess(FOO_NAMESPACE, USER, ImmutableSet.of(Action.WRITE));
    StreamId stream = FOO_NAMESPACE.stream("stream");

    streamAdmin.create(stream);
    Assert.assertTrue(streamAdmin.exists(stream));
    writeEvent(stream);

    // Getting config / properties should work
    streamAdmin.getConfig(stream);
    streamAdmin.getProperties(stream);

    // Now revoke access to the user to the stream and to the namespace
    revokeAndAssertSuccess(FOO_NAMESPACE, USER, ImmutableSet.of(Action.WRITE));
    revokeAndAssertSuccess(stream, USER, EnumSet.allOf(Action.class));
    streamAdmin.getConfig(stream);

    try {
      streamAdmin.getProperties(stream);
      Assert.fail("User should not be able to get the properties.");
    } catch (UnauthorizedException e) {
      // expected
    }

    // read action should be enough to get the stream config
    grantAndAssertSuccess(stream, USER, ImmutableSet.of(Action.READ));
    streamAdmin.getConfig(stream);
    StreamProperties properties = streamAdmin.getProperties(stream);

    try {
      streamAdmin.updateConfig(stream, properties);
      Assert.fail("User should not be able to update the config with just READ permissions.");
    } catch (UnauthorizedException e) {
      // expected
    }

    // This call bypasses the stream handler and thus authorization is not checked for this call and so write
    // to stream will succeed. It is done so that we can check and perform truncate call.
    writeEvent(stream);

    grantAndAssertSuccess(stream, USER, ImmutableSet.of(Action.WRITE));
    writeEvent(stream);

    try {
      streamAdmin.updateConfig(stream, properties);
      Assert.fail("User should not be able to update the config with just READ and WRITE permissions.");
    } catch (UnauthorizedException e) {
      // expected
    }

    try {
      streamAdmin.truncate(stream);
      Assert.fail("User should not be able to truncate the stream without ADMIN permission.");
    } catch (UnauthorizedException e) {
      // expected
    }

    try {
      streamAdmin.drop(stream);
      Assert.fail("User should not be able to drop the stream without ADMIN permission.");
    } catch (UnauthorizedException e) {
      // expdcted
    }

    grantAndAssertSuccess(stream, USER, ImmutableSet.of(Action.ADMIN));
    streamAdmin.updateConfig(stream, properties);
    streamAdmin.truncate(stream);
    Assert.assertEquals(0, getStreamSize(stream));
    streamAdmin.drop(stream);
  }

  @Test
  public void testOwner() throws Exception {
    // crate a stream with owner
    StreamAdmin streamAdmin = getStreamAdmin();
    OwnerAdmin ownerAdmin = getOwnerAdmin();
    grantAndAssertSuccess(FOO_NAMESPACE, USER, ImmutableSet.of(Action.WRITE));
    StreamId stream = FOO_NAMESPACE.stream("stream");
    Properties properties = new Properties();
    String ownerPrincipal = "user/somehost@somekdc.net";
    properties.put(Constants.Security.PRINCIPAL, ownerPrincipal);
    streamAdmin.create(stream, properties);
    Assert.assertTrue(streamAdmin.exists(stream));

    // Check that the owner information got stored in owner store
    Assert.assertTrue(ownerAdmin.exists(stream));

    // also verify that we are able to get owner information back in properties
    Assert.assertEquals(ownerPrincipal, streamAdmin.getProperties(stream).getOwnerPrincipal());

    // updating stream owner should fail
    try {
      streamAdmin.updateConfig(stream, new StreamProperties(1L, null, null, null, "user/somekdc.net"));
      Assert.fail();
    } catch (UnauthorizedException e) {
      // expected
    }

    // trying to create same stream with different owner should fail
    properties.put(Constants.Security.PRINCIPAL, "someOtherUser/someHost@somekdc.net");
    try {
      streamAdmin.create(stream, properties);
      Assert.fail("Should have failed to add the same stream with different owner");
    } catch (UnauthorizedException e) {
      // expected
    }

    // ensure that the previous owner still exists
    Assert.assertEquals(ownerPrincipal, streamAdmin.getProperties(stream).getOwnerPrincipal());

    // drop the stream which should also delete the owner info
    streamAdmin.drop(stream);
    Assert.assertFalse(ownerAdmin.exists(stream));
  }

  @Test
  public void testListStreams() throws Exception {
    StreamAdmin streamAdmin = getStreamAdmin();
    NamespaceId nsId = FOO_NAMESPACE;
    grantAndAssertSuccess(nsId, USER, EnumSet.allOf(Action.class));
    StreamId s1 = nsId.stream("s1");
    StreamId s2 = nsId.stream("s2");
    List<StreamSpecification> specifications = streamAdmin.listStreams(nsId);
    Assert.assertTrue(specifications.isEmpty());
    streamAdmin.create(s1);
    streamAdmin.create(s2);
    specifications = streamAdmin.listStreams(nsId);
    Assert.assertEquals(2, specifications.size());

    // Revoke all privileges on s1.
    revokeAndAssertSuccess(s1, USER, EnumSet.allOf(Action.class));

    // User should still be able to list both streams because it has all privilege on the parent
    specifications = streamAdmin.listStreams(nsId);
    Assert.assertEquals(2, specifications.size());
    Set<String> streamNames = ImmutableSet.of(s1.getStream(), s2.getStream());
    Assert.assertTrue(streamNames.contains(specifications.get(0).getName()));
    Assert.assertTrue(streamNames.contains(specifications.get(1).getName()));

    // Revoke all privileges on s2.
    revokeAndAssertSuccess(s2, USER, EnumSet.allOf(Action.class));

    // User should still be able to list both streams because it has all privilege on the parent
    specifications = streamAdmin.listStreams(nsId);
    Assert.assertEquals(2, specifications.size());
    Assert.assertTrue(streamNames.contains(specifications.get(0).getName()));
    Assert.assertTrue(streamNames.contains(specifications.get(1).getName()));

    // Revoke all privileges on the namespace
    revokeAndAssertSuccess(nsId, USER, EnumSet.allOf(Action.class));

    // User shouldn't be able to see any streams
    specifications = streamAdmin.listStreams(nsId);
    Assert.assertTrue(specifications.isEmpty());

    grantAndAssertSuccess(s1, USER, EnumSet.allOf(Action.class));
    grantAndAssertSuccess(s2, USER, EnumSet.allOf(Action.class));
    streamAdmin.drop(s1);
    streamAdmin.drop(s2);
  }

  @Test
  public void testDropAllInNamespace() throws Exception {
    StreamAdmin streamAdmin = getStreamAdmin();
    grantAndAssertSuccess(FOO_NAMESPACE, USER, ImmutableSet.of(Action.WRITE, Action.ADMIN));
    grantAndAssertSuccess(OTHER_NAMESPACE, USER, ImmutableSet.of(Action.WRITE, Action.ADMIN));

    StreamId otherStream = OTHER_NAMESPACE.stream("otherStream");

    List<StreamId> fooStreams = Lists.newArrayList();
    for (int i = 0; i < 4; i++) {
      fooStreams.add(FOO_NAMESPACE.stream("stream" + i));
    }

    List<StreamId> allStreams = Lists.newArrayList();
    allStreams.addAll(fooStreams);
    allStreams.add(otherStream);

    for (StreamId stream : allStreams) {
      streamAdmin.create(stream);
      writeEvent(stream);
      // all of the streams should have data in it after writing to them
      Assert.assertNotEquals(0, getStreamSize(stream));
    }

    streamAdmin.dropAllInNamespace(FOO_NAMESPACE);

    // All of the streams within the default namespace should no longer exist
    for (StreamId defaultStream : fooStreams) {
      Assert.assertFalse(streamAdmin.exists(defaultStream));
    }
    // otherStream isn't in the foo namespace so its data is not deleted in the above call to dropAllInNamespace.
    Assert.assertNotEquals(0, getStreamSize(otherStream));

    // truncate should also delete all the data of a stream
    streamAdmin.truncate(otherStream);
    Assert.assertEquals(0, getStreamSize(otherStream));
  }

  @Test
  public void testAuditPublish() throws Exception {
    grantAndAssertSuccess(FOO_NAMESPACE, USER, EnumSet.allOf(Action.class));

    // clear existing all messages
    getInMemoryAuditPublisher().popMessages();

    final List<AuditMessage> expectedMessages = new ArrayList<>();
    StreamAdmin streamAdmin = getStreamAdmin();

    StreamId stream1 = FOO_NAMESPACE.stream("stream1");
    streamAdmin.create(stream1);
    expectedMessages.add(new AuditMessage(0, stream1, "", AuditType.CREATE,
                                          AuditPayload.EMPTY_PAYLOAD));

    StreamId stream2 = FOO_NAMESPACE.stream("stream2");
    streamAdmin.create(stream2);
    expectedMessages.add(new AuditMessage(0, stream2, "", AuditType.CREATE,
                                          AuditPayload.EMPTY_PAYLOAD));

    streamAdmin.truncate(stream1);
    expectedMessages.add(new AuditMessage(0, stream1, "", AuditType.TRUNCATE,
                                          AuditPayload.EMPTY_PAYLOAD));

    streamAdmin.updateConfig(stream1, new StreamProperties(100L, new FormatSpecification("f", null), 100));
    expectedMessages.add(new AuditMessage(0, stream1, "", AuditType.UPDATE,
                                          AuditPayload.EMPTY_PAYLOAD));

    ProgramRunId run = new ProgramId("ns1", "app", ProgramType.FLOW, "flw").run(RunIds.generate().getId());
    streamAdmin.addAccess(run, stream1, AccessType.READ);
    expectedMessages.add(new AuditMessage(0, stream1, "", AuditType.ACCESS,
                                          new AccessPayload(co.cask.cdap.proto.audit.payload.access.AccessType.READ,
                                                            run)));

    streamAdmin.drop(stream1);
    expectedMessages.add(new AuditMessage(0, stream1, "", AuditType.DELETE,
                                          AuditPayload.EMPTY_PAYLOAD));

    streamAdmin.dropAllInNamespace(FOO_NAMESPACE);
    expectedMessages.add(new AuditMessage(0, stream2, "", AuditType.DELETE,
                                          AuditPayload.EMPTY_PAYLOAD));

    // Ignore audit messages for system namespace (creation of system datasets, etc)
    final String systemNs = NamespaceId.SYSTEM.getNamespace();
    final Iterable<AuditMessage> actualMessages =
      Iterables.filter(getInMemoryAuditPublisher().popMessages(),
                       new Predicate<AuditMessage>() {
                         @Override
                         public boolean apply(AuditMessage input) {
                           return !(input.getEntityId() instanceof NamespacedEntityId &&
                             ((NamespacedEntityId) input.getEntityId()).getNamespace().equals(systemNs));
                         }
                       });

    Assert.assertEquals(expectedMessages, Lists.newArrayList(actualMessages));
  }

  private long getStreamSize(StreamId streamId) throws IOException {
    StreamAdmin streamAdmin = getStreamAdmin();
    StreamConfig config = streamAdmin.getConfig(streamId);

    Location generationLocation = StreamUtils.createGenerationLocation(config.getLocation(),
                                                                       StreamUtils.getGeneration(config));
    return StreamUtils.fetchStreamFilesSize(generationLocation);
  }

  // simply writes a static string to a stream
  private void writeEvent(StreamId streamId) throws IOException {
    StreamConfig streamConfig = getStreamAdmin().getConfig(streamId);
    FileWriter<StreamEvent> streamEventFileWriter = getFileWriterFactory().create(streamConfig, 0);
    streamEventFileWriter.append(new StreamEvent(Charsets.UTF_8.encode("EVENT")));
  }

  private void grantAndAssertSuccess(EntityId entityId, Principal principal, Set<Action> actions) throws Exception {
    Authorizer authorizer = getAuthorizer();
    Set<Privilege> existingPrivileges = authorizer.listPrivileges(principal);
    authorizer.grant(entityId, principal, actions);
    ImmutableSet.Builder<Privilege> expectedPrivilegesAfterGrant = ImmutableSet.builder();
    for (Action action : actions) {
      expectedPrivilegesAfterGrant.add(new Privilege(entityId, action));
    }
    Assert.assertEquals(Sets.union(existingPrivileges, expectedPrivilegesAfterGrant.build()),
                        authorizer.listPrivileges(principal));
  }

  private void revokeAndAssertSuccess(EntityId entityId, Principal principal, Set<Action> actions) throws Exception {
    Authorizer authorizer = getAuthorizer();
    Set<Privilege> existingPrivileges = authorizer.listPrivileges(principal);
    authorizer.revoke(entityId, principal, actions);
    Set<Privilege> revokedPrivileges = new HashSet<>();
    for (Action action : actions) {
      revokedPrivileges.add(new Privilege(entityId, action));
    }
    Assert.assertEquals(Sets.difference(existingPrivileges, revokedPrivileges), authorizer.listPrivileges(principal));
  }
}

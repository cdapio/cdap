/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.data.stream;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.data.file.FileWriter;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.store.NamespaceStore;
import org.apache.hadoop.security.UserGroupInformation;
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
import java.util.EnumSet;
import java.util.Properties;

/**
 * Base test class for stream file janitor.
 */
public abstract class StreamFileJanitorTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  protected static CConfiguration cConf = CConfiguration.create();

  private static final Principal ALICE = new Principal("ALICE", Principal.PrincipalType.USER);

  private static final Principal BOB = new Principal("BOB", Principal.PrincipalType.USER);

  protected abstract LocationFactory getLocationFactory();

  protected abstract NamespacedLocationFactory getNamespacedLocationFactory();

  protected abstract StreamAdmin getStreamAdmin();

  protected abstract NamespaceStore getNamespaceStore();

  protected abstract NamespaceAdmin getNamespaceAdmin();

  protected abstract CConfiguration getCConfiguration();

  protected abstract Authorizer getAuthorizer();

  protected abstract StreamFileJanitor getJanitor();

  protected abstract FileWriter<StreamEvent> createWriter(StreamId streamId) throws IOException;

  private Authorizer authorizer;

  private StreamFileJanitor janitor;

  @Before
  public void setup() throws Exception {
    // FileStreamAdmin expects namespace directory to exist.
    // Simulate namespace create, since its an inmemory-namespace admin
    getNamespaceAdmin().create(NamespaceMeta.DEFAULT);
    getNamespacedLocationFactory().get(NamespaceId.DEFAULT).mkdirs();
    janitor = getJanitor();
    authorizer = getAuthorizer();
    SecurityRequestContext.setUserId(ALICE.getName());
  }

  /**
   * Test the clean up of the janitor, also checks that user without privilege on the stream is also able to clean up
   * the stream.
   */
  @Test
  public void testCleanupGeneration() throws Exception {
    // Create a stream and performs couple truncate
    String streamName = "testCleanupGeneration";
    StreamId streamId = NamespaceId.DEFAULT.stream(streamName);
    authorizer.grant(Authorizable.fromEntityId(streamId), ALICE, EnumSet.of(Action.ADMIN));

    StreamAdmin streamAdmin = getStreamAdmin();
    streamAdmin.create(streamId);
    StreamConfig streamConfig = streamAdmin.getConfig(streamId);

    for (int i = 0; i < 5; i++) {
      FileWriter<StreamEvent> writer = createWriter(streamId);
      writer.append(StreamFileTestUtils.createEvent(System.currentTimeMillis(), "Testing"));
      writer.close();

      // Call cleanup before truncate. The current generation should stand.
      janitor.clean(streamConfig.getLocation(), streamConfig.getTTL(), System.currentTimeMillis());
      verifyGeneration(streamConfig, i);

      streamAdmin.truncate(streamId);
    }

    SecurityRequestContext.setUserId(BOB.getName());

    int generation = StreamUtils.getGeneration(streamConfig);
    Assert.assertEquals(5, generation);

    janitor.clean(streamConfig.getLocation(), streamConfig.getTTL(), System.currentTimeMillis());

    // Verify the stream directory should only contains the generation directory
    for (Location location : streamConfig.getLocation().list()) {
      if (location.isDirectory()) {
        Assert.assertEquals(generation, Integer.parseInt(location.getName()));
      }
    }
  }

  @Test
  public void testCleanupTTL() throws Exception {
    // Create a stream with 5 seconds TTL, partition duration of 2 seconds
    String streamName = "testCleanupTTL";
    StreamId streamId = NamespaceId.DEFAULT.stream(streamName);
    authorizer.grant(Authorizable.fromEntityId(streamId), ALICE, EnumSet.of(Action.ADMIN));

    StreamAdmin streamAdmin = getStreamAdmin();

    Properties properties = new Properties();
    properties.setProperty(Constants.Stream.PARTITION_DURATION, "2000");
    properties.setProperty(Constants.Stream.TTL, "5000");

    streamAdmin.create(streamId, properties);

    // Truncate to increment generation to 1. This make verification condition easier (won't affect correctness).
    streamAdmin.truncate(streamId);
    StreamConfig config = streamAdmin.getConfig(streamId);

    SecurityRequestContext.setUserId(BOB.getName());

    // Write data with different timestamps that spans across 5 partitions
    FileWriter<StreamEvent> writer = createWriter(streamId);

    for (int i = 0; i < 10; i++) {
      writer.append(StreamFileTestUtils.createEvent(i * 1000, "Testing " + i));
    }
    writer.close();

    // Should see 5 partitions
    Location generationLocation = StreamUtils.createGenerationLocation(config.getLocation(), 1);
    Assert.assertEquals(5, generationLocation.list().size());

    // Perform clean with current time = 10000 (10 seconds since epoch).
    // Since TTL = 5 seconds, 2 partitions will be remove (Ends at 2000 and ends at 4000).
    janitor.clean(config.getLocation(), config.getTTL(), 10000);

    Assert.assertEquals(3, generationLocation.list().size());

    // Cleanup again with current time = 16000, all partitions should be deleted.
    janitor.clean(config.getLocation(), config.getTTL(), 16000);
    Assert.assertTrue(generationLocation.list().isEmpty());
  }

  /**
   * Test clean up for all streams and verify user who does not have privileges on the streams is able to clean up.
   */
  @Test
  public void testCleanupDeletedStream() throws Exception {
    StreamId streamId = NamespaceId.DEFAULT.stream("cleanupDelete");
    StreamAdmin streamAdmin = getStreamAdmin();
    authorizer.grant(Authorizable.fromEntityId(streamId), ALICE, EnumSet.of(Action.ADMIN));
    streamAdmin.create(streamId);

    // Write some data
    try (FileWriter<StreamEvent> writer = createWriter(streamId)) {
      for (int i = 0; i < 10; i++) {
        writer.append(StreamFileTestUtils.createEvent(i * 1000, "Testing " + i));
      }
    }

    // Delete the stream
    streamAdmin.drop(streamId);
    SecurityRequestContext.setUserId(BOB.getName());

    // Run janitor. Should be running fine without exception.
    // Even Bob does not have privilege on the stream, he should be able to clean up the streams
    janitor.cleanAll();
  }

  private void verifyGeneration(StreamConfig config, int generation) throws IOException {
    Location generationLocation = StreamUtils.createGenerationLocation(config.getLocation(), generation);
    Assert.assertTrue(generationLocation.isDirectory());

    // There should be a partition directory inside
    for (Location location : generationLocation.list()) {
      if (location.isDirectory() && location.getName().indexOf('.') > 0) {
        return;
      }
    }

    throw new IOException("Not a valid generation directory");
  }

  static CConfiguration setupAuthzConfig() throws IOException {
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES, 0);
    LocationFactory locationFactory = new LocalLocationFactory(new File(tmpFolder.newFolder().toURI()));
    Location authorizerJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authorizerJar.toURI().getPath());
    // this is needed since now DefaultAuthorizationEnforcer expects this non-null
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, UserGroupInformation.getLoginUser().getShortUserName());
    return cConf;
  }
}

/*
 * Copyright © 2014-2023 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.ZkClientModule;
import io.cdap.cdap.common.guice.ZkDiscoveryModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.security.guice.CoreSecurityModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests covering the {@link DistributedKeyManager} implementation.
 */
public class
DistributedKeyManagerTest extends TestTokenManager {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedKeyManagerTest.class);
  private static final String OWNER_AUTH = "cdap:secret";
  private static final long TIMEOUT_SECONDS = 10;
  private static TestingCluster zkCluster;
  private static Injector injector1;
  private static Injector injector2;

  @BeforeClass
  public static void setup() throws Exception {
    zkCluster = new TestingCluster(1);
    zkCluster.start();
    String zkConnectString = zkCluster.getConnectString();
    LOG.info("Running ZK cluster at " + zkConnectString);
    CConfiguration cConf1 = CConfiguration.create();
    cConf1.setBoolean(Constants.Security.ENABLED, true);
    cConf1.set(Constants.Zookeeper.QUORUM, zkConnectString);

    CConfiguration cConf2 = CConfiguration.create();
    cConf2.setBoolean(Constants.Security.ENABLED, true);
    cConf2.set(Constants.Zookeeper.QUORUM, zkConnectString);

    List<Module> modules = new ArrayList<>();
    modules.add(new ConfigModule(cConf1, new Configuration()));
    modules.add(new IOModule());

    CoreSecurityModule coreSecurityModule = CoreSecurityRuntimeModule.getDistributedModule(cConf1);
    modules.add(coreSecurityModule);
    if (coreSecurityModule.requiresZKClient()) {
      modules.add(new ZkClientModule());
      modules.add(new ZkDiscoveryModule());
    }
    injector1 = Guice.createInjector(modules);

    modules.clear();
    modules.add(new ConfigModule(cConf2, new Configuration()));
    modules.add(new IOModule());

    coreSecurityModule = CoreSecurityRuntimeModule.getDistributedModule(cConf2);
    modules.add(coreSecurityModule);
    if (coreSecurityModule.requiresZKClient()) {
      modules.add(new ZkClientModule());
      modules.add(new ZkDiscoveryModule());
    }

    injector2 = Guice.createInjector(modules);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    zkCluster.stop();
  }

  @Test
  public void testKeyDistribution() throws Exception {
    DistributedKeyManager manager1 = getKeyManager(injector1, true);
    DistributedKeyManager manager2 = getKeyManager(injector2, false);
    TimeUnit.MILLISECONDS.sleep(1000);

    TestingTokenManager tokenManager1 =
        new TestingTokenManager(manager1, injector1.getInstance(UserIdentityCodec.class));
    TestingTokenManager tokenManager2 =
        new TestingTokenManager(manager2, injector2.getInstance(UserIdentityCodec.class));
    tokenManager1.startAndWait();
    tokenManager2.startAndWait();

    long now = System.currentTimeMillis();
    UserIdentity ident1 = new UserIdentity("testuser", UserIdentity.IdentifierType.EXTERNAL,
        Lists.newArrayList("users", "admins"), now,
        now + 60 * 60 * 1000);
    AccessToken token1 = tokenManager1.signIdentifier(ident1);
    // make sure the second token manager has the secret key required to validate the signature
    tokenManager2.waitForKey(tokenManager1.getCurrentKey().getKeyId(), 2000, TimeUnit.MILLISECONDS);
    tokenManager2.validateSecret(token1);

    tokenManager2.waitForCurrentKey(2000, TimeUnit.MILLISECONDS);
    AccessToken token2 = tokenManager2.signIdentifier(ident1);
    tokenManager1.validateSecret(token2);
    assertEquals(token1.getIdentifier().getUsername(), token2.getIdentifier().getUsername());
    assertEquals(token1.getIdentifier().getGroups(), token2.getIdentifier().getGroups());
    assertEquals(token1, token2);

    tokenManager1.stopAndWait();
    tokenManager2.stopAndWait();
  }

  @Test
  public void testGetAcls() throws Exception {
    CConfiguration kerbConf = CConfiguration.create();
    kerbConf.set(Constants.Security.KERBEROS_ENABLED, "true");
    kerbConf.set(Constants.Security.Authentication.MODE, "MANAGED");
    kerbConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "prinicpal@REALM.NET");
    kerbConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH, "/path/to/keytab");
    Assert.assertEquals(ZooDefs.Ids.CREATOR_ALL_ACL, DistributedKeyManager.getACLs(kerbConf));

    CConfiguration noKerbConf = CConfiguration.create();
    noKerbConf.unset(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
    Assert.assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, DistributedKeyManager.getACLs(noKerbConf));
  }

  /**
   * Runs the real key distribution flow with the ACLs used on Kerberos clusters: the leader
   * creates the keys parent locked to its identity, a peer with the same identity still shares
   * keys and validates tokens, and a client without credentials can neither add keys nor fail
   * to start.
   */
  @Test
  public void testKeyDistributionWithRestrictedAcls() throws Exception {
    String namespace = "/restrictedAclKeys";
    ZKClientService ownerClient1 = null;
    ZKClientService ownerClient2 = null;
    ZKClientService anonymousClient = null;
    List<Service> services = new ArrayList<>();
    try {
      ownerClient1 = startClient(OWNER_AUTH);
      ownerClient2 = startClient(OWNER_AUTH);
      anonymousClient = startClient(null);

      WaitableDistributedKeyManager leader = startRestrictedKeyManager(ownerClient1, namespace);
      services.add(leader);
      Tasks.waitFor(true, () -> leader.getCurrentKey() != null, TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertEquals(digestAcl(OWNER_AUTH), ownerClient1.getACL(namespace + "/keys")
          .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getACL());

      WaitableDistributedKeyManager follower = startRestrictedKeyManager(ownerClient2, namespace);
      services.add(follower);
      TestingTokenManager leaderTokens =
          new TestingTokenManager(leader, injector1.getInstance(UserIdentityCodec.class));
      TestingTokenManager followerTokens =
          new TestingTokenManager(follower, injector2.getInstance(UserIdentityCodec.class));
      leaderTokens.startAndWait();
      followerTokens.startAndWait();
      services.add(leaderTokens);
      services.add(followerTokens);

      long now = System.currentTimeMillis();
      UserIdentity identity = new UserIdentity("testuser", UserIdentity.IdentifierType.EXTERNAL,
          Lists.newArrayList("users"), now, now + TimeUnit.HOURS.toMillis(1));
      AccessToken token = leaderTokens.signIdentifier(identity);
      followerTokens.waitForKey(leader.getCurrentKey().getKeyId(), TIMEOUT_SECONDS,
          TimeUnit.SECONDS);
      followerTokens.validateSecret(token);

      // A participant without credentials, such as a program container, starts without error
      // but cannot add a key of its own.
      WaitableDistributedKeyManager anonymous =
          startRestrictedKeyManager(anonymousClient, namespace);
      services.add(anonymous);
      assertEquals(Service.State.RUNNING, anonymous.state());
      try {
        anonymousClient.create(namespace + "/keys/forged", new byte[] {1}, CreateMode.PERSISTENT)
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        fail("Expected NoAuthException when adding a key without credentials");
      } catch (ExecutionException e) {
        assertTrue("Unexpected cause: " + e.getCause(),
            e.getCause() instanceof KeeperException.NoAuthException);
      }
    } finally {
      stopAll(services, ownerClient1, ownerClient2, anonymousClient);
    }
  }

  /**
   * Covers a participant without credentials that starts before the keys parent exists. It must
   * start, fail to write a key as leader without creating an open parent, and let a credentialed
   * participant create the parent with restricted ACLs afterwards.
   */
  @Test
  public void testUnauthenticatedFirstParticipantDoesNotCreateOpenKeysParent() throws Exception {
    String namespace = "/restrictedAclFreshKeys";
    ZKClientService anonymousClient = null;
    ZKClientService ownerClient = null;
    List<Service> services = new ArrayList<>();
    try {
      anonymousClient = startClient(null);
      ownerClient = startClient(OWNER_AUTH);

      WaitableDistributedKeyManager anonymous =
          startRestrictedKeyManager(anonymousClient, namespace);
      services.add(anonymous);
      assertEquals(Service.State.RUNNING, anonymous.state());
      // As the only participant it becomes leader and tries to write the first key.
      assertTrue(anonymous.awaitAddKeyAttempt(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertNull(anonymous.getCurrentKey());
      assertNull(ownerClient.exists(namespace + "/keys").get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

      WaitableDistributedKeyManager owner = startRestrictedKeyManager(ownerClient, namespace);
      services.add(owner);
      Tasks.waitFor(true, () -> owner.getCurrentKey() != null, TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertEquals(digestAcl(OWNER_AUTH), ownerClient.getACL(namespace + "/keys")
          .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getACL());
    } finally {
      stopAll(services, anonymousClient, ownerClient);
    }
  }

  @Override
  protected ImmutablePair<TokenManager, Codec<AccessToken>> getTokenManagerAndCodec()
      throws Exception {
    DistributedKeyManager keyManager = getKeyManager(injector1, true);
    TokenManager tokenManager = new TokenManager(keyManager,
        injector1.getInstance(UserIdentityCodec.class));
    tokenManager.startAndWait();
    return new ImmutablePair<>(tokenManager, injector1.getInstance(AccessTokenCodec.class));
  }

  private DistributedKeyManager getKeyManager(Injector injector, boolean expectLeader)
      throws Exception {
    ZKClientService zk = injector.getInstance(ZKClientService.class);
    zk.startAndWait();
    WaitableDistributedKeyManager keyManager =
        new WaitableDistributedKeyManager(injector.getInstance(CConfiguration.class),
            injector.getInstance(Key.get(new TypeLiteral<Codec<KeyIdentifier>>() {
            })),
            zk);

    keyManager.startAndWait();
    if (expectLeader) {
      Tasks.waitFor(true, () -> keyManager.getCurrentKey() != null, 5L, TimeUnit.SECONDS);
    }
    return keyManager;
  }

  /**
   * Starts a key manager with {@code CREATOR_ALL_ACL} on a client namespaced under the given path,
   * so its keys parent is separate from the one the other tests create with open ACLs.
   */
  private WaitableDistributedKeyManager startRestrictedKeyManager(ZKClientService client,
      String namespace) {
    WaitableDistributedKeyManager keyManager =
        new WaitableDistributedKeyManager(injector1.getInstance(CConfiguration.class),
            injector1.getInstance(Key.get(new TypeLiteral<Codec<KeyIdentifier>>() {
            })),
            ZKClients.namespace(client, namespace), ZooDefs.Ids.CREATOR_ALL_ACL);
    keyManager.startAndWait();
    return keyManager;
  }

  private static ZKClientService startClient(@Nullable String digestAuth) {
    ZKClientService.Builder builder = ZKClientService.Builder.of(zkCluster.getConnectString());
    if (digestAuth != null) {
      builder.addAuthInfo("digest", digestAuth.getBytes(StandardCharsets.UTF_8));
    }
    ZKClientService client = builder.build();
    client.startAndWait();
    return client;
  }

  private static List<ACL> digestAcl(String auth) throws Exception {
    return Collections.singletonList(new ACL(ZooDefs.Perms.ALL,
        new Id("digest", DigestAuthenticationProvider.generateDigest(auth))));
  }

  private static void stopAll(List<Service> services, ZKClientService... clients) {
    for (Service service : Lists.reverse(services)) {
      try {
        service.stopAndWait();
      } catch (Exception e) {
        LOG.warn("Failed to stop {}", service, e);
      }
    }
    for (ZKClientService client : clients) {
      if (client == null) {
        continue;
      }
      try {
        client.stopAndWait();
      } catch (Exception e) {
        LOG.warn("Failed to stop test ZKClientService", e);
      }
    }
  }

  private static class WaitableDistributedKeyManager extends DistributedKeyManager {

    private final CountDownLatch addKeyAttempted = new CountDownLatch(1);

    WaitableDistributedKeyManager(CConfiguration conf, Codec<KeyIdentifier> codec, ZKClient zk) {
      this(conf, codec, zk, Lists.newArrayList(ZooDefs.Ids.OPEN_ACL_UNSAFE));
    }

    WaitableDistributedKeyManager(CConfiguration conf, Codec<KeyIdentifier> codec, ZKClient zk,
        List<ACL> acls) {
      super(conf, codec, zk, acls);
    }

    public KeyIdentifier getCurrentKey() {
      return currentKey;
    }

    public boolean hasKey(int keyId) {
      return super.hasKey(keyId);
    }

    @Override
    protected void addKey(KeyIdentifier key) {
      try {
        super.addKey(key);
      } finally {
        addKeyAttempted.countDown();
      }
    }

    boolean awaitAddKeyAttempt(long timeout, TimeUnit unit) throws InterruptedException {
      return addKeyAttempted.await(timeout, unit);
    }
  }

  private static class TestingTokenManager extends TokenManager {

    private TestingTokenManager(KeyManager keyManager, Codec<UserIdentity> identifierCodec) {
      super(keyManager, identifierCodec);
    }

    public KeyIdentifier getCurrentKey() {
      if (keyManager instanceof WaitableDistributedKeyManager) {
        return ((WaitableDistributedKeyManager) keyManager).getCurrentKey();
      }
      return null;
    }

    void waitForKey(int keyId, long duration,
        TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
      if (keyManager instanceof WaitableDistributedKeyManager) {
        WaitableDistributedKeyManager waitKeyManager = (WaitableDistributedKeyManager) keyManager;
        Tasks.waitFor(true, () -> waitKeyManager.hasKey(keyId), duration, unit);
      }
    }

    void waitForCurrentKey(long duration,
        TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
      if (keyManager instanceof WaitableDistributedKeyManager) {
        WaitableDistributedKeyManager waitKeyManager = (WaitableDistributedKeyManager) keyManager;
        Tasks.waitFor(true, () -> waitKeyManager.getCurrentKey() != null, duration, unit);
      }
    }
  }
}

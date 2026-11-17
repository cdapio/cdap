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

package io.cdap.cdap.security.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.ZkClientModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.utils.Tasks;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.zookeeper.ACLData;
import org.apache.twill.zookeeper.ForwardingZKClient;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests covering the {@link SharedResourceCache} implementation.
 */
public class SharedResourceCacheTest {

  private static final String ZK_NAMESPACE = "/SharedResourceCacheTest";
  private static final String OWNER_AUTH = "cdap:secret";
  private static final long TIMEOUT_SECONDS = 10;
  private static final Logger LOG = LoggerFactory.getLogger(SharedResourceCacheTest.class);
  private static TestingCluster zkCluster;
  private static String zkConnectString;
  private static Injector injector1;
  private static Injector injector2;

  @BeforeClass
  public static void startUp() throws Exception {
    zkCluster = new TestingCluster(1);
    zkCluster.start();
    zkConnectString = zkCluster.getConnectString();
    LOG.info("Running ZK cluster at " + zkConnectString);
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkConnectString);
    injector1 = Guice.createInjector(new ConfigModule(cConf, new Configuration()),
        new ZkClientModule());
    injector2 = Guice.createInjector(new ConfigModule(cConf, new Configuration()),
        new ZkClientModule());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    zkCluster.stop();
  }

  @Test
  public void testCache() throws Exception {
    String parentNode = ZK_NAMESPACE + "/testCache";

    List<ACL> acls = Lists.newArrayList(ZooDefs.Ids.OPEN_ACL_UNSAFE);

    // create 2 cache instances
    ZKClientService zkClient1 = injector1.getInstance(ZKClientService.class);
    zkClient1.startAndWait();
    SharedResourceCache<String> cache1 =
        new SharedResourceCache<>(zkClient1, new StringCodec(), parentNode, acls);
    cache1.init();

    // add items to one and wait for them to show up in the second
    String key1 = "key1";
    String value1 = "value1";
    cache1.put(key1, value1);

    ZKClientService zkClient2 = injector2.getInstance(ZKClientService.class);
    zkClient2.startAndWait();
    SharedResourceCache<String> cache2 =
        new SharedResourceCache<>(zkClient2, new StringCodec(), parentNode, acls);
    cache2.init();

    waitForEntry(cache2, key1, value1, 10000);
    assertEquals(cache1.get(key1), cache2.get(key1));

    final String key2 = "key2";
    String value2 = "value2";
    cache1.put(key2, value2);

    waitForEntry(cache2, key2, value2, 10000);
    assertEquals(cache1.get(key2), cache2.get(key2));

    final String key3 = "key3";
    String value3 = "value3";
    cache2.put(key3, value3);

    waitForEntry(cache1, key3, value3, 10000);
    assertEquals(cache2.get(key3), cache1.get(key3));

    // replace an existing key
    final String value2new = "value2.2";
    final SettableFuture<String> value2future = SettableFuture.create();
    ResourceListener<String> value2listener = new BaseResourceListener<String>() {
      @Override
      public void onResourceUpdate(String name, String instance) {
        LOG.info("Resource updated: {}={}", name, instance);
        if (key2.equals(name) && value2new.equals(instance)) {
          value2future.set(instance);
        }
      }
    };

    cache2.addListener(value2listener);
    cache1.put(key2, value2new);

    assertEquals(value2new, value2future.get(10, TimeUnit.SECONDS));
    assertEquals(value2new, cache2.get(key2));

    cache2.removeListener(value2listener);

    // remove items from the second and wait for them to disappear from the first
    // Use a latch to make sure both cache see the changes
    final CountDownLatch key3RemoveLatch = new CountDownLatch(2);
    cache1.addListener(new BaseResourceListener<String>() {
      @Override
      public void onResourceDelete(String name) {
        LOG.info("Resource deleted on cache 1 {}", name);
        if (name.equals(key3)) {
          key3RemoveLatch.countDown();
        }
      }
    });

    final SettableFuture<String> key3RemoveFuture = SettableFuture.create();
    ResourceListener<String> key3Listener = new BaseResourceListener<String>() {
      @Override
      public void onResourceDelete(String name) {
        LOG.info("Resource deleted on cache 2 {}", name);
        if (name.equals(key3)) {
          key3RemoveFuture.set(name);
          key3RemoveLatch.countDown();
        }
      }
    };

    cache2.addListener(key3Listener);
    cache1.remove(key3);
    String removedKey = key3RemoveFuture.get();
    assertEquals(key3, removedKey);
    assertNull(cache2.get(key3));

    key3RemoveLatch.await(5, TimeUnit.SECONDS);

    // verify that cache contents are equal
    assertEquals(cache1, cache2);
  }

  @Test
  public void testParentCreatedWithConfiguredAcl() throws Exception {
    String parent = ZK_NAMESPACE + "/parentAcl";
    ZKClientService owner = null;
    try {
      owner = startClient(OWNER_AUTH);
      initCache(owner, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      assertEquals(digestAcl(OWNER_AUTH),
          aclOf(owner, parent));
    } finally {
      stopAll(owner);
    }
  }

  @Test
  public void testOtherClientCannotAddChildUnderRestrictedParent() throws Exception {
    String parent = ZK_NAMESPACE + "/restrictedParent";
    ZKClientService owner = null;
    ZKClientService anonymous = null;
    ZKClientService otherUser = null;
    try {
      owner = startClient(OWNER_AUTH);
      anonymous = startClient(null);
      otherUser = startClient("intruder:pw");
      initCache(owner, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      for (ZKClientService intruder : Arrays.asList(anonymous, otherUser)) {
        assertNoAuth(intruder.create(parent + "/injected", Bytes.toBytes("forged"),
            CreateMode.PERSISTENT));
      }
      assertTrue(owner.getChildren(parent).get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .getChildren().isEmpty());
    } finally {
      stopAll(owner, anonymous, otherUser);
    }
  }

  @Test
  public void testSameAuthClientsShareEntriesUnderRestrictedParent() throws Exception {
    String parent = ZK_NAMESPACE + "/sharedRestrictedParent";
    ZKClientService client1 = null;
    ZKClientService client2 = null;
    try {
      client1 = startClient(OWNER_AUTH);
      client2 = startClient(OWNER_AUTH);
      SharedResourceCache<String> cache1 = initCache(client1, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      SharedResourceCache<String> cache2 = initCache(client2, parent, ZooDefs.Ids.CREATOR_ALL_ACL);

      cache1.put("key1", "value1");
      waitForEntry(cache2, "key1", "value1", 10000);
      cache2.put("key2", "value2");
      waitForEntry(cache1, "key2", "value2", 10000);

      // remove() is asynchronous; wait until the znode and both in-memory entries disappear.
      cache2.remove("key1");
      final ZKClientService reader = client1;
      Tasks.waitFor(true,
          () -> reader.exists(parent + "/key1").get(TIMEOUT_SECONDS, TimeUnit.SECONDS) == null
              && cache1.get("key1") == null && cache2.get("key1") == null,
          TIMEOUT_SECONDS, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      assertEquals("value2", cache1.get("key2"));
    } finally {
      stopAll(client1, client2);
    }
  }

  @Test
  public void testOpenOrNullAclParentStaysOpen() throws Exception {
    String parent = ZK_NAMESPACE + "/openParent";
    String nullAclParent = ZK_NAMESPACE + "/nullAclParent";
    ZKClientService owner = null;
    ZKClientService other = null;
    try {
      owner = startClient(null);
      other = startClient(null);
      initCache(owner, parent, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, aclOf(owner, parent));
      // Re-initializing on an existing open parent must not write its ACL at all.
      initCache(owner, parent, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      assertEquals(0, owner.exists(parent).get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getAversion());
      other.create(parent + "/child", null, CreateMode.PERSISTENT)
          .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

      SharedResourceCache<String> nullAclCache = initCache(owner, nullAclParent, null);
      nullAclCache.put("key1", "value1");
      assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, aclOf(owner, nullAclParent));
      assertEquals("value1", nullAclCache.get("key1"));
    } finally {
      stopAll(owner, other);
    }
  }

  @Test
  public void testInitRestrictsExistingOpenParentAcl() throws Exception {
    String parent = ZK_NAMESPACE + "/preExistingOpenParent";
    ZKClientService owner = null;
    ZKClientService anonymous = null;
    try {
      owner = startClient(OWNER_AUTH);
      anonymous = startClient(null);
      // Older versions created the parent writable by any client.
      owner.create(parent, null, CreateMode.PERSISTENT).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      initCache(owner, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      assertEquals(digestAcl(OWNER_AUTH),
          aclOf(owner, parent));
      assertNoAuth(anonymous.create(parent + "/injected", Bytes.toBytes("forged"),
          CreateMode.PERSISTENT));
    } finally {
      stopAll(owner, anonymous);
    }
  }

  @Test
  public void testInitDoesNotModifyExistingCustomParentAcl() throws Exception {
    String parent = ZK_NAMESPACE + "/preExistingCustomParent";
    ZKClientService owner = null;
    try {
      owner = startClient(OWNER_AUTH);
      // An operator-managed ACL that differs from both the open ACL and the configured ACL.
      List<ACL> customAcl = Arrays.asList(
          new ACL(ZooDefs.Perms.ALL, new Id("digest",
              DigestAuthenticationProvider.generateDigest(OWNER_AUTH))),
          new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
      owner.create(parent, null, CreateMode.PERSISTENT, customAcl)
          .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      initCache(owner, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      assertEquals(customAcl, aclOf(owner, parent));
    } finally {
      stopAll(owner);
    }
  }

  @Test
  public void testUnauthenticatedInitLeavesExistingOpenParentUnchanged() throws Exception {
    String parent = ZK_NAMESPACE + "/preExistingOpenParentAnonymous";
    ZKClientService anonymous = null;
    try {
      anonymous = startClient(null);
      anonymous.create(parent, null, CreateMode.PERSISTENT).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      // ZooKeeper rejects the restricted ACL for a session without credentials; init must
      // still succeed.
      initCache(anonymous, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, aclOf(anonymous, parent));
    } finally {
      stopAll(anonymous);
    }
  }

  @Test
  public void testInitIgnoresConcurrentParentAclChange() throws Exception {
    String parent = ZK_NAMESPACE + "/concurrentAclChangeParent";
    ZKClientService owner = null;
    ZKClientService other = null;
    try {
      owner = startClient(OWNER_AUTH);
      other = startClient(null);
      owner.create(parent, null, CreateMode.PERSISTENT).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      final ZKClientService concurrentWriter = other;
      // Another instance changes the ACL version right after this cache reads the ACL, so the
      // cache's version-checked setACL fails and must be ignored.
      ZKClient racingClient = runAfterGetAcl(owner,
          path -> concurrentWriter.setACL(path, ZooDefs.Ids.OPEN_ACL_UNSAFE)
              .get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      SharedResourceCache<String> cache =
          new SharedResourceCache<>(racingClient, new StringCodec(), parent,
              ZooDefs.Ids.CREATOR_ALL_ACL);
      cache.init();
      assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, aclOf(owner, parent));
    } finally {
      stopAll(owner, other);
    }
  }

  @Test
  public void testInitSurvivesParentDeletedBeforeAclRestriction() throws Exception {
    String parent = ZK_NAMESPACE + "/deletedBeforeRestrictParent";
    ZKClientService owner = null;
    try {
      owner = startClient(OWNER_AUTH);
      owner.create(parent, null, CreateMode.PERSISTENT).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      final ZKClientService deleter = owner;
      // The parent disappears between reading its ACL and restricting it, so setACL fails with
      // NoNodeException. That is neither InvalidACL nor BadVersion, and init must still succeed.
      ZKClient racingClient = runAfterGetAcl(owner,
          path -> deleter.delete(path).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      SharedResourceCache<String> cache =
          new SharedResourceCache<>(racingClient, new StringCodec(), parent,
              ZooDefs.Ids.CREATOR_ALL_ACL);
      cache.init();
      assertNull(owner.exists(parent).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertNull(cache.get("key1"));
    } finally {
      stopAll(owner);
    }
  }

  @Test
  public void testUnauthenticatedInitWithRestrictedAclDoesNotCreateOpenParent() throws Exception {
    String parent = ZK_NAMESPACE + "/unauthenticatedInit";
    ZKClientService anonymous = null;
    ZKClientService owner = null;
    try {
      anonymous = startClient(null);
      owner = startClient(OWNER_AUTH);
      initCache(anonymous, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      assertNull(anonymous.exists(parent).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

      SharedResourceCache<String> ownerCache =
          initCache(owner, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      assertEquals(digestAcl(OWNER_AUTH),
          aclOf(owner, parent));
      ownerCache.put("key1", "value1");
      assertNoAuth(anonymous.getChildren(parent));
      assertNoAuth(anonymous.getData(parent + "/key1"));
    } finally {
      stopAll(anonymous, owner);
    }
  }

  @Test
  public void testUnauthenticatedInitLoadsEntriesOnceReadableParentIsCreated() throws Exception {
    String parent = ZK_NAMESPACE + "/delayedReadableParent";
    // Place the world-readable entry first so the auth-scheme check iterates past it.
    List<ACL> creatorWriteWorldRead = Arrays.asList(
        new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE),
        new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
    ZKClientService anonymous = null;
    ZKClientService owner = null;
    try {
      anonymous = startClient(null);
      owner = startClient(OWNER_AUTH);
      SharedResourceCache<String> readerCache = initCache(anonymous, parent, creatorWriteWorldRead);
      assertNull(anonymous.exists(parent).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

      SharedResourceCache<String> ownerCache = initCache(owner, parent, creatorWriteWorldRead);
      ownerCache.put("key1", "value1");
      waitForEntry(readerCache, "key1", "value1", 10000);
      assertNoAuth(anonymous.create(parent + "/injected", Bytes.toBytes("forged"),
          CreateMode.PERSISTENT));
    } finally {
      stopAll(anonymous, owner);
    }
  }

  @Test
  public void testInitPropagatesNonAuthErrorsAndMalformedAcls() throws Exception {
    String restrictedRoot = ZK_NAMESPACE + "/restrictedRoot";
    ZKClientService owner = null;
    ZKClientService otherUser = null;
    try {
      owner = startClient(OWNER_AUTH);
      otherUser = startClient("intruder:pw");
      owner.create(restrictedRoot, null, CreateMode.PERSISTENT, ZooDefs.Ids.CREATOR_ALL_ACL)
          .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

      try {
        initCache(otherUser, restrictedRoot + "/cache", ZooDefs.Ids.OPEN_ACL_UNSAFE);
        fail("Expected NoAuthException when ancestor znode denies CREATE");
      } catch (RuntimeException expected) {
        assertTrue("Unexpected cause: " + expected.getCause(),
            expected.getCause() instanceof KeeperException.NoAuthException);
      }

      // Auth-scheme ACLs only tolerate InvalidACL; other errors such as NoAuth still propagate.
      try {
        initCache(otherUser, restrictedRoot + "/authAclCache", ZooDefs.Ids.CREATOR_ALL_ACL);
        fail("Expected NoAuthException when ancestor znode denies CREATE");
      } catch (RuntimeException expected) {
        assertTrue("Unexpected cause: " + expected.getCause(),
            expected.getCause() instanceof KeeperException.NoAuthException);
      }

      try {
        initCache(owner, ZK_NAMESPACE + "/emptyAclParent", Collections.emptyList());
        fail("Expected InvalidACLException when ACL list is empty");
      } catch (RuntimeException expected) {
        assertTrue("Unexpected cause: " + expected.getCause(),
            expected.getCause() instanceof KeeperException.InvalidACLException);
      }

      List<ACL> malformedAcls = Arrays.asList(
          new ACL(ZooDefs.Perms.ALL, new Id("unknown", "id")),
          new ACL(ZooDefs.Perms.ALL, new Id("digest", "missing-colon")));
      for (int i = 0; i < malformedAcls.size(); i++) {
        ACL malformedAcl = malformedAcls.get(i);
        try {
          initCache(owner, ZK_NAMESPACE + "/malformedAclParent" + i,
              Collections.singletonList(malformedAcl));
          fail("Expected InvalidACLException for malformed ACL: " + malformedAcl);
        } catch (RuntimeException expected) {
          assertTrue("Unexpected cause for " + malformedAcl + ": " + expected.getCause(),
              expected.getCause() instanceof KeeperException.InvalidACLException);
        }
      }
    } finally {
      stopAll(owner, otherUser);
    }
  }

  @Test
  public void testUnauthorizedPutOnExistingRestrictedParentFailsAndNotifiesError()
      throws Exception {
    String parent = ZK_NAMESPACE + "/unauthorizedPutExistingParent";
    ZKClientService owner = null;
    ZKClientService otherUser = null;
    try {
      owner = startClient(OWNER_AUTH);
      otherUser = startClient("intruder:pw");
      final SharedResourceCache<String> ownerCache =
          initCache(owner, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      ownerCache.put("key1", "value1");

      SharedResourceCache<String> intruderCache =
          initCache(otherUser, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      AtomicReference<Throwable> reportedError = new AtomicReference<>();
      CountDownLatch errorLatch = new CountDownLatch(1);
      intruderCache.addListener(new BaseResourceListener<String>() {
        @Override
        public void onError(String name, Throwable throwable) {
          if ("injected".equals(name)) {
            reportedError.set(throwable);
            errorLatch.countDown();
          }
        }
      });

      try {
        intruderCache.put("injected", "forged");
        fail("Expected put by differently authenticated client under restricted parent to fail");
      } catch (RuntimeException expected) {
        assertTrue("Expected ExecutionException cause, got: " + expected.getCause(),
            expected.getCause() instanceof ExecutionException);
        assertTrue("Expected NoAuthException root cause, got: " + expected.getCause().getCause(),
            expected.getCause().getCause() instanceof KeeperException.NoAuthException);
      }

      assertTrue(errorLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertTrue(reportedError.get() instanceof KeeperException.NoAuthException);
      assertNull(intruderCache.get("injected"));
      assertNull(ownerCache.get("injected"));
      assertEquals("value1", ownerCache.get("key1"));
      assertEquals(Collections.singletonList("key1"),
          owner.getChildren(parent).get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getChildren());
    } finally {
      stopAll(owner, otherUser);
    }
  }

  @Test
  public void testPutRestoresInterruptFlagWhenInterrupted() throws Exception {
    String parent = ZK_NAMESPACE + "/interruptedPut";
    ZKClientService owner = null;
    try {
      owner = startClient(OWNER_AUTH);
      SharedResourceCache<String> cache = initCache(owner, parent, ZooDefs.Ids.CREATOR_ALL_ACL);

      // 1. Interrupt before put(): hits createParentIfAbsent() catch (InterruptedException).
      Thread.currentThread().interrupt();
      try {
        cache.put("key1", "value1");
        fail("Expected put() to fail when thread is interrupted before createParentIfAbsent()");
      } catch (RuntimeException expected) {
        assertTrue("Expected InterruptedException cause, got: " + expected.getCause(),
            expected.getCause() instanceof InterruptedException);
        assertTrue("Expected interrupt flag to remain set", Thread.interrupted());
      }

      // 2. Interrupt during encode(): hits completion.get() catch (InterruptedException).
      Codec<String> interruptingEncodeCodec = new Codec<String>() {
        @Override
        public byte[] encode(String object) {
          Thread.currentThread().interrupt();
          return Bytes.toBytes(object);
        }

        @Override
        public String decode(byte[] data) {
          return Bytes.toString(data);
        }
      };
      SharedResourceCache<String> completionInterruptCache =
          new SharedResourceCache<>(owner, interruptingEncodeCodec, parent,
              ZooDefs.Ids.CREATOR_ALL_ACL);
      completionInterruptCache.init();
      try {
        completionInterruptCache.put("key2", "value2");
        fail("Expected put() to fail when thread is interrupted before completion.get()");
      } catch (RuntimeException expected) {
        assertTrue("Expected InterruptedException cause, got: " + expected.getCause(),
            expected.getCause() instanceof InterruptedException);
        assertTrue("Expected interrupt flag to remain set", Thread.interrupted());
      }
    } finally {
      Thread.interrupted();
      stopAll(owner);
    }
  }

  @Test
  public void testUnauthenticatedPutOnMissingParentDoesNotCreateOpenParent() throws Exception {
    String parent = ZK_NAMESPACE + "/unauthenticatedPut";
    ZKClientService anonymous = null;
    ZKClientService owner = null;
    try {
      anonymous = startClient(null);
      owner = startClient(OWNER_AUTH);
      final SharedResourceCache<String> unauthenticatedCache =
          initCache(anonymous, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      AtomicReference<Throwable> reportedError = new AtomicReference<>();
      CountDownLatch errorLatch = new CountDownLatch(1);
      unauthenticatedCache.addListener(new BaseResourceListener<String>() {
        @Override
        public void onError(String name, Throwable throwable) {
          if ("key1".equals(name)) {
            reportedError.set(throwable);
            errorLatch.countDown();
          }
        }
      });

      try {
        unauthenticatedCache.put("key1", "value1");
        fail("Expected unauthenticated put with CREATOR_ALL_ACL to fail");
      } catch (RuntimeException expected) {
        assertTrue("Unexpected cause: " + expected.getCause(),
            expected.getCause() instanceof ExecutionException);
        assertTrue("Unexpected cause: " + expected.getCause().getCause(),
            expected.getCause().getCause() instanceof KeeperException.InvalidACLException);
      }

      assertTrue(errorLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertTrue(reportedError.get() instanceof KeeperException.InvalidACLException);
      assertNull(unauthenticatedCache.get("key1"));
      assertNull(anonymous.exists(parent).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

      // An authenticated writer on a missing parent creates the parent with the configured ACL.
      SharedResourceCache<String> ownerCache =
          initCache(owner, parent, ZooDefs.Ids.CREATOR_ALL_ACL);
      owner.delete(parent).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertNull(owner.exists(parent).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      ownerCache.put("key1", "value1");
      assertEquals(digestAcl(OWNER_AUTH),
          aclOf(owner, parent));
      assertEquals(digestAcl(OWNER_AUTH),
          aclOf(owner, parent + "/key1"));
      assertEquals("value1", ownerCache.get("key1"));
    } finally {
      stopAll(anonymous, owner);
    }
  }

  private static ZKClientService startClient(@Nullable String digestAuth) {
    ZKClientService.Builder builder = ZKClientService.Builder.of(zkConnectString);
    if (digestAuth != null) {
      builder.addAuthInfo("digest", Bytes.toBytes(digestAuth));
    }
    ZKClientService client = builder.build();
    client.startAndWait();
    return client;
  }

  private static SharedResourceCache<String> initCache(ZKClientService client, String parent,
      List<ACL> acl) throws InterruptedException {
    SharedResourceCache<String> cache =
        new SharedResourceCache<>(client, new StringCodec(), parent, acl);
    cache.init();
    return cache;
  }

  private static List<ACL> aclOf(ZKClient client, String path) throws Exception {
    return client.getACL(path).get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getACL();
  }

  /**
   * Returns a client that runs {@code action} on a path right after reading that path's ACL, to
   * simulate another instance acting between this cache's getACL and setACL calls.
   */
  private static ZKClient runAfterGetAcl(ZKClient delegate, PathAction action) {
    return new ForwardingZKClient(delegate) {
      @Override
      public OperationFuture<ACLData> getACL(String path) {
        OperationFuture<ACLData> result = super.getACL(path);
        try {
          result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
          action.run(path);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return result;
      }
    };
  }

  /**
   * An action on a znode path that may throw.
   */
  private interface PathAction {
    void run(String path) throws Exception;
  }

  private static List<ACL> digestAcl(String auth) throws NoSuchAlgorithmException {
    return Collections.singletonList(new ACL(ZooDefs.Perms.ALL,
        new Id("digest", DigestAuthenticationProvider.generateDigest(auth))));
  }

  private static void assertNoAuth(OperationFuture<?> future) throws Exception {
    try {
      future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      fail("Expected NoAuthException");
    } catch (ExecutionException e) {
      assertTrue("Unexpected cause: " + e.getCause(),
          e.getCause() instanceof KeeperException.NoAuthException);
    }
  }

  private static void stopAll(ZKClientService... clients) {
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


  private static final class StringCodec implements Codec<String> {

    @Override
    public byte[] encode(String object) throws IOException {
      return Bytes.toBytes(object);
    }

    @Override
    public String decode(byte[] data) throws IOException {
      return Bytes.toString(data);
    }
  }

  private void waitForEntry(SharedResourceCache<String> cache, String key, String expectedValue,
      long timeToWaitMillis) throws InterruptedException {
    String value = cache.get(key);
    boolean isPresent = expectedValue.equals(value);

    Stopwatch watch = new Stopwatch().start();
    while (!isPresent && watch.elapsedTime(TimeUnit.MILLISECONDS) < timeToWaitMillis) {
      TimeUnit.MILLISECONDS.sleep(200);
      value = cache.get(key);
      isPresent = expectedValue.equals(value);
    }

    if (!isPresent) {
      throw new RuntimeException(
          "Timed out waiting for expected value '" + expectedValue + "' in cache");
    }
  }
}

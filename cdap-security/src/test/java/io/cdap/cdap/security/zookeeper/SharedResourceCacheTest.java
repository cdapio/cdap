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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.zookeeper.SettableOperationFuture;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests covering the {@link SharedResourceCache} implementation.
 */
public class SharedResourceCacheTest {

  private static final String ZK_NAMESPACE = "/SharedResourceCacheTest";
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
    zkClient1.startAsync().awaitRunning();
    SharedResourceCache<String> cache1 =
        new SharedResourceCache<>(zkClient1, new StringCodec(), parentNode, acls);
    cache1.init();

    // add items to one and wait for them to show up in the second
    String key1 = "key1";
    String value1 = "value1";
    cache1.put(key1, value1);

    ZKClientService zkClient2 = injector2.getInstance(ZKClientService.class);
    zkClient2.startAsync().awaitRunning();
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
  public void testInitAndPutRemoveErrorPropagationAndListenerExceptions() throws Exception {
    ZKClient failingInitZk = Mockito.mock(ZKClient.class);
    SettableOperationFuture<Stat> failedExists =
        SettableOperationFuture.create("/failInit", Threads.SAME_THREAD_EXECUTOR);
    IllegalStateException expectedInitCause = new IllegalStateException("zk exists failed");
    failedExists.setException(expectedInitCause);
    Mockito.when(failingInitZk.exists("/failInit")).thenReturn(failedExists);

    SharedResourceCache<String> failingInitCache = new SharedResourceCache<>(
        failingInitZk, new StringCodec(), "/failInit", ZooDefs.Ids.OPEN_ACL_UNSAFE);
    try {
      failingInitCache.init();
      fail("Expected IllegalStateException propagated from ExecutionException cause");
    } catch (RuntimeException caught) {
      assertSame(expectedInitCause, caught);
    }

    ZKClient mockZk = Mockito.mock(ZKClient.class);
    SettableOperationFuture<Stat> okExists =
        SettableOperationFuture.create("/unitCache", Threads.SAME_THREAD_EXECUTOR);
    okExists.set(new Stat());
    Mockito.when(mockZk.exists("/unitCache")).thenReturn(okExists);

    NodeChildren emptyChildren = Mockito.mock(NodeChildren.class);
    Mockito.when(emptyChildren.getChildren()).thenReturn(Collections.<String>emptyList());
    SettableOperationFuture<NodeChildren> childrenFuture =
        SettableOperationFuture.create("/unitCache", Threads.SAME_THREAD_EXECUTOR);
    childrenFuture.set(emptyChildren);
    Mockito.when(mockZk.getChildren(Mockito.eq("/unitCache"), Mockito.any(Watcher.class))).thenReturn(childrenFuture);

    SharedResourceCache<String> unitCache = new SharedResourceCache<>(
        mockZk, new StringCodec(), "/unitCache", ZooDefs.Ids.OPEN_ACL_UNSAFE);

    final CountDownLatch secondListenerUpdateLatch = new CountDownLatch(1);
    final CountDownLatch errorNotifiedLatch = new CountDownLatch(2);
    final AtomicBoolean listenerAfterFatalCalled = new AtomicBoolean(false);

    // First listener throws RuntimeException on onUpdate; ListenerManager must swallow it and still notify next.
    unitCache.addListener(new BaseResourceListener<String>() {
      @Override
      public void onUpdate() {
        throw new IllegalStateException("listener runtime error");
      }
    });
    unitCache.addListener(new BaseResourceListener<String>() {
      @Override
      public void onUpdate() {
        secondListenerUpdateLatch.countDown();
      }

      @Override
      public void onError(String name, Throwable throwable) {
        errorNotifiedLatch.countDown();
      }
    });

    unitCache.init();
    assertTrue(secondListenerUpdateLatch.await(5, TimeUnit.SECONDS));

    // Failing put() notifies onError and propagates exception.
    SettableOperationFuture<String> failedCreate =
        SettableOperationFuture.create("/unitCache/badKey", Threads.SAME_THREAD_EXECUTOR);
    IOException createIoError = new IOException("create failed");
    failedCreate.setException(createIoError);
    Mockito.when(mockZk.create(
        Mockito.eq("/unitCache/badKey"), Mockito.any(byte[].class),
        Mockito.eq(CreateMode.PERSISTENT), Mockito.eq(true), Mockito.anyList())).thenReturn(failedCreate);
    try {
      unitCache.put("badKey", "val");
      fail("Expected RuntimeException on put failure");
    } catch (RuntimeException expected) {
      assertTrue(expected.getCause() != null);
    }

    // Failing remove() notifies onError via callback.
    SettableOperationFuture<String> failedDelete =
        SettableOperationFuture.create("/unitCache/badKey", Threads.SAME_THREAD_EXECUTOR);
    failedDelete.setException(new IOException("delete failed"));
    Mockito.when(mockZk.delete("/unitCache/badKey")).thenReturn(failedDelete);
    unitCache.remove("badKey");

    assertTrue(errorNotifiedLatch.await(5, TimeUnit.SECONDS));

    // Listener throwing Error aborts the ListenerManager notification loop via propagateIfInstanceOf(t, Error.class).
    SharedResourceCache<String> fatalListenerCache = new SharedResourceCache<>(
        mockZk, new StringCodec(), "/unitCache", ZooDefs.Ids.OPEN_ACL_UNSAFE);
    final CountDownLatch fatalListenerEntered = new CountDownLatch(1);
    fatalListenerCache.addListener(new BaseResourceListener<String>() {
      @Override
      public void onUpdate() {
        fatalListenerEntered.countDown();
        throw new AssertionError("fatal listener error");
      }
    });
    fatalListenerCache.addListener(new BaseResourceListener<String>() {
      @Override
      public void onUpdate() {
        listenerAfterFatalCalled.set(true);
      }
    });
    fatalListenerCache.init();
    assertTrue(fatalListenerEntered.await(5, TimeUnit.SECONDS));
    TimeUnit.MILLISECONDS.sleep(50);
    assertFalse(listenerAfterFatalCalled.get());
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

    Stopwatch watch = Stopwatch.createUnstarted().start();
    while (!isPresent && watch.elapsed(TimeUnit.MILLISECONDS) < timeToWaitMillis) {
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

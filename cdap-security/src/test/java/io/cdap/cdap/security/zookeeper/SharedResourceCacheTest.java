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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
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
  private static final Logger LOG = LoggerFactory.getLogger(SharedResourceCacheTest.class);
  private static MiniZooKeeperCluster zkCluster;
  private static String zkConnectString;
  private static Injector injector1;
  private static Injector injector2;

  @BeforeClass
  public static void startUp() throws Exception {
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    zkCluster = testUtil.startMiniZKCluster();
    zkConnectString = testUtil.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM) + ":"
        + zkCluster.getClientPort();
    LOG.info("Running ZK cluster at " + zkConnectString);
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkConnectString);
    injector1 = Guice.createInjector(new ConfigModule(cConf, testUtil.getConfiguration()),
        new ZkClientModule());
    injector2 = Guice.createInjector(new ConfigModule(cConf, testUtil.getConfiguration()),
        new ZkClientModule());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    zkCluster.shutdown();
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

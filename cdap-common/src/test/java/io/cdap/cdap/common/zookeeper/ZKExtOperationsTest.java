/*
 * Copyright © 2014-2024 Cask Data, Inc.
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

package io.cdap.cdap.common.zookeeper;

import com.google.common.base.Function;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.io.Codec;
import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for testing helper methods in {@link ZKExtOperations}.
 */
public class ZKExtOperationsTest {

  private static final Codec<Integer> INT_CODEC = new Codec<Integer>() {
    @Override
    public byte[] encode(Integer object) {
      return Bytes.toBytes(object);
    }

    @Override
    public Integer decode(byte[] data) {
      return Bytes.toInt(data);
    }
  };

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static InMemoryZKServer zkServer;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAsync().awaitRunning();
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAsync().awaitTerminated();
  }

  @Test
  public void testGetAndSet() throws Exception {
    String path = "/testGetAndSet";
    ZKClientService zkClient1 = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    ZKClientService zkClient2 = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();

    zkClient1.startAsync().awaitRunning();
    zkClient2.startAsync().awaitRunning();

    ZKExtOperations.updateOrCreate(zkClient1, path, new Function<Integer, Integer>() {
      @Nullable
      @Override
      public Integer apply(@Nullable Integer input) {
        Assert.assertNull(input);
        return 0;
      }
    }, INT_CODEC).get(10, TimeUnit.SECONDS);

    ZKExtOperations.updateOrCreate(zkClient2, path, new Function<Integer, Integer>() {
      @Nullable
      @Override
      public Integer apply(@Nullable Integer input) {
        Assert.assertEquals(0, input.intValue());
        return 1;
      }
    }, INT_CODEC).get(10, TimeUnit.SECONDS);

    final CyclicBarrier barrier = new CyclicBarrier(2);
    Function<Integer, Integer> modifier = new Function<Integer, Integer>() {
      @Nullable
      @Override
      public Integer apply(@Nullable Integer input) {
        try {
          if (input == 1) {
            barrier.await();
            return 2;
          } else if (input == 2) {
            return 3;
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        throw new IllegalStateException("Illegal input " + input);
      }
    };
    Future<Integer> future1 = ZKExtOperations.updateOrCreate(zkClient1, path, modifier, INT_CODEC);
    Future<Integer> future2 = ZKExtOperations.updateOrCreate(zkClient2, path, modifier, INT_CODEC);

    int r1 = future1.get(10, TimeUnit.SECONDS);
    int r2 = future2.get(10, TimeUnit.SECONDS);

    Assert.assertTrue((r1 == 2 && r2 == 3) || (r1 == 3 && r2 == 2));

    Integer result = ZKExtOperations.updateOrCreate(zkClient1, path,
        new Function<Integer, Integer>() {
          @Nullable
          @Override
          public Integer apply(@Nullable Integer input) {
            return (input == 3) ? null : 4;
          }
        }, INT_CODEC).get();

    Assert.assertNull(result);

    zkClient1.stopAsync().awaitTerminated();
    zkClient2.stopAsync().awaitTerminated();
  }

  @Test
  public void testCreateOrSet() throws Exception {
    String path = "/parent/testCreateOrSet";
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAsync().awaitRunning();

    Assert.assertEquals(1, ZKExtOperations.createOrSet(zkClient, path,
        Suppliers.ofInstance(1), INT_CODEC, 0).get().intValue());
    Assert.assertEquals(1, INT_CODEC.decode(zkClient.getData(path).get().getData()).intValue());

    Assert.assertEquals(2, ZKExtOperations.createOrSet(zkClient, path,
        Suppliers.ofInstance(2), INT_CODEC, 0).get().intValue());
    Assert.assertEquals(2, INT_CODEC.decode(zkClient.getData(path).get().getData()).intValue());

    zkClient.stopAsync().awaitTerminated();
  }

  @Test
  public void testSetOrCreate() throws Exception {
    String path = "/parent/testSetOrCreate";
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAsync().awaitRunning();

    Assert.assertEquals(1, ZKExtOperations.setOrCreate(zkClient, path,
        Suppliers.ofInstance(1), INT_CODEC, 0).get().intValue());
    Assert.assertEquals(1, INT_CODEC.decode(zkClient.getData(path).get().getData()).intValue());

    Assert.assertEquals(2, ZKExtOperations.setOrCreate(zkClient, path,
        Suppliers.ofInstance(2), INT_CODEC, 0).get().intValue());
    Assert.assertEquals(2, INT_CODEC.decode(zkClient.getData(path).get().getData()).intValue());

    zkClient.stopAsync().awaitTerminated();
  }

  @Test
  public void testCreateOrSetRetryCallbackAndExhaustion() throws Exception {
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAsync().awaitRunning();
    try {
      AtomicInteger encodeAttempts = new AtomicInteger(0);
      Codec<Integer> flakyCodec = new Codec<Integer>() {
        @Override
        public byte[] encode(Integer object) throws IOException {
          if (encodeAttempts.incrementAndGet() == 1) {
            throw new IOException("transient encode failure");
          }
          return Bytes.toBytes(object);
        }

        @Override
        public Integer decode(byte[] data) {
          return Bytes.toInt(data);
        }
      };

      String retryPath = "/retrySucceeds";
      int stored = ZKExtOperations.createOrSet(
          zkClient, retryPath, Suppliers.ofInstance(42), flakyCodec, 2).get(5, TimeUnit.SECONDS);
      Assert.assertEquals(42, stored);
      Assert.assertEquals(2, encodeAttempts.get());

      Codec<Integer> alwaysFailingCodec = new Codec<Integer>() {
        @Override
        public byte[] encode(Integer object) throws IOException {
          throw new IOException("permanent encode failure");
        }

        @Override
        public Integer decode(byte[] data) {
          return Bytes.toInt(data);
        }
      };

      try {
        ZKExtOperations.createOrSet(
            zkClient, "/retry/fails", Suppliers.ofInstance(99), alwaysFailingCodec, 1)
            .get(5, TimeUnit.SECONDS);
        Assert.fail("Expected ExecutionException after retries exhausted");
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof IOException);
        Assert.assertEquals("permanent encode failure", e.getCause().getMessage());
      }
    } finally {
      zkClient.stopAsync().awaitTerminated();
    }
  }
}

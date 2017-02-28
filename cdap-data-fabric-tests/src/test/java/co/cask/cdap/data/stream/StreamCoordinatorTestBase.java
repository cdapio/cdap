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

import co.cask.cdap.api.data.stream.StreamProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class StreamCoordinatorTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(StreamCoordinatorTestBase.class);

  protected static CConfiguration cConf = CConfiguration.create();

  protected abstract StreamCoordinatorClient getStreamCoordinator();

  protected abstract StreamAdmin getStreamAdmin();

  protected static void setupNamespaces(NamespacedLocationFactory namespacedLocationFactory) throws IOException {
    // FileStreamAdmin expects namespace directory to exist.
    // Simulate namespace create
    namespacedLocationFactory.get(NamespaceId.DEFAULT).mkdirs();
  }

  @Test
  public void testGeneration() throws Exception {
    final StreamAdmin streamAdmin = getStreamAdmin();
    final String streamName = "testGen";
    final StreamId streamId = NamespaceId.DEFAULT.stream(streamName);
    streamAdmin.create(streamId);

    StreamCoordinatorClient coordinator = getStreamCoordinator();
    final CountDownLatch genIdChanged = new CountDownLatch(1);
    coordinator.addListener(streamId, new StreamPropertyListener() {
      @Override
      public void generationChanged(StreamId streamId, int generation) {
        if (generation == 10) {
          genIdChanged.countDown();
        }
      }
    });

    // Do concurrent calls to nextGeneration using two threads
    final CyclicBarrier barrier = new CyclicBarrier(2);
    for (int i = 0; i < 2; i++) {
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            barrier.await();
            for (int i = 0; i < 5; i++) {
              streamAdmin.truncate(streamId);
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };
      t.start();
    }

    Assert.assertTrue(genIdChanged.await(10, TimeUnit.SECONDS));
  }

  @Test
  public void testConfig() throws Exception {
    final StreamAdmin streamAdmin = getStreamAdmin();
    final String streamName = "testConfig";
    final StreamId streamId = NamespaceId.DEFAULT.stream(streamName);
    streamAdmin.create(streamId);


    StreamCoordinatorClient coordinator = getStreamCoordinator();
    final BlockingDeque<Integer> thresholds = new LinkedBlockingDeque<>();
    final BlockingDeque<Long> ttls = new LinkedBlockingDeque<>();
    coordinator.addListener(streamId, new StreamPropertyListener() {
      @Override
      public void thresholdChanged(StreamId streamId, int threshold) {
        thresholds.add(threshold);
      }

      @Override
      public void ttlChanged(StreamId streamId, long ttl) {
        ttls.add(ttl);
      }
    });

    // Have two threads, one update the threshold, one update the ttl
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final CountDownLatch completeLatch = new CountDownLatch(2);
    for (int i = 0; i < 2; i++) {
      final int threadId = i;
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            barrier.await();
            for (int i = 0; i < 100; i++) {
              StreamProperties.Builder builder = StreamProperties.builder();
              if (threadId == 0) {
                builder.setTTL(i);
              }
              if (threadId == 1) {
                builder.setNotificatonThreshold(i);
              }
              streamAdmin.updateConfig(streamId, builder.build());
            }
            completeLatch.countDown();
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };

      t.start();
    }

    Assert.assertTrue(completeLatch.await(60, TimeUnit.SECONDS));

    // Check the last threshold and ttl are correct. We don't check if the listener gets every update as it's
    // possible that it doesn't see every updates, but only the latest value (that's what ZK watch guarantees).
    Assert.assertTrue(validateLastElement(thresholds, 99));
    Assert.assertTrue(validateLastElement(ttls, 99L));

    // Verify the config is right
    StreamConfig config = streamAdmin.getConfig(streamId);
    Assert.assertEquals(99, config.getNotificationThresholdMB());
    Assert.assertEquals(99000L, config.getTTL());
  }

  @Test
  public void testDeleteStream() throws Exception {
    final StreamId streamId = NamespaceId.DEFAULT.stream("test");

    StreamAdmin streamAdmin = getStreamAdmin();
    streamAdmin.create(streamId);

    Assert.assertTrue(streamAdmin.exists(streamId));

    StreamCoordinatorClient streamCoordinator = getStreamCoordinator();

    final CountDownLatch latch = new CountDownLatch(1);
    streamCoordinator.addListener(streamId, new StreamPropertyListener() {
      @Override
      public void deleted(StreamId id) {
        if (id.equals(streamId)) {
          latch.countDown();
        }
      }
    });

    streamAdmin.drop(streamId);
    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
  }

  private <T> boolean validateLastElement(BlockingDeque<T> deque, T value) throws InterruptedException {
    int count = 0;
    T peekValue = deque.peekLast();
    while (!value.equals(peekValue) && count++ < 20) {
      TimeUnit.MILLISECONDS.sleep(100);
      LOG.info("Expected {}, got {}", value, peekValue);
      peekValue = deque.peekLast();
    }

    return count < 20;
  }
}

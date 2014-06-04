/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.common.base.Throwables;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class StreamCoordinatorTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  protected abstract StreamCoordinator createStreamCoordinator();

  @Test
  public void testGeneration() throws ExecutionException, InterruptedException, IOException {
    final StreamCoordinator coordinator = createStreamCoordinator();

    final CountDownLatch genIdChanged = new CountDownLatch(1);
    coordinator.addListener("testGen", new StreamPropertyListener() {
      @Override
      public void generationChanged(String streamName, int generation) {
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
              coordinator.nextGeneration(createStreamConfig("testGen"), 0);
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };
      t.start();
    }

    Assert.assertTrue(genIdChanged.await(10, TimeUnit.SECONDS));

    coordinator.close();
  }

  private StreamConfig createStreamConfig(String stream) throws IOException {
    return new StreamConfig(stream, 3600000, 10000, Long.MAX_VALUE,
                            new LocalLocationFactory(tmpFolder.newFolder()).create(stream));
  }
}

package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.omid.queueproxy.QueueRunnable;
import com.continuuity.data.operation.executor.omid.queueproxy.QueueStateProxy;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class QueueStateProxyTest {

  @Test
  public void testSerializedCalls() throws Exception {
    // Two async calls with same consumer should be run serially
    ListeningExecutorService listeningExecutorService =
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));

    final QueueStateProxy queueStateProxy = new QueueStateProxy(1024 * 1024);

    final CountDownLatch future1Latch = new CountDownLatch(1);
    final CountDownLatch mainThreadLatch = new CountDownLatch(1);

    final byte[] queueName = "queue1".getBytes();
    final QueueConsumer consumer = new QueueConsumer(0, 0, 1,
                                                     new QueueConfig(QueuePartitioner.PartitionerType.FIFO,
                                                                     true, 5, true));
    final AtomicInteger data = new AtomicInteger(0);

    ListenableFuture future1 = listeningExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          queueStateProxy.run(queueName, consumer,
                              new QueueRunnable() {
                                @Override
                                public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                                  try {
                                    mainThreadLatch.countDown();
                                    // Wait till others are started
                                    future1Latch.await();
                                    // This is the first call so state should be UNINITIALIZED
                                    Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                        statefulQueueConsumer.getStateType());
                                    assertConsumerEquals(consumer, statefulQueueConsumer);
                                    data.incrementAndGet();
                                    Assert.assertEquals(1, data.get());
                                    statefulQueueConsumer.setStateType(QueueConsumer.StateType.INITIALIZED);
                                  } catch (Exception e) {
                                    throw Throwables.propagate(e);
                                  }
                                }
                              });
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });

    // Wait till future1 executes
    mainThreadLatch.await();

    ListenableFuture future2 = listeningExecutorService.submit(
      new Runnable() {
        @Override
        public void run() {
          try {
            queueStateProxy.run(queueName, consumer,
                                new QueueRunnable() {
                                  @Override
                                  public void run(StatefulQueueConsumer statefulQueueConsumer)
                                    throws OperationException {
                                    try {
                                      // This is the second call so state should be INITIALIZED
                                      Assert.assertEquals(QueueConsumer.StateType.INITIALIZED,
                                                          statefulQueueConsumer.getStateType());
                                      assertConsumerEquals(consumer, statefulQueueConsumer);
                                      data.incrementAndGet();
                                      Assert.assertEquals(2, data.get());
                                    } catch (Exception e) {
                                      throw Throwables.propagate(e);
                                    }
                                  }
                                });
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }
    );

    // First operation should have started by now, but waiting on latch1
    // Second operation should not start till first one completes
    // Start the first operation
    future1Latch.countDown();

    // Wait for the tasks to complete
    future1.get();
    future2.get();

    listeningExecutorService.shutdown();
  }

  @Test
  public void testUpdatedQueueConsumer() throws Exception {
    // A new StatefulQueueConsumer that is created in the QueueOperation should replace the old one
    ListeningExecutorService listeningExecutorService =
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));

    final QueueStateProxy queueStateProxy = new QueueStateProxy(1024 * 1024);

    final CountDownLatch future1Latch = new CountDownLatch(1);
    final CountDownLatch mainThreadLatch = new CountDownLatch(1);

    final byte[] queueName = "queue1".getBytes();
    final QueueConsumer consumer = new QueueConsumer(0, 0, 1,
                                                     new QueueConfig(QueuePartitioner.PartitionerType.FIFO,
                                                                     true, 5, true));
    final AtomicInteger data = new AtomicInteger(0);

    ListenableFuture future1 = listeningExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          queueStateProxy.run(queueName, consumer,
                              new QueueRunnable() {
                                @Override
                                public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                                  try {
                                    mainThreadLatch.countDown();
                                    // Wait till others are started
                                    future1Latch.await();
                                    // This is the first call so state should be UNINITIALIZED
                                    Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                        statefulQueueConsumer.getStateType());
                                    assertConsumerEquals(consumer, statefulQueueConsumer);
                                    data.incrementAndGet();
                                    Assert.assertEquals(1, data.get());
                                    // Create new QueueConsumer
                                    StatefulQueueConsumer newConsumer =
                                      new StatefulQueueConsumer(statefulQueueConsumer.getInstanceId(),
                                                                statefulQueueConsumer.getGroupId(),
                                                                statefulQueueConsumer.getGroupSize(),
                                                                statefulQueueConsumer.getGroupName(),
                                                                statefulQueueConsumer.getPartitioningKey(),
                                                                statefulQueueConsumer.getQueueConfig());
                                    newConsumer.setStateType(QueueConsumer.StateType.INITIALIZED);
                                    updateStatefulConsumer(newConsumer);
                                  } catch (Exception e) {
                                    throw Throwables.propagate(e);
                                  }
                                }
                              });
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });

    // Wait till future1 executes
    mainThreadLatch.await();

    ListenableFuture future2 = listeningExecutorService.submit(
      new Runnable() {
        @Override
        public void run() {
          try {
            queueStateProxy.run(queueName, consumer,
                                new QueueRunnable() {
                                  @Override
                                  public void run(StatefulQueueConsumer statefulQueueConsumer)
                                    throws OperationException {
                                    try {
                                      // This is the second call so state should be INITIALIZED
                                      Assert.assertEquals(QueueConsumer.StateType.INITIALIZED,
                                                          statefulQueueConsumer.getStateType());
                                      assertConsumerEquals(consumer, statefulQueueConsumer);
                                      data.incrementAndGet();
                                      Assert.assertEquals(2, data.get());
                                    } catch (Exception e) {
                                      throw Throwables.propagate(e);
                                    }
                                  }
                                });
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }
    );

    // First operation should have started by now, but waiting on latch1
    // Second operation should not start till first one completes
    // Start the first operation
    future1Latch.countDown();

    // Wait for the tasks to complete
    future1.get();
    future2.get();

    listeningExecutorService.shutdown();
  }

  @Test
  public void testMultiConsumerCalls() throws Exception {
    // Multi consumer calls can run asynchronously
    ListeningExecutorService listeningExecutorService =
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));

    final QueueStateProxy queueStateProxy = new QueueStateProxy(1024 * 1024);

    final CountDownLatch future1Latch = new CountDownLatch(1);

    final byte[] queueName = "queue1".getBytes();
    final QueueConsumer consumer1 = new QueueConsumer(0, 0, 2,
                                                     new QueueConfig(QueuePartitioner.PartitionerType.FIFO,
                                                                     true, 5, true));
    final QueueConsumer consumer2 = new QueueConsumer(1, 0, 2,
                                                      new QueueConfig(QueuePartitioner.PartitionerType.FIFO,
                                                                      true, 5, true));
    final AtomicInteger data = new AtomicInteger(0);

    ListenableFuture future1 = listeningExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          queueStateProxy.run(queueName, consumer1,
                              new QueueRunnable() {
                                @Override
                                public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                                  try {
                                    // Wait till others are started
                                    future1Latch.await();
                                    // This is the first call for consumer1 so state should be UNINITIALIZED
                                    Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                        statefulQueueConsumer.getStateType());
                                    assertConsumerEquals(consumer1, statefulQueueConsumer);
                                    data.incrementAndGet();
                                    Assert.assertEquals(2, data.get());
                                    statefulQueueConsumer.setStateType(QueueConsumer.StateType.INITIALIZED);
                                  } catch (Exception e) {
                                    throw Throwables.propagate(e);
                                  }
                                }
                              });
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });

    ListenableFuture future2 = listeningExecutorService.submit(
      new Runnable() {
        @Override
        public void run() {
          try {
            queueStateProxy.run(queueName, consumer2,
                                new QueueRunnable() {
                                  @Override
                                  public void run(StatefulQueueConsumer statefulQueueConsumer)
                                    throws OperationException {
                                    try {
                                      // This is the first for consumer2 call so state should be INITIALIZED
                                      Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                          statefulQueueConsumer.getStateType());
                                      assertConsumerEquals(consumer2, statefulQueueConsumer);
                                      data.incrementAndGet();
                                      Assert.assertEquals(1, data.get());
                                    } catch (Exception e) {
                                      throw Throwables.propagate(e);
                                    }
                                  }
                                });
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }
    );

    // Wait for the second operation to complete first
    future2.get();
    future1Latch.countDown();

    // Wait for the first operation
    future1.get();

    listeningExecutorService.shutdown();
  }

  @Test
  public void testEviction() throws Exception {
    // Eviction state should be handled properly
    // Create a proxy with size 0, so that entries are evicted after every call
    final QueueStateProxy queueStateProxy = new QueueStateProxy(0);

    final byte[] queueName = "queue1".getBytes();
    final QueueConsumer consumer = new QueueConsumer(0, 0, 1,
                                                     new QueueConfig(QueuePartitioner.PartitionerType.FIFO,
                                                                     true, 5, true));
    final AtomicInteger data = new AtomicInteger(0);

    queueStateProxy.run(queueName, consumer,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            try {
                              // This is the first call so state should be UNINITIALIZED
                              Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                  statefulQueueConsumer.getStateType());
                              assertConsumerEquals(consumer, statefulQueueConsumer);
                              data.incrementAndGet();
                              Assert.assertEquals(1, data.get());
                              statefulQueueConsumer.setStateType(QueueConsumer.StateType.INITIALIZED);
                            } catch (Exception e) {
                              throw Throwables.propagate(e);
                            }
                          }
                        });

    consumer.setStateType(QueueConsumer.StateType.INITIALIZED);

    queueStateProxy.run(queueName, consumer,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer)
                            throws OperationException {
                            try {
                              // This is the second call so state should be NOT_FOUND since all calls will be evicted
                              Assert.assertEquals(QueueConsumer.StateType.NOT_FOUND,
                                                  statefulQueueConsumer.getStateType());
                              assertConsumerEquals(consumer, statefulQueueConsumer);
                              data.incrementAndGet();
                              Assert.assertEquals(2, data.get());
                            } catch (Exception e) {
                              throw Throwables.propagate(e);
                            }
                          }
                        });

    // Real crash
    consumer.setStateType(QueueConsumer.StateType.UNINITIALIZED);

    queueStateProxy.run(queueName, consumer,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer)
                            throws OperationException {
                            try {
                              // This is the second call so state should be UNINITIALIZED since consumer has crashed
                              Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                  statefulQueueConsumer.getStateType());
                              assertConsumerEquals(consumer, statefulQueueConsumer);
                              data.incrementAndGet();
                              Assert.assertEquals(3, data.get());
                            } catch (Exception e) {
                              throw Throwables.propagate(e);
                            }
                          }
                        });
  }

  private void assertConsumerEquals(QueueConsumer expected, QueueConsumer actual) {
    Assert.assertEquals(expected.getInstanceId(), actual.getInstanceId());
    Assert.assertEquals(expected.getGroupId(), actual.getGroupId());
    Assert.assertEquals(expected.getGroupSize(), actual.getGroupSize());
    Assert.assertEquals(expected.getGroupName(), actual.getGroupName());
    Assert.assertEquals(expected.getPartitioningKey(), actual.getPartitioningKey());
    Assert.assertEquals(expected.getQueueConfig(), actual.getQueueConfig());
  }

  @Test
  public void testDeleteConsumerState() throws Exception {
    byte[] queueName = "q1".getBytes();

    // Create 2 consumers
    QueueConsumer consumer1 = new QueueConsumer(0, 0, 3,
                                                new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true, 10));
    QueueConsumer consumer2 = new QueueConsumer(1, 0, 3,
                                                new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true, 10));

    // Add them to proxy cache
    QueueStateProxy queueStateProxy = new QueueStateProxy(1024 * 1024);
    queueStateProxy.run(queueName, consumer1,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                statefulQueueConsumer.getStateType());
                            statefulQueueConsumer.setStateType(QueueConsumer.StateType.INITIALIZED);
                          }
                        });
    queueStateProxy.run(queueName, consumer2,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                statefulQueueConsumer.getStateType());
                            statefulQueueConsumer.setStateType(QueueConsumer.StateType.INITIALIZED);
                          }
                        });

    // Now we should have 2 consumers in cache
    queueStateProxy.run(queueName, consumer1,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.INITIALIZED,
                                                statefulQueueConsumer.getStateType());
                          }
                        });
    queueStateProxy.run(queueName, consumer2,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.INITIALIZED,
                                                statefulQueueConsumer.getStateType());
                          }
                        });

    // Delete consumer1
    queueStateProxy.deleteConsumerState(queueName, consumer1.getGroupId(), consumer1.getInstanceId());

    // Verify that only consumer1 got deleted
    queueStateProxy.run(queueName, consumer1,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.NOT_FOUND,
                                                statefulQueueConsumer.getStateType());
                          }
                        });
    queueStateProxy.run(queueName, consumer2,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.INITIALIZED,
                                                statefulQueueConsumer.getStateType());
                          }
                        });
  }

  @Test
  public void testDeleteGroupState() throws Exception {
    byte[] queueName = "q1".getBytes();

    // Create 2 consumer group, with 2 consumers each
    QueueConsumer consumer1G0 = new QueueConsumer(0, 0, 3,
                                                new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true, 10));
    QueueConsumer consumer2G0 = new QueueConsumer(1, 0, 3,
                                                new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true, 10));
    QueueConsumer consumer1G1 = new QueueConsumer(0, 1, 3,
                                                  new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true, 10));
    QueueConsumer consumer2G1 = new QueueConsumer(1, 1, 3,
                                                  new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true, 10));

    // Add them to proxy cache
    QueueStateProxy queueStateProxy = new QueueStateProxy(1024 * 1024);
    queueStateProxy.run(queueName, consumer1G0,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                statefulQueueConsumer.getStateType());
                            statefulQueueConsumer.setStateType(QueueConsumer.StateType.INITIALIZED);
                          }
                        });
    queueStateProxy.run(queueName, consumer2G0,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                statefulQueueConsumer.getStateType());
                            statefulQueueConsumer.setStateType(QueueConsumer.StateType.INITIALIZED);
                          }
                        });
    queueStateProxy.run(queueName, consumer1G1,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                statefulQueueConsumer.getStateType());
                            statefulQueueConsumer.setStateType(QueueConsumer.StateType.INITIALIZED);
                          }
                        });
    queueStateProxy.run(queueName, consumer2G1,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.UNINITIALIZED,
                                                statefulQueueConsumer.getStateType());
                            statefulQueueConsumer.setStateType(QueueConsumer.StateType.INITIALIZED);
                          }
                        });

    // Now we should have 2 consumers in cache
    queueStateProxy.run(queueName, consumer1G0,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.INITIALIZED,
                                                statefulQueueConsumer.getStateType());
                          }
                        });
    queueStateProxy.run(queueName, consumer2G0,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.INITIALIZED,
                                                statefulQueueConsumer.getStateType());
                          }
                        });
    queueStateProxy.run(queueName, consumer1G1,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.INITIALIZED,
                                                statefulQueueConsumer.getStateType());
                          }
                        });
    queueStateProxy.run(queueName, consumer2G1,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.INITIALIZED,
                                                statefulQueueConsumer.getStateType());
                          }
                        });

    // Delete group1
    queueStateProxy.deleteGroupState(queueName, consumer1G1.getGroupId());

    // Verify that only group1 got deleted
    queueStateProxy.run(queueName, consumer1G0,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.INITIALIZED,
                                                statefulQueueConsumer.getStateType());
                          }
                        });
    queueStateProxy.run(queueName, consumer2G0,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.INITIALIZED,
                                                statefulQueueConsumer.getStateType());
                          }
                        });
    queueStateProxy.run(queueName, consumer1G1,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.NOT_FOUND,
                                                statefulQueueConsumer.getStateType());
                          }
                        });
    queueStateProxy.run(queueName, consumer2G1,
                        new QueueRunnable() {
                          @Override
                          public void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
                            Assert.assertEquals(QueueConsumer.StateType.NOT_FOUND,
                                                statefulQueueConsumer.getStateType());
                          }
                        });
  }
}

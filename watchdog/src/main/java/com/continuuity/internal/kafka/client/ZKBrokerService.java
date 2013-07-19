/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.kafka.client;

import com.continuuity.kafka.client.BrokerInfo;
import com.continuuity.kafka.client.BrokerService;
import com.continuuity.kafka.client.TopicPartition;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.zookeeper.NodeChildren;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link BrokerService} that watches kafka zk nodes for updates of broker lists and leader for
 * each topic partition.
 */
final class ZKBrokerService extends AbstractIdleService implements BrokerService {

  private static final Logger LOG = LoggerFactory.getLogger(ZKBrokerService.class);
  private static final String BROKER_IDS_PATH = "/brokers/ids";
  private static final String BROKER_TOPICS_PATH = "/brokers/topics";
  private static final long FAILURE_RETRY_SECONDS = 5;
  private static final Gson GSON = new Gson();
  private static final Function<String, BrokerId> BROKER_ID_TRANSFORMER = new Function<String, BrokerId>() {
    @Override
    public BrokerId apply(String input) {
      return new BrokerId(Integer.parseInt(input));
    }
  };
  private static final Function<BrokerInfo, String> BROKER_INFO_TO_ADDRESS = new Function<BrokerInfo, String>() {
    @Override
    public String apply(BrokerInfo input) {
      return String.format("%s:%d", input.getHost(), input.getPort());
    }
  };

  private final ZKClient zkClient;
  private final LoadingCache<BrokerId, Supplier<BrokerInfo>> brokerInfos;
  private final LoadingCache<KeyPathTopicPartition, Supplier<PartitionInfo>> partitionInfos;
  // A comma separated list of brokers (host:port,host:port)

  private ExecutorService executorService;
  private Supplier<Iterable<BrokerInfo>> brokerList;

  ZKBrokerService(ZKClient zkClient) {
    this.zkClient = zkClient;
    this.brokerInfos = CacheBuilder.newBuilder().build(createCacheLoader(new CacheInvalidater<BrokerId>() {
      @Override
      public void invalidate(BrokerId key) {
        brokerInfos.invalidate(key);
      }
    }, BrokerInfo.class));
    this.partitionInfos = CacheBuilder.newBuilder().build(createCacheLoader(
      new CacheInvalidater<KeyPathTopicPartition>() {
      @Override
      public void invalidate(KeyPathTopicPartition key) {
        partitionInfos.invalidate(key);
      }
    }, PartitionInfo.class));
  }

  @Override
  protected void startUp() throws Exception {
    executorService = Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("zk-kafka-broker"));
  }

  @Override
  protected void shutDown() throws Exception {
    executorService.shutdownNow();
  }

  @Override
  public BrokerInfo getLeader(String topic, int partition) {
    Preconditions.checkState(isRunning(), "BrokerService is not running.");
    PartitionInfo partitionInfo = partitionInfos.getUnchecked(new KeyPathTopicPartition(topic, partition)).get();
    return partitionInfo == null ? null : brokerInfos.getUnchecked(new BrokerId(partitionInfo.getLeader())).get();
  }

  @Override
  public synchronized Iterable<BrokerInfo> getBrokers() {
    Preconditions.checkState(isRunning(), "BrokerService is not running.");

    if (brokerList != null) {
      return brokerList.get();
    }

    final SettableFuture<?> readerFuture = SettableFuture.create();
    final AtomicReference<Iterable<BrokerInfo>> brokers =
      new AtomicReference<Iterable<BrokerInfo>>(ImmutableList.<BrokerInfo>of());

    actOnExists(BROKER_IDS_PATH, new Runnable() {
      @Override
      public void run() {
        // Callback for fetching children list. This callback should be executed in the executorService.
        final FutureCallback<NodeChildren> childrenCallback = new FutureCallback<NodeChildren>() {
          @Override
          public void onSuccess(NodeChildren result) {
            try {
              // For each children node, get the BrokerInfo from the brokerInfo cache.
              brokers.set(
                ImmutableList.copyOf(
                  Iterables.transform(
                    brokerInfos.getAll(Iterables.transform(result.getChildren(), BROKER_ID_TRANSFORMER)).values(),
                    Suppliers.<BrokerInfo>supplierFunction())));
              readerFuture.set(null);

            } catch (ExecutionException e) {
              readerFuture.setException(e.getCause());
            }
          }

          @Override
          public void onFailure(Throwable t) {
            readerFuture.setException(t);
          }
        };

        // Fetch list of broker ids
        Futures.addCallback(zkClient.getChildren(BROKER_IDS_PATH, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if (!isRunning()) {
              return;
            }
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
              Futures.addCallback(zkClient.getChildren(BROKER_IDS_PATH, this), childrenCallback, executorService);
            }
          }
        }), childrenCallback, executorService);
      }
    }, readerFuture, FAILURE_RETRY_SECONDS, TimeUnit.SECONDS);

    brokerList = createSupplier(brokers);
    try {
      readerFuture.get();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return brokerList.get();
  }

  @Override
  public String getBrokerList() {
    return Joiner.on(',').join(Iterables.transform(getBrokers(), BROKER_INFO_TO_ADDRESS));
  }

  /**
   * Creates a cache loader for the given path to supply data with the data node.
   */
  private <K extends KeyPath, T> CacheLoader<K, Supplier<T>> createCacheLoader(final CacheInvalidater<K> invalidater,
                                                                               final Class<T> resultType) {
    return new CacheLoader<K, Supplier<T>>() {

      @Override
      public Supplier<T> load(final K key) throws Exception {
        // A future to tell if the result is ready, even it is failure.
        final SettableFuture<T> readyFuture = SettableFuture.create();
        final AtomicReference<T> resultValue = new AtomicReference<T>();

        // Fetch for node data when it exists.
        final String path = key.getPath();
        actOnExists(path, new Runnable() {
          @Override
          public void run() {
            // Callback for getData call
            final FutureCallback<NodeData> dataCallback = new FutureCallback<NodeData>() {
              @Override
              public void onSuccess(NodeData result) {
                // Update with latest data
                T value = decodeNodeData(result, resultType);
                resultValue.set(value);
                readyFuture.set(value);
              }

              @Override
              public void onFailure(Throwable t) {
                LOG.error("Failed to fetch node data on {}", path, t);
                if (t instanceof KeeperException.NoNodeException) {
                  resultValue.set(null);
                  readyFuture.set(null);
                  return;
                }

                // On error, simply invalidate the key so that it'll be fetched next time.
                invalidater.invalidate(key);
                readyFuture.setException(t);
              }
            };

            // Fetch node data
            Futures.addCallback(zkClient.getData(path, new Watcher() {
              @Override
              public void process(WatchedEvent event) {
                if (!isRunning()) {
                  return;
                }
                if (event.getType() == Event.EventType.NodeDataChanged) {
                  // If node data changed, fetch it again.
                  Futures.addCallback(zkClient.getData(path, this), dataCallback, executorService);
                } else if (event.getType() == Event.EventType.NodeDeleted) {
                  // If node removed, invalidate the cached value.
                  brokerInfos.invalidate(key);
                }
              }
            }), dataCallback, executorService);
          }
        }, readyFuture, FAILURE_RETRY_SECONDS, TimeUnit.SECONDS);

        readyFuture.get();
        return createSupplier(resultValue);
      }
    };
  }

  /**
   * Gson decode the NodeData into object.
   * @param nodeData The data to decode
   * @param type Object class to decode into.
   * @param <T> Type of the object.
   * @return The decoded object or {@code null} if node data is null.
   */
  private <T> T decodeNodeData(NodeData nodeData, Class<T> type) {
    byte[] data = nodeData == null ? null : nodeData.getData();
    if (data == null) {
      return null;
    }
    return GSON.fromJson(new String(data, Charsets.UTF_8), type);
  }

  /**
   * Checks exists of a given ZK path and execute the action when it exists.
   */
  private void actOnExists(final String path, final Runnable action,
                           final SettableFuture<?> readyFuture, final long retryTime, final TimeUnit retryUnit) {
    Futures.addCallback(zkClient.exists(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!isRunning()) {
          return;
        }
        if (event.getType() == Event.EventType.NodeCreated) {
          action.run();
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result != null) {
          action.run();
        } else {
          // If the node doesn't exists, treat it as ready. When the node becomes available later, data will be
          // fetched by the watcher.
          readyFuture.set(null);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // Retry the operation based on the retry time.
        Thread retryThread = new Thread("zk-broker-service-retry") {
          @Override
          public void run() {
            try {
              retryUnit.sleep(retryTime);
              actOnExists(path, action, readyFuture, retryTime, retryUnit);
            } catch (InterruptedException e) {
              LOG.warn("ZK retry thread interrupted. Action not retried.");
            }
          }
        };
        retryThread.setDaemon(true);
        retryThread.start();
      }
    }, executorService);
  }

  /**
   * Creates a supplier that always return latest copy from an {@link AtomicReference}.
   */
  private <T> Supplier<T> createSupplier(final AtomicReference<T> ref) {
    return new Supplier<T>() {
      @Override
      public T get() {
        return ref.get();
      }
    };
  }


  /**
   * Interface for invalidating an entry in a cache.
   * @param <T> Key type.
   */
  private interface CacheInvalidater<T> {
    void invalidate(T key);
  }

  /**
   * Represents a path in zookeeper for cache key.
   */
  private interface KeyPath {
    String getPath();
  }

  private static final class BrokerId implements KeyPath {
    private final int id;

    private BrokerId(int id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      return this == o || !(o == null || getClass() != o.getClass()) && id == ((BrokerId) o).id;
    }

    @Override
    public int hashCode() {
      return Ints.hashCode(id);
    }

    @Override
    public String getPath() {
      return BROKER_IDS_PATH + "/" + id;
    }
  }

  /**
   * Represents a topic + partition combination. Used for loading cache key.
   */
  private static final class KeyPathTopicPartition extends TopicPartition implements KeyPath {

    private KeyPathTopicPartition(String topic, int partition) {
      super(topic, partition);
    }

    @Override
    public String getPath() {
      return String.format("%s/%s/partitions/%d/state", BROKER_TOPICS_PATH, getTopic(), getPartition());
    }
  }

  /**
   * Class for holding information about a partition. Only used by gson to decode partition state node in zookeeper.
   */
  private static final class PartitionInfo {
    private int[] isr;
    private int leader;

    private int[] getIsr() {
      return isr;
    }

    private int getLeader() {
      return leader;
    }
  }
}

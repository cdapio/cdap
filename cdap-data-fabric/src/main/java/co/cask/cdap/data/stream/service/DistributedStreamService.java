/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.stream.notification.StreamSizeNotification;
import co.cask.cdap.common.zookeeper.coordination.BalancedAssignmentStrategy;
import co.cask.cdap.common.zookeeper.coordination.PartitionReplica;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinator;
import co.cask.cdap.common.zookeeper.coordination.ResourceCoordinatorClient;
import co.cask.cdap.common.zookeeper.coordination.ResourceHandler;
import co.cask.cdap.common.zookeeper.coordination.ResourceModifier;
import co.cask.cdap.common.zookeeper.coordination.ResourceRequirement;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamLeaderListener;
import co.cask.cdap.data.stream.StreamPropertyListener;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data.stream.service.heartbeat.HeartbeatPublisher;
import co.cask.cdap.data.stream.service.heartbeat.StreamWriterHeartbeat;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * Stream service running in a {@link TwillRunnable}. It is responsible for sending {@link StreamWriterHeartbeat}s
 * at a fixed rate, describing the sizes of the stream files on which this service writes data, for each stream.
 */
public class DistributedStreamService extends AbstractStreamService {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedStreamService.class);

  private static final String STREAMS_COORDINATOR = "streams.coordinator";

  private final ZKClient zkClient;
  private final StreamAdmin streamAdmin;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final StreamWriterSizeCollector streamWriterSizeCollector;
  private final HeartbeatPublisher heartbeatPublisher;
  private final StreamMetaStore streamMetaStore;
  private final ResourceCoordinatorClient resourceCoordinatorClient;
  private final NotificationService notificationService;
  private final NotificationFeedManager feedManager;
  private final Set<StreamLeaderListener> leaderListeners;
  private final int instanceId;

  private Cancellable leaderListenerCancellable;

  private final ConcurrentMap<Id.Stream, StreamSizeAggregator> aggregators;
  private Cancellable heartbeatsSubscription;

  private Supplier<Discoverable> discoverableSupplier;

  private LeaderElection leaderElection;
  private ResourceCoordinator resourceCoordinator;
  private Cancellable coordinationSubscription;

  @Inject
  public DistributedStreamService(CConfiguration cConf,
                                  StreamAdmin streamAdmin,
                                  StreamCoordinatorClient streamCoordinatorClient,
                                  StreamFileJanitorService janitorService,
                                  ZKClient zkClient,
                                  DiscoveryServiceClient discoveryServiceClient,
                                  StreamMetaStore streamMetaStore,
                                  Supplier<Discoverable> discoverableSupplier,
                                  StreamWriterSizeCollector streamWriterSizeCollector,
                                  HeartbeatPublisher heartbeatPublisher,
                                  NotificationFeedManager feedManager,
                                  NotificationService notificationService) {
    super(streamCoordinatorClient, janitorService, streamWriterSizeCollector);
    this.zkClient = zkClient;
    this.streamAdmin = streamAdmin;
    this.notificationService = notificationService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.streamMetaStore = streamMetaStore;
    this.discoverableSupplier = discoverableSupplier;
    this.feedManager = feedManager;
    this.streamWriterSizeCollector = streamWriterSizeCollector;
    this.heartbeatPublisher = heartbeatPublisher;
    this.resourceCoordinatorClient = new ResourceCoordinatorClient(getCoordinatorZKClient());
    this.leaderListeners = Sets.newHashSet();
    this.instanceId = cConf.getInt(Constants.Stream.CONTAINER_INSTANCE_ID);
    this.aggregators = Maps.newConcurrentMap();
  }

  @Override
  protected void initialize() throws Exception {
    createHeartbeatsFeed();
    heartbeatPublisher.startAndWait();
    resourceCoordinatorClient.startAndWait();
    coordinationSubscription = resourceCoordinatorClient.subscribe(discoverableSupplier.get().getName(),
                                                                   new StreamsLeaderHandler());

    heartbeatsSubscription = subscribeToHeartbeatsFeed();
    leaderListenerCancellable = addLeaderListener(new StreamLeaderListener() {
      @Override
      public void leaderOf(Set<Id.Stream> streamIds) {
        aggregate(streamIds);
      }
    });

    performLeaderElection();
  }

  @Override
  protected void doShutdown() throws Exception {
    for (StreamSizeAggregator aggregator : aggregators.values()) {
      aggregator.cancel();
    }

    if (leaderListenerCancellable != null) {
      leaderListenerCancellable.cancel();
    }

    if (heartbeatsSubscription != null) {
      heartbeatsSubscription.cancel();
    }

    heartbeatPublisher.stopAndWait();

    if (leaderElection != null) {
      Uninterruptibles.getUninterruptibly(leaderElection.stop(), 5, TimeUnit.SECONDS);
    }

    if (coordinationSubscription != null) {
      coordinationSubscription.cancel();
    }

    if (resourceCoordinatorClient != null) {
      resourceCoordinatorClient.stopAndWait();
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    LOG.trace("Performing heartbeat publishing in Stream service instance {}", instanceId);
    ImmutableMap.Builder<Id.Stream, Long> sizes = ImmutableMap.builder();
    String namespace = Constants.DEFAULT_NAMESPACE;
    //TODO: use listStreams() across all namespaces, not just default namespace.
    for (StreamSpecification streamSpec : streamMetaStore.listStreams(namespace)) {
      Id.Stream streamId = Id.Stream.from(namespace, streamSpec.getName());
      sizes.put(streamId, streamWriterSizeCollector.getTotalCollected(streamId));
    }
    StreamWriterHeartbeat heartbeat = new StreamWriterHeartbeat(System.currentTimeMillis(), instanceId, sizes.build());
    LOG.trace("Publishing heartbeat {}", heartbeat);
    heartbeatPublisher.sendHeartbeat(heartbeat);
  }

  /**
   * Perform aggregation on the Streams described by the {@code streamIds}, and no other Streams.
   * If aggregation was previously done on other Streams, those must be cancelled.
   *
   * @param streamIds Ids of the streams to perform data sizes aggregation on
   */
  private void aggregate(Set<Id.Stream> streamIds) {
    Set<Id.Stream> existingAggregators = Sets.newHashSet(aggregators.keySet());
    for (Id.Stream streamId : streamIds) {
      if (existingAggregators.remove(streamId)) {
        continue;
      }

      try {
        StreamConfig config = streamAdmin.getConfig(streamId);
        long filesSize = StreamUtils.fetchStreamFilesSize(config);
        LOG.debug("Size of the files already present for stream {}: {}", streamId, filesSize);
        createSizeAggregator(streamId, filesSize, config.getNotificationThresholdMB());
      } catch (IOException e) {
        LOG.error("Could not compute sizes of files for stream {}", streamId);
        Throwables.propagate(e);
      }
    }

    // Stop aggregating the heartbeats we used to listen to before the call to that method,
    // but don't anymore
    for (Id.Stream outdatedStream : existingAggregators) {
      // We need to first cancel the aggregator and then remove it from the map of aggregators,
      // to avoid race conditions in createSizeAggregator
      StreamSizeAggregator aggregator = aggregators.get(outdatedStream);
      if (aggregator != null) {
        aggregator.cancel();
      }
      aggregators.remove(outdatedStream);
    }
  }

  /**
   * Create a new aggregator for the {@code streamId}, and add it to the existing map of {@link Cancellable}
   * {@code aggregators}. This method does not cancel previously existing aggregator associated to the
   * {@code streamId}.
   *
   * @param streamId stream Id to create a new aggregator for
   * @param baseCount stream size from which to start aggregating
   * @return the created {@link StreamSizeAggregator}
   */
  private StreamSizeAggregator createSizeAggregator(Id.Stream streamId, long baseCount, int threshold) {
    LOG.debug("Creating size aggregator for stream {}", streamId);
    // Handle threshold changes
    final Cancellable thresholdSubscription =
      getStreamCoordinatorClient().addListener(streamId, new StreamPropertyListener() {
        @Override
        public void thresholdChanged(Id.Stream streamId, int threshold) {
          StreamSizeAggregator aggregator = aggregators.get(streamId);
          while (aggregator == null) {
            Thread.yield();
            aggregator = aggregators.get(streamId);
          }
          aggregator.setStreamThresholdMB(threshold);
        }
      });

    // Handle stream truncation, by creating creating a new empty aggregator for the stream
    // and cancelling the existing one
    final Cancellable truncationSubscription =
      getStreamCoordinatorClient().addListener(streamId, new StreamPropertyListener() {
        @Override
        public void generationChanged(Id.Stream streamId, int generation) {
          StreamSizeAggregator aggregator = aggregators.get(streamId);
          while (aggregator == null) {
            Thread.yield();
            aggregator = aggregators.get(streamId);
          }
          aggregator.resetCount();
        }
      });

    StreamSizeAggregator newAggregator = new StreamSizeAggregator(streamId, baseCount, threshold, new Cancellable() {
      @Override
      public void cancel() {
        thresholdSubscription.cancel();
        truncationSubscription.cancel();
      }
    });
    aggregators.put(streamId, newAggregator);
    return newAggregator;
  }

  private ZKClient getCoordinatorZKClient() {
    return ZKClients.namespace(zkClient, Constants.Stream.STREAM_ZK_COORDINATION_NAMESPACE);
  }

  /**
   * Subscribe to the streams heartbeat notification feed. One heartbeat contains data for all existing streams,
   * we filter that to only take into account the streams that this {@link DistributedStreamService} is a leader
   * of.
   *
   * @return a {@link Cancellable} to cancel the subscription
   * @throws NotificationFeedNotFoundException if the heartbeat feed does not exist
   * @throws NotificationFeedException in case of any other error concerning the feed
   */
  private Cancellable subscribeToHeartbeatsFeed() throws NotificationFeedNotFoundException, NotificationFeedException {
    final Id.NotificationFeed heartbeatsFeed = new Id.NotificationFeed.Builder()
      .setNamespaceId(Constants.DEFAULT_NAMESPACE)
      .setCategory(Constants.Notification.Stream.STREAM_INTERNAL_FEED_CATEGORY)
      .setName(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_NAME)
      .build();
    LOG.trace("Subscribing to stream heartbeats notification feed");
    return notificationService.subscribe(heartbeatsFeed, new NotificationHandler<StreamWriterHeartbeat>() {
      @Override
      public Type getNotificationFeedType() {
        return StreamWriterHeartbeat.class;
      }

      @Override
      public void received(StreamWriterHeartbeat heartbeat, NotificationContext notificationContext) {
        LOG.trace("Received heartbeat {}", heartbeat);
        for (Map.Entry<Id.Stream, Long> entry : heartbeat.getStreamsSizes().entrySet()) {
          StreamSizeAggregator streamSizeAggregator = aggregators.get(entry.getKey());
          if (streamSizeAggregator == null) {
            LOG.trace("Aggregator for stream {} is null", entry.getKey());
            continue;
          }
          streamSizeAggregator.bytesReceived(heartbeat.getInstanceId(), entry.getValue());
        }
      }
    });
  }

  /**
   * This method is called every time the Stream handler in which this {@link DistributedStreamService}
   * runs becomes the leader of a set of streams. Prior to this call, the Stream handler might
   * already have been the leader of some of those streams.
   *
   * @param listener {@link StreamLeaderListener} called when this Stream handler becomes leader
   *                 of a collection of streams
   * @return A {@link Cancellable} to cancel the watch
   */
  private Cancellable addLeaderListener(final StreamLeaderListener listener) {
    synchronized (this) {
      leaderListeners.add(listener);
    }
    return new Cancellable() {
      @Override
      public void cancel() {
        synchronized (DistributedStreamService.this) {
          leaderListeners.remove(listener);
        }
      }
    };
  }

  /**
   * Create Notification feed for stream's heartbeats, if it does not already exist.
   */
  private void createHeartbeatsFeed() throws NotificationFeedException {
    // TODO worry about namespaces here. Should we create one heartbeat feed per namespace?
    Id.NotificationFeed streamHeartbeatsFeed = new Id.NotificationFeed.Builder()
      .setNamespaceId(Constants.DEFAULT_NAMESPACE)
      .setCategory(Constants.Notification.Stream.STREAM_INTERNAL_FEED_CATEGORY)
      .setName(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_NAME)
      .setDescription("Stream heartbeats feed.")
      .build();

    try {
      feedManager.getFeed(streamHeartbeatsFeed);
    } catch (NotificationFeedNotFoundException e) {
      feedManager.createFeed(streamHeartbeatsFeed);
    }
  }

  /**
   * Elect one leader among the {@link DistributedStreamService}s running in different Twill runnables.
   */
  private void performLeaderElection() {
    // Start the resource coordinator that will map Streams to Stream handlers
    leaderElection = new LeaderElection(
      // TODO: Should unify this leader election with DistributedStreamFileJanitorService
      zkClient, "/election/" + STREAMS_COORDINATOR, new ElectionHandler() {
      @Override
      public void leader() {
        LOG.info("Became Stream handler leader. Starting resource coordinator.");
        resourceCoordinator = new ResourceCoordinator(getCoordinatorZKClient(), discoveryServiceClient,
                                                      new BalancedAssignmentStrategy());
        resourceCoordinator.startAndWait();
        updateRequirement();
      }

      @Override
      public void follower() {
        LOG.info("Became Stream handler follower.");
        if (resourceCoordinator != null) {
          resourceCoordinator.stopAndWait();
        }
      }
    });
    leaderElection.start();
  }

  /**
   * Updates stream resource requirement. It will retry if failed to do so.
   */
  private void updateRequirement() {
    final ResourceModifier modifier = createRequirementModifier();
    Futures.addCallback(resourceCoordinatorClient.modifyRequirement(Constants.Service.STREAMS, modifier),
                        new FutureCallback<ResourceRequirement>() {
      @Override
      public void onSuccess(ResourceRequirement result) {
        // No-op
        LOG.info("Stream resource requirement updated to {}", result);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.warn("Failed to update stream resource requirement.", t);
        if (isRunning()) {
          final FutureCallback<ResourceRequirement> callback = this;
          // Retry in 2 seconds. Shouldn't sleep in this callback thread. Should start a new thread for the retry.
          Thread retryThread = new Thread("stream-resource-update") {
            @Override
            public void run() {
              try {
                TimeUnit.SECONDS.sleep(2);
                LOG.info("Retrying update stream resource requirement");
                Futures.addCallback(resourceCoordinatorClient.modifyRequirement(Constants.Service.STREAMS, modifier),
                                    callback);
              } catch (InterruptedException e) {
                LOG.warn("Stream resource retry thread interrupted", e);
              }
            }
          };
          retryThread.setDaemon(true);
          retryThread.start();
        }
      }
    });
  }

  /**
   * Creates a {@link ResourceModifier} that updates stream resource requirement by consulting stream meta store.
   */
  private ResourceModifier createRequirementModifier() {
    return new ResourceModifier() {
      @Nullable
      @Override
      public ResourceRequirement apply(@Nullable ResourceRequirement existingRequirement) {
        try {
          // Create one requirement for the resource coordinator for all the streams.
          // One stream is identified by one partition
          ResourceRequirement.Builder builder = ResourceRequirement.builder(Constants.Service.STREAMS);
          for (StreamSpecification spec : streamMetaStore.listStreams(Constants.DEFAULT_NAMESPACE)) {
            LOG.debug("Adding {} stream as a resource to the coordinator to manager streams leaders.",
                      spec.getName());
            builder.addPartition(new ResourceRequirement.Partition(spec.getName(), 1));
          }
          return builder.build();
        } catch (Throwable e) {
          LOG.error("Could not create requirement for coordinator in Stream handler leader", e);
          throw Throwables.propagate(e);
        }
      }
    };
  }

  /**
   * Call all the listeners that are interested in knowing that this Stream writer is the leader of a set of Streams.
   *
   * @param streamIds set of Streams that this coordinator is the leader of
   */
  private void invokeLeaderListeners(Set<Id.Stream> streamIds) {
    LOG.debug("Stream writer is the leader of streams: {}", streamIds);
    Set<StreamLeaderListener> listeners;
    synchronized (this) {
      listeners = ImmutableSet.copyOf(leaderListeners);
    }
    for (StreamLeaderListener listener : listeners) {
      listener.leaderOf(streamIds);
    }
  }

  /**
   * Class that defines the behavior of a leader of a collection of Streams.
   */
  private final class StreamsLeaderHandler extends ResourceHandler {

    protected StreamsLeaderHandler() {
      super(discoverableSupplier.get());
    }

    @Override
    public void onChange(Collection<PartitionReplica> partitionReplicas) {
      LOG.info("Stream leader requirement has changed to {}", partitionReplicas);
      Set<Id.Stream> streamIds =
        ImmutableSet.copyOf(Iterables.transform(partitionReplicas, new Function<PartitionReplica, Id.Stream>() {
          @Nullable
          @Override
          public Id.Stream apply(@Nullable PartitionReplica input) {
            return input != null ? Id.Stream.fromId(input.getName()) : null;
          }
        }));
      invokeLeaderListeners(ImmutableSet.copyOf(streamIds));
    }

    @Override
    public void finished(Throwable failureCause) {
      if (failureCause != null) {
        LOG.error("Finished with failure for Stream handler instance {}", discoverableSupplier.get().getName(),
                  failureCause);
      }
    }
  }

  /**
   * Aggregate the sizes of all stream writers. A notification is published if the aggregated
   * size is higher than a threshold.
   */
  private final class StreamSizeAggregator implements Cancellable {

    private final Map<Integer, Long> streamWriterSizes;
    private final Id.NotificationFeed streamFeed;
    private final AtomicLong streamBaseCount;
    private final AtomicLong countFromFiles;
    private final AtomicInteger streamThresholdMB;
    private final Cancellable cancellable;
    private boolean isInit;

    protected StreamSizeAggregator(Id.Stream streamId, long baseCount, int streamThresholdMB, Cancellable cancellable) {
      this.streamWriterSizes = Maps.newHashMap();
      this.streamBaseCount = new AtomicLong(baseCount);
      this.countFromFiles = new AtomicLong(baseCount);
      this.streamThresholdMB = new AtomicInteger(streamThresholdMB);
      this.cancellable = cancellable;
      this.isInit = true;
      this.streamFeed = new Id.NotificationFeed.Builder()
        .setNamespaceId(streamId.getNamespaceId())
        .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
        .setName(streamId.getName())
        .build();
    }

    @Override
    public void cancel() {
      cancellable.cancel();
    }

    /**
     * Set the notification threshold for the stream that this {@link StreamSizeAggregator} is linked to.
     *
     * @param newThreshold new notification threshold, in megabytes
     */
    public void setStreamThresholdMB(int newThreshold) {
      streamThresholdMB.set(newThreshold);
    }

    /**
     * Reset the counts of this {@link StreamSizeAggregator} to zero.
     */
    public void resetCount() {
      streamBaseCount.set(0);
      countFromFiles.set(0);
    }

    /**
     * Notify this aggregator that a certain number of bytes have been received from the stream writer with instance
     * {@code instanceId}.
     *
     * @param instanceId id of the stream writer from which we received some bytes
     * @param nbBytes number of bytes of data received
     */
    public void bytesReceived(int instanceId, long nbBytes) {
      LOG.trace("Bytes received from instanceId {}: {}B", instanceId, nbBytes);
      streamWriterSizes.put(instanceId, nbBytes);
      checkSendNotification();
    }

    /**
     * Check if the current size of data is enough to trigger a notification.
     */
    private void checkSendNotification() {
      long sum = countFromFiles.get();
      for (Long size : streamWriterSizes.values()) {
        sum += size;
      }

      if (isInit || sum - streamBaseCount.get() > toBytes(streamThresholdMB.get())) {
        try {
          publishNotification(sum);
        } finally {
          streamBaseCount.set(sum);
          countFromFiles.set(0);
        }
      }
      isInit = false;
    }

    private long toBytes(int mb) {
      return ((long) mb) * 1024 * 1024;
    }

    private void publishNotification(long absoluteSize) {
      try {
        notificationService.publish(streamFeed, new StreamSizeNotification(System.currentTimeMillis(), absoluteSize))
          .get();
      } catch (NotificationFeedException e) {
        LOG.warn("Error with notification feed {}", streamFeed, e);
      } catch (Throwable t) {
        LOG.warn("Could not publish notification on feed {}", streamFeed.getId(), t);
      }
    }
  }
}

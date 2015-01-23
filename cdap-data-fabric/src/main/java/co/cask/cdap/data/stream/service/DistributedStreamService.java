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
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationException;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

  private final Map<String, StreamSizeAggregator> aggregators;
  private Cancellable heartbeatsSubscription;
  private boolean isInit;

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
    this.resourceCoordinatorClient = new ResourceCoordinatorClient(zkClient);
    this.leaderListeners = Sets.newHashSet();
    this.instanceId = cConf.getInt(Constants.Stream.CONTAINER_INSTANCE_ID);
    this.aggregators = Maps.newConcurrentMap();
    this.isInit = true;
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
      public void leaderOf(Set<String> streamNames) {
        aggregate(streamNames);
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
    ImmutableMap.Builder<String, Long> sizes = ImmutableMap.builder();
    for (StreamSpecification streamSpec : streamMetaStore.listStreams()) {
      sizes.put(streamSpec.getName(), streamWriterSizeCollector.getTotalCollected(streamSpec.getName()));
    }
    heartbeatPublisher.sendHeartbeat(new StreamWriterHeartbeat(System.currentTimeMillis(), instanceId, sizes.build()));
  }

  /**
   * Perform aggregation on the Streams described by the {@code streamNames}, and no other Streams.
   * If aggregation was previously done on other Streams, those must be cancelled.
   *
   * @param streamNames names of the streams to perform data sizes aggregation on
   */
  private void aggregate(Set<String> streamNames) {
    Set<String> existingAggregators = Sets.newHashSet(aggregators.keySet());
    for (String streamName : streamNames) {
      if (existingAggregators.remove(streamName)) {
        continue;
      }

      long filesSize = 0;
      try {
        StreamConfig config = streamAdmin.getConfig(streamName);
        filesSize = StreamUtils.fetchStreamFilesSize(config);
      } catch (IOException e) {
        LOG.error("Could not compute sizes of files for stream {}", streamName);
      }

      StreamSizeAggregator streamSizeAggregator = new StreamSizeAggregator(streamName, filesSize);
      aggregators.put(streamName, streamSizeAggregator);
    }

    // Stop aggregating the heartbeats we used to listen to before the call to that method,
    // but don't anymore
    for (String outdatedStream : existingAggregators) {
      StreamSizeAggregator aggregator = aggregators.remove(outdatedStream);
      aggregator.cancel();
    }
  }

  /**
   * Subscribe to the streams heartbeat notification feed. One heartbeat contains data for all existing streams,
   * we filter that to only take into account the streams that this {@link DistributedStreamService} is a leader
   * of.
   *
   * @return a {@link Cancellable} to cancel the subscription
   * @throws NotificationException if the heartbeat feed does not exist
   */
  private Cancellable subscribeToHeartbeatsFeed() throws NotificationException {
    final NotificationFeed heartbeatsFeed = new NotificationFeed.Builder()
      .setNamespace(Constants.DEFAULT_NAMESPACE)
      .setCategory(Constants.Notification.Stream.STREAM_INTERNAL_FEED_CATEGORY)
      .setName(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_NAME)
      .build();
    return notificationService.subscribe(heartbeatsFeed, new NotificationHandler<StreamWriterHeartbeat>() {
      @Override
      public Type getNotificationFeedType() {
        return StreamWriterHeartbeat.class;
      }

      @Override
      public void received(StreamWriterHeartbeat heartbeat, NotificationContext notificationContext) {
        for (Map.Entry<String, Long> entry : heartbeat.getStreamsSizes().entrySet()) {
          StreamSizeAggregator streamSizeAggregator = aggregators.get(entry.getKey());
          if (streamSizeAggregator == null) {
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
    NotificationFeed streamHeartbeatsFeed = new NotificationFeed.Builder()
      .setNamespace(Constants.DEFAULT_NAMESPACE)
      .setCategory(Constants.Notification.Stream.STREAM_INTERNAL_FEED_CATEGORY)
      .setName(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_NAME)
      .setDescription("Stream heartbeats feed.")
      .build();

    try {
      feedManager.getFeed(streamHeartbeatsFeed);
    } catch (NotificationFeedException e) {
      feedManager.createFeed(streamHeartbeatsFeed);
    }
  }

  /**
   * Elect one leader among the {@link DistributedStreamService}s running in different Twill runnables.
   */
  private void performLeaderElection() {
    // Start the resource coordinator that will map Streams to Stream handlers
    leaderElection = new LeaderElection(
      zkClient, "/election/" + STREAMS_COORDINATOR, new ElectionHandler() {
      @Override
      public void leader() {
        LOG.info("Became Stream handler leader. Starting resource coordinator.");
        resourceCoordinator = new ResourceCoordinator(zkClient, discoveryServiceClient,
                                                      new BalancedAssignmentStrategy());
        resourceCoordinator.startAndWait();

        resourceCoordinatorClient.modifyRequirement(Constants.Service.STREAMS, new ResourceModifier() {
          @Nullable
          @Override
          public ResourceRequirement apply(@Nullable ResourceRequirement existingRequirement) {
            try {
              // Create one requirement for the resource coordinator for all the streams.
              // One stream is identified by one partition
              ResourceRequirement.Builder builder = ResourceRequirement.builder(Constants.Service.STREAMS);
              for (StreamSpecification spec : streamMetaStore.listStreams()) {
                LOG.debug("Adding {} stream as a resource to the coordinator to manager streams leaders.",
                          spec.getName());
                builder.addPartition(new ResourceRequirement.Partition(spec.getName(), 1));
              }
              return builder.build();
            } catch (Throwable e) {
              LOG.error("Could not create requirement for coordinator in Stream handler leader", e);
              Throwables.propagate(e);
              return null;
            }
          }
        });
      }

      @Override
      public void follower() {
        LOG.info("Became Stream handler follower.");
        if (resourceCoordinator != null) {
          resourceCoordinator.stopAndWait();
        }
      }
    });
  }

  /**
   * Call all the listeners that are interested in knowing that this coordinator is the leader of a set of Streams.
   *
   * @param streamNames set of Streams that this coordinator is the leader of
   */
  private void invokeLeaderListeners(Set<String> streamNames) {
    Set<StreamLeaderListener> listeners;
    synchronized (this) {
      listeners = ImmutableSet.copyOf(leaderListeners);
    }
    for (StreamLeaderListener listener : listeners) {
      listener.leaderOf(streamNames);
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
      Set<String> streamNames =
        ImmutableSet.copyOf(Iterables.transform(partitionReplicas, new Function<PartitionReplica, String>() {
          @Nullable
          @Override
          public String apply(@Nullable PartitionReplica input) {
            return input != null ? input.getName() : null;
          }
        }));
      invokeLeaderListeners(ImmutableSet.copyOf(streamNames));
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
    private final NotificationFeed streamFeed;
    private final AtomicLong streamBaseCount;
    private final Cancellable truncationSubscription;

    protected StreamSizeAggregator(String streamName, long baseCount) {
      this.streamWriterSizes = Maps.newHashMap();
      this.streamBaseCount = new AtomicLong(baseCount);
      this.streamFeed = new NotificationFeed.Builder()
        .setNamespace(Constants.DEFAULT_NAMESPACE)
        .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
        .setName(streamName)
        .build();

      this.truncationSubscription = getStreamCoordinatorClient().addListener(streamName, new StreamPropertyListener() {
        @Override
        public void generationChanged(String streamName, int generation) {
          reset();
        }
      });
    }

    /**
     * Notify this aggregator that a certain number of bytes have been received from the stream writer with instance
     * {@code instanceId}.
     *
     * @param instanceId id of the stream writer from which we received some bytes
     * @param nbBytes number of bytes of data received
     */
    public void bytesReceived(int instanceId, long nbBytes) {
      Long lastSize = streamWriterSizes.get(instanceId);
      if (lastSize == null) {
        streamWriterSizes.put(instanceId, nbBytes);
        return;
      }
      streamWriterSizes.put(instanceId, lastSize + nbBytes);
      checkSendNotification();
    }

    @Override
    public void cancel() {
      truncationSubscription.cancel();
    }

    /**
     * Check if the current size of data is enough to trigger a notification.
     */
    private void checkSendNotification() {
      long sum = 0;
      for (Long size : streamWriterSizes.values()) {
        sum += size;
      }

      if (isInit || sum - streamBaseCount.get() > Constants.Notification.Stream.DEFAULT_DATA_THRESHOLD) {
        try {
          publishNotification(sum);
        } finally {
          streamBaseCount.set(sum);
        }
      }
    }

    private void reset() {
      streamWriterSizes.clear();
      streamBaseCount.set(0);
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

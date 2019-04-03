/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.messaging.distributed;

import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.messaging.MessageFetcher;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.RollbackDetail;
import co.cask.cdap.messaging.StoreRequest;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.server.MessagingHttpService;
import co.cask.cdap.messaging.service.CoreMessagingService;
import co.cask.cdap.messaging.store.ForwardingTableFactory;
import co.cask.cdap.messaging.store.TableFactory;
import co.cask.cdap.messaging.store.cache.MessageTableCacheProvider;
import co.cask.cdap.messaging.store.hbase.HBaseTableFactory;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicMarkableReference;
import javax.annotation.Nullable;

/**
 * A {@link MessagingService} that performs lead-election and only operates if it is currently a leader.
 */
public class LeaderElectionMessagingService extends AbstractIdleService implements MessagingService {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionMessagingService.class);

  private final Injector injector;
  private final CConfiguration cConf;
  private final MessageTableCacheProvider cacheProvider;
  private final ZKClient zkClient;
  private final AtomicMarkableReference<DelegateService> delegate;
  private boolean tableUpgraded;
  private LeaderElection leaderElection;
  private ScheduledExecutorService delayExecutor;

  @Inject
  LeaderElectionMessagingService(Injector injector, CConfiguration cConf,
                                 MessageTableCacheProvider cacheProvider, ZKClient zkClient) {
    this.injector = injector;
    this.cConf = cConf;
    this.cacheProvider = cacheProvider;
    this.zkClient = zkClient;
    this.delegate = new AtomicMarkableReference<>(null, false);
  }

  @Override
  protected void startUp() throws Exception {
    delayExecutor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("fencing-delay"));

    // Starts leader election
    final CountDownLatch latch = new CountDownLatch(1);
    leaderElection = new LeaderElection(zkClient, Constants.Service.MESSAGING_SERVICE, new ElectionHandler() {
      @Override
      public void leader() {
        if (!tableUpgraded) {
          upgradeTable();
          tableUpgraded = true;
        }

        final DelegateService delegateService = new DelegateService(injector.getInstance(CoreMessagingService.class),
                                                                    injector.getInstance(MessagingHttpService.class));
        updateDelegate(delegateService);
        LOG.info("Messaging service instance {} running at {} becomes leader",
                 cConf.get(Constants.MessagingSystem.CONTAINER_INSTANCE_ID),
                 cConf.get(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS));

        fencingStart(delegateService);
        latch.countDown();
      }

      @Override
      public void follower() {
        updateDelegate(null);
        LOG.info("Messaging service instance {} running at {} becomes follower",
                 cConf.get(Constants.MessagingSystem.CONTAINER_INSTANCE_ID),
                 cConf.get(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS));
        latch.countDown();
      }
    });
    leaderElection.startAndWait();
    latch.await();
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      leaderElection.stopAndWait();
    } catch (Exception e) {
      // It can happen if it is currently disconnected from ZK. There is no harm in just continue the shutdown process.
      LOG.warn("Exception during shutting down leader election", e);
    } finally {
      delayExecutor.shutdownNow();
    }
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata) throws TopicAlreadyExistsException, IOException {
    getMessagingService().createTopic(topicMetadata);
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException {
    getMessagingService().updateTopic(topicMetadata);
  }

  @Override
  public void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    getMessagingService().deleteTopic(topicId);
  }

  @Override
  public TopicMetadata getTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    return getMessagingService().getTopic(topicId);
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId) throws IOException {
    return getMessagingService().listTopics(namespaceId);
  }

  @Override
  public MessageFetcher prepareFetch(TopicId topicId) throws TopicNotFoundException, IOException {
    return getMessagingService().prepareFetch(topicId);
  }

  @Override
  @Nullable
  public RollbackDetail publish(StoreRequest request) throws TopicNotFoundException, IOException {
    return getMessagingService().publish(request);
  }

  @Override
  public void storePayload(StoreRequest request) throws TopicNotFoundException, IOException {
    getMessagingService().storePayload(request);
  }

  @Override
  public void rollback(TopicId topicId, RollbackDetail rollbackDetail) throws TopicNotFoundException, IOException {
    getMessagingService().rollback(topicId, rollbackDetail);
  }

  private void upgradeTable() {
    HBaseTableFactory tableFactory = getHBaseTableFactory(injector.getInstance(TableFactory.class));

    // Upgrade the TMS Message and Payload Tables
    if (tableFactory != null) {
      try {
        tableFactory.upgradeMessageTable(cConf.get(Constants.MessagingSystem.MESSAGE_TABLE_NAME));
      } catch (IOException ex) {
        LOG.warn("Exception while trying to upgrade TMS MessageTable.", ex);
      }

      try {
        tableFactory.upgradePayloadTable(cConf.get(Constants.MessagingSystem.PAYLOAD_TABLE_NAME));
      } catch (IOException ex) {
        LOG.warn("Exception while trying to upgrade TMS PayloadTable.", ex);
      }
    }
  }

  @Nullable
  private HBaseTableFactory getHBaseTableFactory(TableFactory tableFactory) {
    TableFactory factory = tableFactory;

    while (!(factory instanceof HBaseTableFactory) && factory instanceof ForwardingTableFactory) {
      factory = ((ForwardingTableFactory) factory).getDelegate();
    }

    return factory instanceof HBaseTableFactory ? (HBaseTableFactory) factory : null;
  }

  /**
   * Updates the delegate with the given {@link DelegateService} and stop the old one.
   * It also mark the delegate not usable.
   */
  private void updateDelegate(@Nullable DelegateService newService) {
    DelegateService oldService = delegate.getReference();
    while (!delegate.compareAndSet(oldService, newService, delegate.isMarked(), false)) {
      oldService = delegate.getReference();
    }

    if (oldService != null) {
      oldService.stopAndWait();
    }
  }

  private void fencingStart(final DelegateService service) {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        service.startAndWait();
        // If failed to mark the service to become available, this means the follower() call happened before this,
        // so just go ahead and shutdown the service.
        if (!delegate.attemptMark(service, true)) {
          service.stopAndWait();
        }
      }
    };

    long fencingDelaySeconds = cConf.getLong(Constants.MessagingSystem.HA_FENCING_DELAY_SECONDS);
    if (fencingDelaySeconds <= 0) {
      // No-fencing, so just start synchronously. Should only for unit-testing.
      runnable.run();
    } else {
      delayExecutor.schedule(runnable, fencingDelaySeconds, TimeUnit.SECONDS);
    }
  }

  private MessagingService getMessagingService() {
    DelegateService delegateService = delegate.getReference();
    if (delegateService == null || !delegate.isMarked()) {
      throw new ServiceUnavailableException(Constants.Service.MESSAGING_SERVICE,
                                            "Messaging service is temporarily unavailable due to leader transition");
    }
    return delegateService.getMessagingService();
  }

  /**
   * Private class to hold both {@link CoreMessagingService} and {@link MessagingHttpService} together.
   */
  private final class DelegateService extends AbstractIdleService {

    private final CoreMessagingService messagingService;
    private final MessagingHttpService httpService;

    DelegateService(CoreMessagingService messagingService, MessagingHttpService httpService) {
      this.messagingService = messagingService;
      this.httpService = httpService;
    }

    @Override
    protected void startUp() throws Exception {
      messagingService.startAndWait();
      httpService.startAndWait();
    }

    @Override
    protected void shutDown() throws Exception {
      try {
        httpService.stopAndWait();
        messagingService.stopAndWait();
      } finally {
        // Clear the table cache on shutting down.
        cacheProvider.clear();
      }
    }

    MessagingService getMessagingService() {
      return messagingService;
    }
  }
}

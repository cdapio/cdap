/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.messaging.distributed;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.guice.ZkClientModule;
import io.cdap.cdap.common.guice.ZkDiscoveryModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.messaging.store.cache.CachingTableFactory;
import io.cdap.cdap.messaging.store.cache.DefaultMessageTableCacheProvider;
import io.cdap.cdap.messaging.store.cache.MessageTableCacheProvider;
import io.cdap.cdap.messaging.store.leveldb.LevelDBTableFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Fast unit test for {@link LeaderElectionMessagingService} start/stop and leader-follower lifecycle.
 */
public class LeaderElectionMessagingServiceUnitTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testLeaderFollowerStartAndStopLifecycle() throws Exception {
    InMemoryZKServer zkServer = InMemoryZKServer.builder()
        .setDataDir(TEMP_FOLDER.newFolder())
        .build();
    zkServer.startAsync().awaitRunning();
    try {
      CConfiguration baseConf = CConfiguration.create();
      baseConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
      baseConf.setInt(Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS, 2000);
      baseConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
      baseConf.set(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS,
          InetAddress.getLocalHost().getHostName());
      baseConf.set(Constants.MessagingSystem.SYSTEM_TOPICS, "topic");
      baseConf.setLong(Constants.MessagingSystem.HA_FENCING_DELAY_SECONDS, 0L);

      NamespaceQueryAdmin namespaceQueryAdmin = new InMemoryNamespaceAdmin();
      LevelDBTableFactory levelDbFactory = new LevelDBTableFactory(baseConf);
      final AtomicBoolean cacheCleared = new AtomicBoolean(false);

      Injector leaderInjector = createInjector(baseConf, 0, namespaceQueryAdmin,
          levelDbFactory, cacheCleared);
      Injector followerInjector = createInjector(baseConf, 1, namespaceQueryAdmin,
          levelDbFactory, new AtomicBoolean(false));

      ZKClientService zkClient1 = leaderInjector.getInstance(ZKClientService.class);
      ZKClientService zkClient2 = followerInjector.getInstance(ZKClientService.class);
      zkClient1.startAsync().awaitRunning();
      zkClient2.startAsync().awaitRunning();
      try {
        MessagingService leader = leaderInjector.getInstance(MessagingService.class);
        MessagingService follower = followerInjector.getInstance(MessagingService.class);

        ((Service) leader).startAsync().awaitRunning();
        Assert.assertEquals(Service.State.RUNNING, ((Service) leader).state());
        ((Service) follower).startAsync().awaitRunning();
        Assert.assertEquals(Service.State.RUNNING, ((Service) follower).state());

        TopicId topicId = NamespaceId.SYSTEM.topic("topic");
        leader.publish(StoreRequestBuilder.of(topicId).addPayload("msg").build());

        try {
          follower.listTopics(NamespaceId.SYSTEM);
          Assert.fail("Follower must reject requests with ServiceUnavailableException");
        } catch (ServiceUnavailableException expected) {
          // expected
        }

        // Stopping leader exercises DelegateService.shutDown() -> httpService.stopAndWait(),
        // messagingService.stopAndWait(), cacheProvider.clear(), and leaderElection.stopAndWait()
        ((Service) leader).stopAsync().awaitTerminated();
        Assert.assertEquals(Service.State.TERMINATED, ((Service) leader).state());
        Assert.assertTrue(cacheCleared.get());
        ((Service) follower).stopAsync().awaitTerminated();
        Assert.assertEquals(Service.State.TERMINATED, ((Service) follower).state());
      } finally {
        zkClient2.stopAsync().awaitTerminated();
        zkClient1.stopAsync().awaitTerminated();
        levelDbFactory.close();
      }
    } finally {
      zkServer.stopAsync().awaitTerminated();
    }
  }

  private static Injector createInjector(CConfiguration baseConf, int instanceId,
      final NamespaceQueryAdmin namespaceQueryAdmin,
      final LevelDBTableFactory levelDbFactory,
      final AtomicBoolean cacheCleared) {
    CConfiguration cConf = CConfiguration.copy(baseConf);
    cConf.setInt(Constants.MessagingSystem.CONTAINER_INSTANCE_ID, instanceId);

    return Guice.createInjector(
        new ConfigModule(cConf),
        new ZkClientModule(),
        new ZkDiscoveryModule(),
        new AuthorizationEnforcementModule().getNoOpModules(),
        new DFSLocationModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
            bind(NamespaceQueryAdmin.class).toInstance(namespaceQueryAdmin);
          }
        },
        new PrivateModule() {
          @Override
          protected void configure() {
            bind(TableFactory.class)
                .annotatedWith(Names.named(CachingTableFactory.DELEGATE_TABLE_FACTORY))
                .toInstance(levelDbFactory);
            bind(DefaultMessageTableCacheProvider.class).in(Scopes.SINGLETON);
            bind(TableFactory.class).to(CachingTableFactory.class);
            MessagingServerRuntimeModule
                .bindHandlers(binder(), Constants.MessagingSystem.HANDLER_BINDING_NAME);
            bind(MessagingService.class).to(LeaderElectionMessagingService.class)
                .in(Scopes.SINGLETON);
            expose(MessagingService.class);
          }

          @Provides
          @Singleton
          MessageTableCacheProvider provideCacheProvider(
              final DefaultMessageTableCacheProvider delegateCacheProvider) {
            return new MessageTableCacheProvider() {
              @Override
              public io.cdap.cdap.messaging.cache.MessageCache<
                  io.cdap.cdap.messaging.store.MessageTable.Entry> getMessageCache(
                      TopicId topicId) {
                return delegateCacheProvider.getMessageCache(topicId);
              }

              @Override
              public void clear() {
                cacheCleared.set(true);
                delegateCacheProvider.clear();
              }
            };
          }
        },
        new NoOpAuditLogModule());
  }
}

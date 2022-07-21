/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.messaging.data.RawMessage;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.messaging.store.cache.CachingTableFactory;
import io.cdap.cdap.messaging.store.cache.DefaultMessageTableCacheProvider;
import io.cdap.cdap.messaging.store.cache.MessageTableCacheProvider;
import io.cdap.cdap.messaging.store.leveldb.LevelDBTableFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.internal.zookeeper.KillZKSession;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Unit test for {@link LeaderElectionMessagingService}.
 */
public class LeaderElectionMessagingServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static CConfiguration cConf;
  private static NamespaceQueryAdmin namespaceQueryAdmin;
  private static LevelDBTableFactory levelDBTableFactory;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TEMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    cConf.setInt(Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS, 2000);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS, InetAddress.getLocalHost().getHostName());
    cConf.set(Constants.MessagingSystem.SYSTEM_TOPICS, "topic");
    cConf.setLong(Constants.MessagingSystem.HA_FENCING_DELAY_SECONDS, 0L);

    namespaceQueryAdmin = new InMemoryNamespaceAdmin();
    levelDBTableFactory = new LevelDBTableFactory(cConf);
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAndWait();
  }

  @Test
  public void testTransition() throws Throwable {
    final TopicId topicId = NamespaceId.SYSTEM.topic("topic");

    Injector injector1 = createInjector(0);
    Injector injector2 = createInjector(1);

    // Start a messaging service, which would becomes leader
    ZKClientService zkClient1 = injector1.getInstance(ZKClientService.class);
    zkClient1.startAndWait();

    final MessagingService firstService = injector1.getInstance(MessagingService.class);
    if (firstService instanceof Service) {
      ((Service) firstService).startAndWait();
    }

    // Publish a message with the leader
    firstService.publish(StoreRequestBuilder.of(topicId).addPayload("Testing1").build());

    // Start another messaging service, this one would be follower
    ZKClientService zkClient2 = injector2.getInstance(ZKClientService.class);
    zkClient2.startAndWait();

    final MessagingService secondService = injector2.getInstance(MessagingService.class);
    if (secondService instanceof Service) {
      ((Service) secondService).startAndWait();
    }

    // Try to call the follower, should get service unavailable.
    try {
      secondService.listTopics(NamespaceId.SYSTEM);
      Assert.fail("Expected service unavailable");
    } catch (ServiceUnavailableException e) {
      // Expected
    }

    // Make the ZK session timeout for the leader service. The second one should pickup.
    KillZKSession.kill(zkClient1.getZooKeeperSupplier().get(), zkClient1.getConnectString(), 10000);

    // Publish one more message and then fetch from the current leader
    List<String> messages = Retries.callWithRetries(new Retries.Callable<List<String>, Throwable>() {
      @Override
      public List<String> call() throws Throwable {
        secondService.publish(StoreRequestBuilder.of(topicId).addPayload("Testing2").build());

        List<String> messages = new ArrayList<>();
        try (CloseableIterator<RawMessage> iterator = secondService.prepareFetch(topicId).fetch()) {
          while (iterator.hasNext()) {
            messages.add(new String(iterator.next().getPayload(), "UTF-8"));
          }
        }
        return messages;
      }
    }, RetryStrategies.timeLimit(10, TimeUnit.SECONDS, RetryStrategies.fixDelay(1, TimeUnit.SECONDS)));

    Assert.assertEquals(Arrays.asList("Testing1", "Testing2"), messages);

    // Shutdown the current leader. The session timeout one should becomes leader again.
    if (secondService instanceof Service) {
      ((Service) secondService).stopAndWait();
    }

    // Try to fetch message from the current leader again.
    // Should see two messages (because the cache is cleared and fetch is from the backing store).
    messages = Retries.callWithRetries(new Retries.Callable<List<String>, Throwable>() {
      @Override
      public List<String> call() throws Throwable {
        List<String> messages = new ArrayList<>();
        try (CloseableIterator<RawMessage> iterator = firstService.prepareFetch(topicId).fetch()) {
          while (iterator.hasNext()) {
            messages.add(new String(iterator.next().getPayload(), "UTF-8"));
          }
        }
        return messages;
      }
    }, RetryStrategies.timeLimit(10, TimeUnit.SECONDS, RetryStrategies.fixDelay(1, TimeUnit.SECONDS)));

    Assert.assertEquals(Arrays.asList("Testing1", "Testing2"), messages);

    zkClient1.stopAndWait();
    zkClient2.stopAndWait();
  }

  @Test
  public void testFencing()
    throws IOException, InterruptedException, ExecutionException, TimeoutException, UnauthorizedException {
    final TopicId topicId = NamespaceId.SYSTEM.topic("topic");

    // Change the fencing time
    long oldFencingDelay = cConf.getLong(Constants.MessagingSystem.HA_FENCING_DELAY_SECONDS);
    cConf.setLong(Constants.MessagingSystem.HA_FENCING_DELAY_SECONDS, 3L);

    try {
      Injector injector = createInjector(0);
      ZKClientService zkClient = injector.getInstance(ZKClientService.class);
      zkClient.startAndWait();

      final MessagingService messagingService = injector.getInstance(MessagingService.class);
      if (messagingService instanceof Service) {
        ((Service) messagingService).startAndWait();
      }

      // Shouldn't be serving request yet.
      try {
        messagingService.listTopics(NamespaceId.SYSTEM);
        Assert.fail("Expected service unavailable exception");
      } catch (ServiceUnavailableException e) {
        // expected
      }

      // Retry until pass the fencing delay (with some buffer)
      Tasks.waitFor(topicId, new Callable<TopicId>() {
        @Override
        public TopicId call() throws Exception {
          try {
            return messagingService.getTopic(topicId).getTopicId();
          } catch (ServiceUnavailableException e) {
            return null;
          }
        }
      }, 10L, TimeUnit.SECONDS, 200, TimeUnit.MILLISECONDS);

      if (messagingService instanceof Service) {
        ((Service) messagingService).stopAndWait();
      }
      zkClient.stopAndWait();

    } finally {
      cConf.setLong(Constants.MessagingSystem.HA_FENCING_DELAY_SECONDS, oldFencingDelay);
    }
  }


  private Injector createInjector(int instanceId) {
    CConfiguration cConf = CConfiguration.copy(LeaderElectionMessagingServiceTest.cConf);
    cConf.setInt(Constants.MessagingSystem.CONTAINER_INSTANCE_ID, instanceId);

    return Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new ZKDiscoveryModule(),
      new AuthorizationEnforcementModule().getNoOpModules(),
      new DFSLocationModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // Bindings to services for testing only
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);

          // Use the same in memory client across all injectors.
          bind(NamespaceQueryAdmin.class).toInstance(namespaceQueryAdmin);
        }
      },
      new PrivateModule() {
        @Override
        protected void configure() {
          // This is very similar to bindings in distributed mode, except we bind to level db instead of HBase
          // Also the level DB has to be one instance since unit-test runs in the same process.
          bind(TableFactory.class)
            .annotatedWith(Names.named(CachingTableFactory.DELEGATE_TABLE_FACTORY))
            .toInstance(levelDBTableFactory);

          // The cache must be in singleton scope
          bind(MessageTableCacheProvider.class).to(DefaultMessageTableCacheProvider.class).in(Scopes.SINGLETON);
          bind(TableFactory.class).to(CachingTableFactory.class);

          // Bind http handlers
          MessagingServerRuntimeModule.bindHandlers(binder(), Constants.MessagingSystem.HANDLER_BINDING_NAME);

          bind(MessagingService.class).to(LeaderElectionMessagingService.class).in(Scopes.SINGLETON);
          expose(MessagingService.class);
        }
      }
    );
  }
}

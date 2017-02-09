/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.common.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.KafkaConstants;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.kafka.client.ZKBrokerService;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ForwardingZKClientService;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Guice module for {@link KafkaClient} and {@link KafkaClientService}. Requires bindings from
 * {@link ConfigModule} and {@link ZKClientModule}.
 */
public class KafkaClientModule extends PrivateModule {

  private static final String KAFKA_ZK = "kafkaZK";

  @Override
  protected void configure() {
    bind(ZKClientService.class)
      .annotatedWith(Names.named(KAFKA_ZK))
      .toProvider(ZKClientServiceProvider.class).in(Scopes.SINGLETON);
    bind(KafkaClientService.class).to(DefaultKafkaClientService.class).in(Scopes.SINGLETON);
    bind(BrokerService.class).to(DefaultBrokerService.class).in(Scopes.SINGLETON);

    bind(KafkaClient.class).to(KafkaClientService.class);

    expose(KafkaClient.class);
    expose(KafkaClientService.class);
    expose(BrokerService.class);
  }

  /**
   * A {@link Provider} to provide {@link ZKClientService} used by
   * {@link ZKKafkaClientService} and {@link ZKBrokerService}.
   */
  private static final class ZKClientServiceProvider implements Provider<ZKClientService> {

    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    ZKClientServiceProvider(CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    @Override
    public ZKClientService get() {
      String kafkaZKQuorum = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_QUORUM);
      ZKClientService zkClientService;
      final AtomicInteger startedCount = new AtomicInteger();

      if (kafkaZKQuorum == null) {
        // If there is no separate zookeeper quorum, use the shared ZKClientService.
        zkClientService = injector.getInstance(ZKClientService.class);

        String kafkaNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
        if (kafkaNamespace != null) {
          if (!kafkaNamespace.startsWith("/")) {
            kafkaNamespace = "/" + kafkaNamespace;
          }
          zkClientService = ZKClientServices.delegate(ZKClients.namespace(zkClientService, kafkaNamespace));
        }

        // Since it is the shared ZKClientService, we don't want the KafkaClientService or BrokerService to
        // start/stop it, hence having the startedCount set to 1
        startedCount.set(1);
      } else {
        // Otherwise create a new ZKClientService
        zkClientService = ZKClientServices.delegate(
          ZKClients.reWatchOnExpire(
            ZKClients.retryOnFailure(
              ZKClientService.Builder.of(kafkaZKQuorum)
                .setSessionTimeout(cConf.getInt(Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                                                Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
                .build(),
              RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
            )
          )
        );
      }

      // Wrap the ZKClientService using simple reference counting for start/stop
      // The logic doesn't need to be sophisticated since it is a private binding and only used by the
      // wrapping KafkaClientService and BrokerService, which they will make sure no duplicate calls will be
      // made to the start/stop methods.
      return new ForwardingZKClientService(zkClientService) {
        @Override
        public ListenableFuture<State> start() {
          if (startedCount.getAndIncrement() == 0) {
            return super.start();
          }
          return Futures.immediateFuture(State.RUNNING);
        }

        @Override
        public ListenableFuture<State> stop() {
          if (startedCount.decrementAndGet() == 0) {
            return super.stop();
          }
          return Futures.immediateFuture(State.TERMINATED);
        }
      };
    }
  }

  /**
   * A {@link Service} wrapper that wraps a {@link Service} with a {@link ZKClientService}
   * that will get start/stop together.
   */
  private abstract static class AbstractServiceWithZKClient<T extends Service> extends AbstractIdleService {

    private final ZKClientService zkClientService;
    private final T delegate;

    AbstractServiceWithZKClient(ZKClientService zkClientService, T delegate) {
      this.zkClientService = zkClientService;
      this.delegate = delegate;
    }

    @Override
    protected final void startUp() throws Exception {
      zkClientService.startAndWait();
      try {
        delegate.startAndWait();
      } catch (Exception e) {
        try {
          zkClientService.stopAndWait();
        } catch (Exception se) {
          e.addSuppressed(se);
        }
        throw e;
      }
    }

    @Override
    protected final void shutDown() throws Exception {
      try {
        delegate.stopAndWait();
      } catch (Exception e) {
        try {
          zkClientService.stopAndWait();
        } catch (Exception se) {
          e.addSuppressed(se);
        }
        throw e;
      }
      zkClientService.stopAndWait();
    }

    protected T getDelegate() {
      return delegate;
    }
  }

  /**
   * A {@link KafkaClientService} that bundles with a given {@link ZKClientService} for start/stop.
   */
  private static final class DefaultKafkaClientService extends AbstractServiceWithZKClient<KafkaClientService>
                                                       implements KafkaClientService {

    @Inject
    DefaultKafkaClientService(@Named(KAFKA_ZK) ZKClientService zkClientService) {
      super(zkClientService, new ZKKafkaClientService(zkClientService));
    }

    @Override
    public KafkaPublisher getPublisher(KafkaPublisher.Ack ack, Compression compression) {
      return getDelegate().getPublisher(ack, compression);
    }

    @Override
    public KafkaConsumer getConsumer() {
      return getDelegate().getConsumer();
    }
  }

  /**
   * A {@link BrokerService} that bundles with a given {@link ZKClientService} for start/stop
   */
  private static final class DefaultBrokerService extends AbstractServiceWithZKClient<BrokerService>
                                                  implements BrokerService {

    @Inject
    DefaultBrokerService(@Named(KAFKA_ZK) ZKClientService zkClientService) {
      super(zkClientService, new ZKBrokerService(zkClientService));
    }

    @Override
    public BrokerInfo getLeader(String topic, int partition) {
      return getDelegate().getLeader(topic, partition);
    }

    @Override
    public Iterable<BrokerInfo> getBrokers() {
      return getDelegate().getBrokers();
    }

    @Override
    public String getBrokerList() {
      return getDelegate().getBrokerList();
    }

    @Override
    public Cancellable addChangeListener(BrokerChangeListener listener, Executor executor) {
      return getDelegate().addChangeListener(listener, executor);
    }
  }
}

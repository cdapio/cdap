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
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

import java.util.concurrent.TimeUnit;

/**
 * Guice module for {@link KafkaClient} and {@link KafkaClientService}. Requires bindings from
 * {@link ConfigModule} and {@link ZKClientModule}.
 */
public class KafkaClientModule extends AbstractModule {

  @Override
  protected void configure() {
    /**
     * Bindings to {@link KafkaClientService} is done by
     * {@link #providesKafkaClientService(CConfiguration, ZKClient)}
     */
    bind(KafkaClient.class).to(KafkaClientService.class);
  }

  @Provides
  @Singleton
  private KafkaClientService providesKafkaClientService(CConfiguration cConf, ZKClient zkClient) {
    if (cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_QUORUM) != null) {
      return new DefaultKafkaClientService(cConf);
    }
    String kafkaZKNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    return new ZKKafkaClientService(
      kafkaZKNamespace == null ? zkClient : ZKClients.namespace(zkClient, "/" + kafkaZKNamespace)
    );
  }

  /**
   * Wrapper around {@link KafkaClientService} with a {@link ZKClientService} connecting to the Kafka server
   * configured by {@code KafkaConstants.ConfigKeys.ZOOKEEPER_QUORUM}.
   */
  private static final class DefaultKafkaClientService extends AbstractIdleService implements KafkaClientService {

    private final ZKClientService zkClientService;
    private final KafkaClientService delegate;

    DefaultKafkaClientService(CConfiguration cConf) {
      zkClientService = ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_QUORUM))
              .setSessionTimeout(cConf.getInt(Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                                              Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
              .build(),
            RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
          )
        )
      );
      delegate = new ZKKafkaClientService(zkClientService);
    }

    @Override
    protected void startUp() throws Exception {
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
    protected void shutDown() throws Exception {
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

    @Override
    public KafkaPublisher getPublisher(KafkaPublisher.Ack ack, Compression compression) {
      return delegate.getPublisher(ack, compression);
    }

    @Override
    public KafkaConsumer getConsumer() {
      return delegate.getConsumer();
    }
  }
}

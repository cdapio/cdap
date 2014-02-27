package com.continuuity.gateway.run;

import com.continuuity.common.conf.CConfiguration;
import com.google.inject.Injector;
import junit.framework.Assert;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test {@link GatewayTwillRunnable}.
 */
public class GatewayTwillRunnableTest {

  @Test
  public void testInjection() throws Exception {
    ZKClientService zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of("127.0.0.1:2181").build(),
            RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
          )
        ));

    KafkaClientService kafkaClientService = new ZKKafkaClientService(zkClientService);

    Injector injector = GatewayTwillRunnable.createGuiceInjector(kafkaClientService, zkClientService,
                                                                 CConfiguration.create(), HBaseConfiguration.create());
    Assert.assertNotNull(injector);
  }
}

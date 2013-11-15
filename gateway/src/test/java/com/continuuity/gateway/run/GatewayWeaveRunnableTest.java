package com.continuuity.gateway.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.inject.Injector;
import junit.framework.Assert;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test GatewayWeaveRunnableTest.
 */
public class GatewayWeaveRunnableTest {

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

    Injector injector = GatewayWeaveRunnable.createGuiceInjector(kafkaClientService, zkClientService,
                                                                 CConfiguration.create(), HBaseConfiguration.create());
    Assert.assertNotNull(injector);
  }
}

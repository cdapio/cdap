/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.master.environment.k8s;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link MetricsServiceMain}.
 */
public class MetricsServiceMainTest extends MasterServiceMainTestBase {

  @Test
  public void testMetricsService() throws Exception {
    Injector injector = getServiceMainInstance(MetricsServiceMain.class).getInjector();

    // Publish some metrics via the MetricsCollectionService
    MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    MetricsContext context = metricsCollectionService.getContext(ImmutableMap.of(
      Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
      Constants.Metrics.Tag.APP, "test"
    ));

    context.increment("name", 10);

    // Discovery the location of metrics query service
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    Discoverable metricsEndpoint = new RandomEndpointStrategy(
      () -> discoveryServiceClient.discover(Constants.Service.METRICS)).pick(5, TimeUnit.SECONDS);

    Assert.assertNotNull(metricsEndpoint);

    // Try to query the metrics
    InetSocketAddress metricsAddr = metricsEndpoint.getSocketAddress();
    ConnectionConfig connConfig = ConnectionConfig.builder()
      .setHostname(metricsAddr.getHostName())
      .setPort(metricsAddr.getPort())
      .build();
    MetricsClient metricsClient = new MetricsClient(ClientConfig.builder().setConnectionConfig(connConfig).build());

    // Need to poll because metrics processing is async.
    Tasks.waitFor(10L, () -> {
      MetricQueryResult result = metricsClient.query(context.getTags(), "system.name");
      MetricQueryResult.TimeSeries[] series = result.getSeries();
      if (series.length == 0) {
        return 0L;
      }
      return series[0].getData()[0].getValue();

    }, 10, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
  }
}

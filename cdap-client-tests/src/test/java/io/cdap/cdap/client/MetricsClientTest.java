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

package io.cdap.cdap.client;

import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.proto.MetricQueryResult;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Fast unit tests for {@link MetricsClient}.
 */
public class MetricsClientTest {

  private static final ServiceId SERVICE_ID =
      NamespaceId.DEFAULT.app("myApp").service("myService");

  @Test
  public void testGetServiceMetricsReturnsCounterValue() {
    MetricQueryResult.TimeValue[] values =
        new MetricQueryResult.TimeValue[]{new MetricQueryResult.TimeValue(1L, 42L)};
    MetricQueryResult.TimeSeries[] series =
        new MetricQueryResult.TimeSeries[]{
            new MetricQueryResult.TimeSeries("metric", Collections.emptyMap(), values)
        };
    MetricQueryResult queryResult = new MetricQueryResult(0L, 10L, series, 1);

    MetricsClient client = createClientWithQueryBehavior((tags, metric) -> queryResult);
    Assert.assertEquals(42L, client.getServiceMetrics(SERVICE_ID).getInput());

    MetricQueryResult emptyResult =
        new MetricQueryResult(0L, 10L, new MetricQueryResult.TimeSeries[0], 1);
    MetricsClient emptyClient = createClientWithQueryBehavior((tags, metric) -> emptyResult);
    Assert.assertEquals(0L, emptyClient.getServiceMetrics(SERVICE_ID).getProcessed());
  }

  @Test
  public void testGetServiceMetricsWrapsCheckedExceptions() {
    IOException ioFailure = new IOException("metrics network error");
    MetricsClient ioClient = createClientWithQueryBehavior((tags, metric) -> {
      throw ioFailure;
    });
    try {
      ioClient.getServiceMetrics(SERVICE_ID).getInput();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException actual) {
      Assert.assertSame(ioFailure, actual.getCause());
    }

    UnauthenticatedException authFailure = new UnauthenticatedException("unauthenticated");
    MetricsClient authClient = createClientWithQueryBehavior((tags, metric) -> {
      throw authFailure;
    });
    try {
      authClient.getServiceMetrics(SERVICE_ID).getException();
      Assert.fail("Expected UnauthenticatedException directly");
    } catch (UnauthenticatedException actual) {
      Assert.assertSame(authFailure, actual);
    }
  }

  @Test
  public void testGetServiceMetricsPropagatesRuntimeExceptionDirectly() {
    IllegalStateException unchecked = new IllegalStateException("bad client state");
    MetricsClient client = createClientWithQueryBehavior((tags, metric) -> {
      throw unchecked;
    });
    try {
      client.getServiceMetrics(SERVICE_ID).getInput();
      Assert.fail("Expected IllegalStateException directly");
    } catch (IllegalStateException actual) {
      Assert.assertSame(unchecked, actual);
    }
  }

  private interface QueryBehavior {
    MetricQueryResult query(Map<String, String> tags, String metric)
        throws IOException, UnauthenticatedException;
  }

  private MetricsClient createClientWithQueryBehavior(QueryBehavior behavior) {
    ClientConfig clientConfig = ClientConfig.builder()
        .setConnectionConfig(new ConnectionConfig("localhost", 11015, false))
        .build();
    return new MetricsClient(clientConfig) {
      @Override
      public MetricQueryResult query(Map<String, String> tags, String metric)
          throws IOException, UnauthenticatedException {
        return behavior.query(tags, metric);
      }
    };
  }
}

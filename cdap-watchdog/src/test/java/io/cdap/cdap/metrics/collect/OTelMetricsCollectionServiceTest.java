/*
 * Copyright © 2014-2022 Cask Data, Inc.
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

package io.cdap.cdap.metrics.collect;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.common.conf.Constants;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class OTelMetricsCollectionServiceTest {
  private static final HashMap<String, String> EMPTY_TAGS = new HashMap<>();
  private static final String NAMESPACE = "testnamespace";
  private static final String APP = "testapp";
  private static final String SERVICE = "testservice";
  private static final String RUNID = "testrun";
  private static final String HANDLER = "testhandler";
  private static final String INSTANCE = "testInstance";
  private static final String METRIC = "metric";

  @Test
  public void testPublish() throws Exception {
    final BlockingQueue<MetricValues> published = new LinkedBlockingQueue<>();

    OTelMetricsCollectionService service = new OTelMetricsCollectionService();

    service.startAsync().awaitRunning();

    // non-empty tags.
    final Map<String, String> baseTags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, NAMESPACE,
            Constants.Metrics.Tag.APP, APP,
            Constants.Metrics.Tag.SERVICE, SERVICE,
            Constants.Metrics.Tag.RUN_ID, RUNID);

    try {
      // The first section tests with empty tags.
      // Publish couple metrics with empty tags, they should be aggregated.
      service.getContext(EMPTY_TAGS).increment(METRIC, Integer.MAX_VALUE);
      service.getContext(EMPTY_TAGS).increment(METRIC, 2);
      service.getContext(EMPTY_TAGS).increment(METRIC, 3);
      service.getContext(EMPTY_TAGS).increment(METRIC, 4);

    } finally {
      service.shutDown();
      service.stopAsync().awaitTerminated();
    }
  }
}

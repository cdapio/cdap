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

package co.cask.cdap.common.metrics;

import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricsSystemClient;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * A noop {@link MetricsSystemClient} implementation. It is intended for satisfying Guice dependency injection in
 * cases where actual operation is not needed (e.g. UpgradeTool).
 */
public class NoOpMetricsSystemClient implements MetricsSystemClient {

  @Override
  public void delete(MetricDeleteQuery deleteQuery) throws IOException {
    // no-op
  }

  @Override
  public Collection<MetricTimeSeries> query(int start, int end, int resolution, Map<String, String> tags,
                                            Collection<String> metrics, Collection<String> groupByTags) {
    return Collections.emptyList();
  }

  @Override
  public Collection<String> search(Map<String, String> tags) {
    return Collections.emptyList();
  }
}

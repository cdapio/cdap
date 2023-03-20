/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.metadata;

import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.metadata.MetadataConsumerContext;
import io.cdap.cdap.spi.metadata.MetadataConsumerMetrics;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides an initialized default context for MetadataConsumer implementing {@link MetadataConsumerContext}.
 */
public class DefaultMetadataConsumerContext implements MetadataConsumerContext {

  private final Map<String, String> properties;
  private final MetricsCollectionService metricsCollectionService;
  private final String namespace;
  private final String app;

  /**
   * Constructs a new instance of ${@link DefaultMetadataConsumerContext}.
   *
   * @param cConf An instance of an injected ${@link CConfiguration}.
   * @param metadataConsumerName name of the Metadata Consumer extension
   * @param metricsCollectionService the underlying service for metrics publishing
   * @param namespace namespace in which the program was run
   * @param app name of the app
   */
  DefaultMetadataConsumerContext(CConfiguration cConf, String metadataConsumerName,
                                 MetricsCollectionService metricsCollectionService,
                                 String namespace, String app) {
    String prefix = String.format("%s.%s.", Constants.MetadataConsumer.METADATA_CONSUMER_PREFIX, metadataConsumerName);
    this.properties = Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
    this.metricsCollectionService = metricsCollectionService;
    this.namespace = namespace;
    this.app = app;
  }

  @Override
  public Map<String, String> getProperties() {
    return this.properties;
  }

  @Override
  public MetadataConsumerMetrics getMetrics(Map<String, String> context) {
    Map<String, String> tags = new HashMap<>(context);
    tags.put(Constants.Metrics.Tag.NAMESPACE, this.namespace);
    tags.put(Constants.Metrics.Tag.APP, this.app);
    return new DefaultMetadataConsumerMetrics(metricsCollectionService.getContext(tags));
  }
}

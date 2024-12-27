/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.logging.publisher;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.logs.LogPublisherContext;
import java.util.Collections;
import java.util.Map;

/**
 * A context implementation for log publishers that provides configuration properties scoped to a
 * specific log publisher provider.
 */
public class DefaultLogPublisherContext implements LogPublisherContext {

  private final Map<String, String> properties;

  /**
   * Constructs a DefaultLogPublisherContext with configuration properties specific to the
   * provider.
   *
   * @param cConf        The configuration object containing the properties.
   * @param providerName The name of the log publisher.
   */
  public DefaultLogPublisherContext(CConfiguration cConf, String providerName) {
    String prefix = String.format("%s.%s.", Constants.Logging.LOG_PUBLISHER_PREFIX, providerName);
    this.properties = Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }
}

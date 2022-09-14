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

package io.cdap.cdap.metadata;

import io.cdap.cdap.spi.metadata.MetadataConsumer;
import io.cdap.cdap.spi.metadata.MetadataConsumerContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Context for a {@link MetadataConsumer} extension
 */
public class DefaultMetadataConsumerContext implements MetadataConsumerContext {

  private final Map<String, String> properties;

  DefaultMetadataConsumerContext(Map<String, String> properties) {
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }
}

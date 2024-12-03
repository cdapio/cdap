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
package io.cdap.cdap.messaging.client;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.spi.MessagingServiceContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation for {@link MessagingServiceContext}.
 */
public class DefaultMessagingServiceContext implements MessagingServiceContext {

  private final CConfiguration cConf;

  private static final String storageImpl = "gcp-spanner";

  DefaultMessagingServiceContext(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public Map<String, String> getProperties() {
    // TODO: cdap-tms module refactoring will remove this dependency on spanner.
    String spannerStoragePropertiesPrefix =
        Constants.Dataset.STORAGE_EXTENSION_PROPERTY_PREFIX + storageImpl + ".";
    String spannerMessagingPropertiesPrefix = Constants.MessagingSystem.SPANNER_EXTENSION_PROPERTY_PREFIX;
    Map<String, String> propertiesMap = new HashMap<>();
    propertiesMap.putAll(cConf.getPropsWithPrefix(spannerStoragePropertiesPrefix));
    propertiesMap.putAll(cConf.getPropsWithPrefix(spannerMessagingPropertiesPrefix));
    return Collections.unmodifiableMap(propertiesMap);
  }
}


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
import io.cdap.cdap.messaging.spi.MessagingContext;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMessagingContext implements MessagingContext {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMessagingContext.class);

  private final CConfiguration cConf;

  private static final String storageImpl = "gcp-spanner";

  DefaultMessagingContext(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public Map<String, String> getProperties() {
    String propertiesPrefix =
        Constants.Dataset.STORAGE_EXTENSION_PROPERTY_PREFIX + storageImpl + ".";
    LOG.info("Properties prefix {}", propertiesPrefix);
    return Collections.unmodifiableMap(cConf.getPropsWithPrefix(propertiesPrefix));
  }
}

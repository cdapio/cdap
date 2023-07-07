/*
 * Copyright © 2023 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.events;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.events.EventReaderContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides an initialized default context for EventReader implementing {@link EventReaderContext}.
 */
public class DefaultEventReaderContext implements EventReaderContext {

  private final Map<String, String> properties;
  private final static int DEFAULT_ACK_DEADLINE = 600;

  /**
   * Construct the default Event reader context.
   *
   * @param prefix prefix for specific event reader
   * @param cConf        An instance of an injected ${@link CConfiguration}.
   */
  DefaultEventReaderContext(String prefix, CConfiguration cConf) {
    Map<String, String> mutableProperties = new HashMap<>(cConf.getPropsWithPrefix(prefix));
    mutableProperties.put("ackDeadline", cConf.get(Constants.Event.EVENT_READER_ACK_DEADLINE,
            String.valueOf(DEFAULT_ACK_DEADLINE)));
    mutableProperties.put(Constants.INSTANCE_NAME, cConf.get(Constants.INSTANCE_NAME));
    this.properties = Collections.unmodifiableMap(mutableProperties);
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }
}

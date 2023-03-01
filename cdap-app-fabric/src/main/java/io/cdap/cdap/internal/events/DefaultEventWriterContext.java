/*
 * Copyright © 2022 Cask Data, Inc.
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
import io.cdap.cdap.spi.events.EventWriterContext;
import java.util.Collections;
import java.util.Map;

/**
 * Provides an initialized default context for EventWriter implementing {@link EventWriterContext}
 */
public class DefaultEventWriterContext implements EventWriterContext {

  private final Map<String, String> properties;

  /**
   * @param cConf An instance of an injected ${@link CConfiguration}.
   * @param eventsWriterId Id of the event writer extension. E.g.: pub_sub_event_writerA
   */
  DefaultEventWriterContext(CConfiguration cConf, String eventsWriterId) {
    String prefix = String.format("%s.%s.", Constants.Event.EVENTS_WRITER_PREFIX, eventsWriterId);
    this.properties = Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }
}

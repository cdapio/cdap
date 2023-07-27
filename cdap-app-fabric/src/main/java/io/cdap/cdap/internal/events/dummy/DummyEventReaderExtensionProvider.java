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

package io.cdap.cdap.internal.events.dummy;

import com.google.inject.Inject;
import io.cdap.cdap.internal.events.EventReaderProvider;
import io.cdap.cdap.spi.events.Event;
import io.cdap.cdap.spi.events.EventReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Dummy implementation for {@link EventReaderProvider} for test proposals.
 */
public class DummyEventReaderExtensionProvider<T extends Event> implements EventReaderProvider {

  private final DummyEventReader eventReader;

  @Inject
  public DummyEventReaderExtensionProvider(DummyEventReader eventReader) {
    this.eventReader = eventReader;
  }

  @Override
  public Map<String, EventReader> loadEventReaders() {
    Map<String, EventReader> map = new HashMap<>();
    map.put(this.eventReader.getClass().getName(), this.eventReader);
    return Collections.unmodifiableMap(map);
  }
}
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

package io.cdap.cdap.internal.events.dummy;

import com.google.inject.Inject;
import io.cdap.cdap.internal.events.EventWriterProvider;
import io.cdap.cdap.spi.events.EventWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Dummy implementation for {@link EventWriterProvider} for test proposals.
 */
public class DummyEventWriterExtensionProvider implements EventWriterProvider {

  private final DummyEventWriter eventWriter;

  @Inject
  public DummyEventWriterExtensionProvider(DummyEventWriter eventWriter) {
    this.eventWriter = eventWriter;
  }

  @Override
  public Map<String, EventWriter> loadEventWriters() {
    Map<String, EventWriter> map = new HashMap<>();
    map.put(this.eventWriter.getID(), this.eventWriter);
    return Collections.unmodifiableMap(map);
  }
}

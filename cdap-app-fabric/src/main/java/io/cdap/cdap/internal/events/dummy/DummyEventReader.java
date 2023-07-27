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

package io.cdap.cdap.internal.events.dummy;

import io.cdap.cdap.spi.events.Event;
import io.cdap.cdap.spi.events.EventReader;
import io.cdap.cdap.spi.events.EventReaderContext;
import io.cdap.cdap.spi.events.EventResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dummy implementation of {@link EventReader} mainly for test proposals.
 */
public class DummyEventReader<T extends Event> implements EventReader<T> {

  private static final Logger logger = LoggerFactory.getLogger(DummyEventReader.class);
  private final Collection<T> messages;

  public DummyEventReader(Collection<T> messages) {
    this.messages = messages;
  }

  @Override
  public String getId() {
    return "dummy-event-reader";
  }

  @Override
  public void initialize(EventReaderContext eventReaderContext) {
    logger.info("Initializing DummyEventReader...");
  }

  @Override
  public EventResult<T> pull(int maxMessages) {
    ArrayList<T> sentMessages = new ArrayList<>(messages);
    return new DummyEventResult(sentMessages.subList(0, maxMessages));
  }

  @Override
  public void close() throws Exception {
    logger.info("Closing dummy reader");
  }

  class DummyEventResult implements EventResult<T> {
    final Collection<T> events;

    DummyEventResult(Collection<T> events) {
      this.events = events;
    }

    @Override
    public void consumeEvents(Consumer<T> consumer) {
      for (T evt : events) {
        try {
          consumer.accept(evt);
        } catch (Exception e) {
          logger.error("Error on consuming event {}", evt.getVersion(), e);
        }
      }
    }

    @Override
    public void close() throws Exception {
    }
  }
}

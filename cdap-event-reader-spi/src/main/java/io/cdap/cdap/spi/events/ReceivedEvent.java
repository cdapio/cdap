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

package io.cdap.cdap.spi.events;

import javax.annotation.Nullable;
import java.util.Map;

/**
 */
public class ReceivedEvent {

  Event<?> event;
  EventType eventType;
  String ackId;

  @Nullable
  Map<String, String> attributes;

  public ReceivedEvent(String ackId, EventType eventType, Event<?> event) {
    this(ackId, eventType, event, null);
  }
  public ReceivedEvent(String ackId, EventType eventType, Event<?> event, Map<String, String> attributes) {
    this.ackId = ackId;
    this.eventType = eventType;
    this.event = event;
    this.attributes = attributes;
  }

  public EventType getType() {
    return eventType;
  }

  public Event<?> getEvent() {
    return event;
  }

  public String getAckId() {
    return ackId;
  }

  @Nullable
  public Map<String, String> getAttributes() {
    return attributes;
  }

}

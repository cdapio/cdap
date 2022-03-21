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

package io.cdap.cdap.internal.events;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.events.EventWriter;

import java.util.Map;
import java.util.Set;

/**
 * EventPublishManager is responsible for starting all the event publish manager threads
 */
public class EventPublishManager extends AbstractIdleService {

  private final boolean publishEnabled;
  private final Set<EventPublisher> eventPublishers;
  private final EventWriterProvider eventWriterProvider;

  @Inject
  EventPublishManager(CConfiguration cConf, Set<EventPublisher> eventPublishers,
                      EventWriterProvider eventWriterProvider) {
    this.publishEnabled = cConf.getBoolean(Constants.Event.PUBLISH_ENABLED);
    this.eventPublishers = eventPublishers;
    this.eventWriterProvider = eventWriterProvider;
  }

  @Override
  protected void startUp() throws Exception {
    if (!publishEnabled) {
      return; // If publish is not enabled not to start
    }
    eventPublishers.forEach(eventPublisher -> {
      // Loading the event writers from provider
      Map<String, EventWriter> eventWriterMap = this.eventWriterProvider.loadEventWriters();
      // Initialize the event publisher with all the event writers provided by provider
      eventPublisher.initialize(eventWriterMap.values());
      eventPublisher.startPublish();
    });
  }

  @Override
  protected void shutDown() throws Exception {
    if (!publishEnabled) {
      return; // If publish is not enable not to shutdown
    }
    eventPublishers.forEach(eventPublisher -> {
      eventPublisher.stopPublish();
    });
  }
}

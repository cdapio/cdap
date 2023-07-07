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

package io.cdap.cdap.internal.events;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.features.Feature;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EventSubscriberManager is responsible for starting all the event subscriber threads.
 */
public class EventSubscriberManager extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(EventSubscriberManager.class);

  private final boolean enabled;
  private final Set<EventSubscriber> eventSubscribers;

  @Inject
  EventSubscriberManager(CConfiguration cConf, Set<EventSubscriber> eventSubscribers) {
    this.enabled = Feature.EVENT_READER.isEnabled(new DefaultFeatureFlagsProvider(cConf));
    this.eventSubscribers = eventSubscribers;
  }

  @Override
  protected void startUp() throws Exception {
    if (!enabled) {
      return; // If not enabled, don't start
    }
    eventSubscribers.forEach(eventSubscriber -> {
      // Loading the event readers from provider
      // Initialize the event subscribers with all the event readers provided by provider
      if (eventSubscriber.initialize()) {
        eventSubscriber.startAndWait();
        LOG.info("Successfully initialized eventSubscriber: {}",
                eventSubscriber.getClass().getSimpleName());
      } else {
        LOG.error("Failed to initialize eventSubscriber: {}",
                eventSubscriber.getClass().getSimpleName());
      }
    });
  }

  @Override
  protected void shutDown() throws Exception {
    if (!enabled) {
      return; // If not enabled, don't shut down
    }
    eventSubscribers.forEach(eventSubscriber -> {
      eventSubscriber.stopAndWait();
    });
  }
}

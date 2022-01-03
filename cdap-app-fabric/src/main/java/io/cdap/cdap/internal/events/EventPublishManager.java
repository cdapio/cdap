/*
 * Copyright © 2021 Cask Data, Inc.
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
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;

import java.util.Set;

/**
 * EventPublishManager is responsible for starting all the event publish manager threads
 */
public class EventPublishManager extends AbstractIdleService {

  private boolean publishEnabled;
  private Set<EventPublisher> eventPublishers;

  @Inject
  EventPublishManager(CConfiguration cConf, Set<EventPublisher> eventPublishers) {
    this. publishEnabled = cConf.getBoolean(Constants.Event.PUBLISH_ENABLED);
    this.eventPublishers = eventPublishers;
  }

  @Override
  protected void startUp() throws Exception {
    if(!publishEnabled){
      return;
    }
    eventPublishers.forEach(eventPublisher -> {
      String eventWriterPath = eventPublisher.getEventWriterPath();
      //TODO - load classes from the path

    });
  }

  @Override
  protected void shutDown() throws Exception {
    eventPublishers.forEach(ep -> ep.stopPublish());
  }
}

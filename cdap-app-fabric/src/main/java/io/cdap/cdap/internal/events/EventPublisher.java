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

import io.cdap.cdap.spi.events.EventWriter;
import java.util.Collection;

/**
 * Interface for an event publisher
 */
public interface EventPublisher {

  /**
   * Initialize this publisher
   *
   * @param eventWriters {@link Collection} of {@link EventWriter}s to which events has to be
   *     published.
   */
  void initialize(Collection<EventWriter> eventWriters);

  /**
   * Start publish , to be called once init is complete
   */
  void startPublish();

  /**
   * Stop publish , when system is shutting down
   */
  void stopPublish();

  /**
   * Return ID of the Event Publisher.
   */
  String getID();
}

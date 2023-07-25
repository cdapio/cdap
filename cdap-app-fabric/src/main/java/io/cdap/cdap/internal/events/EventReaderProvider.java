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

import io.cdap.cdap.spi.events.Event;
import io.cdap.cdap.spi.events.EventReader;
import java.util.Map;

/**
 * This interface provides Event Readers ({@link EventReader}) in order to load them as an
 * extension.
 */
public interface EventReaderProvider<T extends Event> {

  /**
   * Method which retrieve a {@link Map} of {@link EventReader} and the Extension Type.
   */
  Map<String, EventReader<T>> loadEventReaders();
}

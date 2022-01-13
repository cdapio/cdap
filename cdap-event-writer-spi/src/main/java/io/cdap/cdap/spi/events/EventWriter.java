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

package io.cdap.cdap.spi.events;

import java.util.Collection;

/**
 * Interface for an {@link Event} writer.
 */
public interface EventWriter extends AutoCloseable {

  /**
   * Returns the identifier for this writer
   *
   * @return String id for the writer
   */
  String getID();

  /**
   * Method that can be called to initialize the writer
   *
   * @param eventWriterContext {@link EventWriterContext}
   */
  void initialize(EventWriterContext eventWriterContext);

  /**
   * Write the collection of events
   *
   * @param events {@link Collection} of {@link Event}s.
   */
  void write(Collection<? extends Event<?>> events);
}

/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.service;

import java.io.IOException;

/**
 * An internal interface used by {@link CoreMessagingService} and {@link CoreMessageFetcher} for providing
 * messaging tables.
 *
 * @param <T> Type of the message table.
 */
interface TableProvider<T> {

  /**
   * Provides an instance of messaging table of type {@code <T>}.
   */
  T get() throws IOException;
}

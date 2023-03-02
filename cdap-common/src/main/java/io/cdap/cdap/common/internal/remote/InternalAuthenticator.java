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

package io.cdap.cdap.common.internal.remote;

import java.util.function.BiConsumer;

/**
 * An interface which uses a provided {@link java.util.function.BiConsumer} function to set headers
 * on requests to propagate internal identity.
 */
public interface InternalAuthenticator {

  /**
   * Sets internal authentication headers using a provided header setting function.
   *
   * @param headerSetter A BiConsumer header setting function used to set header values for a
   *     request.
   */
  void applyInternalAuthenticationHeaders(BiConsumer<String, String> headerSetter);
}

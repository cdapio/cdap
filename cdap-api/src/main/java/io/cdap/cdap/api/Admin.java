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

package io.cdap.cdap.api;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.dataset.DatasetManager;
import io.cdap.cdap.api.messaging.MessagingAdmin;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * This interface provides methods for operational calls from within a CDAP application.
 */
@Beta
public interface Admin extends DatasetManager, SecureStoreManager, MessagingAdmin {

  /**
   * Return whether the specified namespace exists or not
   *
   * @param namespace the namespace to check
   * @return true if the namespace exists, false if not
   * @throws IOException if there was a error making the request to check namespace existence
   */
  boolean namespaceExists(String namespace) throws IOException;

  /**
   * Get information about the specified namespace. Returns null if the namespace does not exist.
   *
   * @param namespace the namespace to get information about
   * @return information about the namespace
   * @throws IOException if there was a error making the request to get namespace information
   */
  @Nullable
  NamespaceSummary getNamespaceSummary(String namespace) throws IOException;
}

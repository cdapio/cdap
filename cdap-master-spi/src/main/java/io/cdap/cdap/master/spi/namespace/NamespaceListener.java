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

package io.cdap.cdap.master.spi.namespace;

import java.util.Collection;

/**
 * A listener that responds to namespace updates.
 */
public interface NamespaceListener {

  /**
   * Performs initialization actions for all namespaces. This method will be retried in case of
   * failure, so it must be implemented in an idempotent way.
   */
  default void onStart(Collection<NamespaceDetail> namespaceDetails) throws Exception {
    // no-op by default
  }

  /**
   * Called during namespace creation. This method may be retried in case of failure, so it must be
   * implemented in an idempotent way.
   */
  default void onNamespaceCreation(NamespaceDetail namespaceDetail) throws Exception {
    // no-op by default
  }

  /**
   * Called during namespace deletion. This method may be retried in case of failure, so it must be
   * implemented in an idempotent way.
   */
  default void onNamespaceDeletion(NamespaceDetail namespaceDetail) throws Exception {
    // no-op by default
  }
}

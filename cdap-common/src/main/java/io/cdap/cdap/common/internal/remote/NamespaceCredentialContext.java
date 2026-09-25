/*
 * Copyright © 2026 Cask Data, Inc.
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

import io.cdap.cdap.proto.id.NamespaceId;

/**
 * The single namespaced credential a task worker pod runs as, held by its metadata sidecar. An
 * interface so {@link StickyLeaseManager} can be tested without a sidecar.
 */
public interface NamespaceCredentialContext {

  /**
   * Makes the pod authenticate as the given namespace. Called under the lease manager's lock when
   * the pod goes from idle to busy.
   *
   * @throws RuntimeException if provisioning fails; the task must then not run
   */
  void provision(NamespaceId namespace);

  /**
   * Removes the credential. Called under the lease manager's lock when the pod goes idle. Must not
   * throw; an implementation that can't wipe must stop the pod from taking more work.
   */
  void wipe();
}

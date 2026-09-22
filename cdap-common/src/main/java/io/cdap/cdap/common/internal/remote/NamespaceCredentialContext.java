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
 * The namespaced service account credential a task worker pod runs as.
 *
 * <p>On an RBAC instance this is not an abstraction over many credentials; it is one slot. The
 * pod's metadata sidecar holds a single mutable task context, and every task in the container
 * authenticates through it. That singularity is the reason {@link StickyLeaseManager} exists, and
 * it is why provisioning and wiping are modelled as transitions of one shared thing rather than as
 * something each task owns.
 *
 * <p>Kept as an interface so the lease manager stays a pure state machine with no knowledge of
 * GCP, HTTP, or configuration. The ordering it guarantees, provision at the first task and wipe at
 * the last, is the security property of the whole design, and it can only be asserted directly if
 * the sequence of calls is observable without a live sidecar.
 */
public interface NamespaceCredentialContext {

  /**
   * Makes the pod authenticate as the given namespace.
   *
   * <p>Called while the lease manager holds its lock, only on the transition from idle to busy.
   *
   * @param namespace the namespace whose identity the pod should assume
   * @throws RuntimeException if the credential could not be provisioned, in which case the
   *     admission is unwound and the task must not run: running it would mean running as whatever
   *     identity the pod happened to have
   */
  void provision(NamespaceId namespace);

  /**
   * Removes the credential from the pod.
   *
   * <p>Called while the lease manager holds its lock, only on the transition from busy to idle, so
   * no task is running when it happens.
   *
   * <p>Implementations must not throw. There is no caller left to handle a failure here, since the
   * task that triggered it has already finished and answered. An implementation that cannot wipe
   * is responsible for making sure the pod stops taking work instead, so the credential dies with
   * the process rather than outliving the namespace that provisioned it.
   */
  void wipe();
}

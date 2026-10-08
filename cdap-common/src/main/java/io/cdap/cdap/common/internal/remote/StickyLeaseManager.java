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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.proto.id.NamespaceId;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pins a task worker pod to one namespace while it has running tasks, and provisions that
 * namespace's credential on the first task and wipes it after the last.
 *
 * <p>Admission mirrors the proxy's {@link PodState}: an idle pod is handed to any namespace, so a
 * lease on an idle pod is only a routing hint.
 */
public class StickyLeaseManager {

  private static final Logger LOG = LoggerFactory.getLogger(StickyLeaseManager.class);

  private final int maxConcurrentTasks;
  private final NamespaceCredentialContext credentialContext;

  /** The namespace this pod last served; may be set while idle. Guarded by {@code this}. */
  @Nullable
  private NamespaceId currentLease;

  /** Number of tasks running on this pod. Guarded by {@code this}. */
  private int activeTaskCount;

  /**
   * Constructs a lease manager.
   *
   * @param maxConcurrentTasks the most tasks this pod runs at once for its leased namespace
   * @param credentialContext the pod's namespaced credential
   */
  public StickyLeaseManager(int maxConcurrentTasks,
      NamespaceCredentialContext credentialContext) {
    this.maxConcurrentTasks = maxConcurrentTasks;
    this.credentialContext = credentialContext;
  }

  /**
   * Admits a task, provisioning the namespace's credential if the pod was idle. Each successful
   * call must be paired with exactly one {@link #releaseTask}.
   *
   * @return {@link AdmissionStatus#SUCCESS} if the task may run, otherwise the rejection reason
   * @throws RuntimeException if provisioning fails; no state is changed
   */
  public synchronized AdmissionStatus admitTask(NamespaceId namespace) {
    AdmissionStatus status = evaluateAdmission(namespace);
    if (status != AdmissionStatus.SUCCESS) {
      return status;
    }

    if (activeTaskCount == 0) {
      // Idle pods hold no credential, so claim the lease and provision for this namespace.
      NamespaceId previousLease = currentLease;
      currentLease = namespace;
      try {
        credentialContext.provision(namespace);
      } catch (RuntimeException e) {
        // Undo the claim so the pod doesn't advertise a lease it can't honour.
        currentLease = previousLease;
        throw e;
      }
      if (previousLease != null && !previousLease.equals(namespace)) {
        LOG.debug("Task worker lease moved from namespace {} to {} while idle.",
            previousLease.getNamespace(), namespace.getNamespace());
      }
    }

    activeTaskCount++;
    return AdmissionStatus.SUCCESS;
  }

  /**
   * Releases a task, wiping the credential if it was the last one. Unmatched releases are logged
   * and ignored rather than corrupting the count.
   */
  public synchronized void releaseTask(NamespaceId namespace) {
    if (currentLease == null || !currentLease.equals(namespace)) {
      LOG.warn("Ignoring task completion for namespace {}: the pod lease is held by {}. "
              + "A task completed without ever being admitted.",
          namespace.getNamespace(), currentLease == null ? "nobody" : currentLease.getNamespace());
      return;
    }
    if (activeTaskCount == 0) {
      LOG.warn("Ignoring task completion for namespace {}: no tasks are active. "
          + "A completion callback fired more than once.", namespace.getNamespace());
      return;
    }

    activeTaskCount--;
    if (activeTaskCount == 0) {
      credentialContext.wipe();
    }
  }

  /** Returns the namespace this pod is leased to, or null if it has never served one. */
  @Nullable
  public synchronized NamespaceId getCurrentLease() {
    return currentLease;
  }

  @VisibleForTesting
  synchronized int getActiveTaskCount() {
    return activeTaskCount;
  }

  /** Decides whether a task may be admitted, without changing state. Mirrors {@link PodState}. */
  private AdmissionStatus evaluateAdmission(NamespaceId namespace) {
    if (activeTaskCount == 0) {
      // Idle pod. The lease, if any, is only a routing hint and is surrendered on demand.
      return AdmissionStatus.SUCCESS;
    }
    if (!namespace.equals(currentLease)) {
      // Busy with another namespace; two namespaces must never share a JVM.
      return AdmissionStatus.REJECTED_MISMATCH;
    }
    if (activeTaskCount >= maxConcurrentTasks) {
      return AdmissionStatus.REJECTED_MAX_CONCURRENCY;
    }
    return AdmissionStatus.SUCCESS;
  }

  /**
   * Outcome of an attempt to admit a task onto this pod.
   */
  public enum AdmissionStatus {
    /**
     * The task may run.
     */
    SUCCESS,

    /**
     * The pod is busy running user code for a different namespace.
     */
    REJECTED_MISMATCH,

    /**
     * The pod is serving the right namespace but is already at its concurrency limit.
     */
    REJECTED_MAX_CONCURRENCY
  }
}

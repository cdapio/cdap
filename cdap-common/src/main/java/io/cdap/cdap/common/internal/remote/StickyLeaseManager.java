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
 * Tracks which namespace a single task worker pod is currently serving, and owns the lifetime of
 * that namespace's service account credential on the pod.
 *
 * <p>On an RBAC instance every task runs as a namespaced service account, and the credential is
 * held in a single mutable context on the pod's metadata sidecar. Historically the task worker
 * protected that shared context by refusing to run more than one task at a time. This class
 * replaces that blunt guarantee with a narrower one that survives concurrency:
 *
 * <blockquote>The credential never outlives the last task of the namespace that provisioned
 * it.</blockquote>
 *
 * <p>At one task per pod the two statements are identical, so this is not a weakening. It is the
 * same invariant restated in a form that is still true when tasks overlap, which is what lets a
 * pod serve up to {@code task.worker.request.limit} tasks concurrently.
 *
 * <h2>Credential lifetime</h2>
 *
 * <p>The credential is provisioned when {@code activeTaskCount} rises from zero to one and wiped
 * when it falls back to zero. Both transitions happen while holding this object's monitor, so a
 * task starting cannot interleave with a task finishing and leave the pod either credential-less
 * while work is running, or credentialed while idle. This yields the invariant relied on
 * throughout this class: <em>{@code activeTaskCount == 0} implies no credential is provisioned</em>.
 *
 * <p>Because the wipe happens the instant the pod goes idle, no background reclamation thread is
 * needed. Any periodic sweep could only ever be later.
 *
 * <h2>Admission</h2>
 *
 * <p>The admission rules deliberately mirror the proxy's pod selection in {@link PodState} exactly.
 * The proxy will steal an idle pod for a different namespace without consulting how long it has
 * been idle, so the worker must be willing to hand it over on the same terms. If the worker were
 * stricter, the proxy would keep routing to pods that then reject the request, manufacturing
 * rejections in steady state that no amount of retrying would fix.
 *
 * <p>A lease on an idle pod is therefore only a routing hint, not a reservation. It records who
 * ran last so the proxy can prefer a warm pod, and it is surrendered the moment somebody else
 * wants it.
 */
public class StickyLeaseManager {

  private static final Logger LOG = LoggerFactory.getLogger(StickyLeaseManager.class);

  private final int maxConcurrentTasks;
  private final NamespaceCredentialContext credentialContext;

  /**
   * The namespace this pod most recently served. Non-null does not imply work is in progress; see
   * the class javadoc on leases as routing hints. Guarded by {@code this}.
   */
  @Nullable
  private NamespaceId currentLease;

  /**
   * Number of tasks currently executing on this pod. Guarded by {@code this}.
   */
  private int activeTaskCount;

  /**
   * Constructs a lease manager.
   *
   * @param maxConcurrentTasks the most tasks this pod will run at once for its leased namespace
   * @param credentialContext the pod's single namespaced credential, provisioned when the pod goes
   *     from idle to busy and wiped when it goes back to idle, both while holding this object's
   *     lock
   */
  public StickyLeaseManager(int maxConcurrentTasks,
      NamespaceCredentialContext credentialContext) {
    this.maxConcurrentTasks = maxConcurrentTasks;
    this.credentialContext = credentialContext;
  }

  /**
   * Admits a task for the given namespace, provisioning the namespace's credential first if this
   * is the task that wakes an idle pod.
   *
   * <p>A successful call must be paired with exactly one {@link #releaseTask(NamespaceId)} for the
   * same namespace, otherwise the pod either wipes the credential while work is still running or
   * never wipes it at all. A rejected call must not be paired with anything.
   *
   * @param namespace the namespace the task belongs to
   * @return {@link AdmissionStatus#SUCCESS} if the task may run, otherwise the reason it may not
   * @throws RuntimeException if credential provisioning fails, in which case no state is changed
   *     and the task must not run
   */
  public synchronized AdmissionStatus admitTask(NamespaceId namespace) {
    AdmissionStatus status = evaluateAdmission(namespace);
    if (status != AdmissionStatus.SUCCESS) {
      return status;
    }

    if (activeTaskCount == 0) {
      // The pod is idle, so by the class invariant it currently holds no credential. Point the
      // lease at this namespace and provision for it. This covers all three idle cases: a pod that
      // has never run anything, one being re-woken by the namespace it already served, and one
      // being taken over by a different namespace.
      NamespaceId previousLease = currentLease;
      currentLease = namespace;
      try {
        credentialContext.provision(namespace);
      } catch (RuntimeException e) {
        // Provisioning failed, so the task cannot run as the right identity. Undo the claim rather
        // than leave the pod advertising a lease it cannot honour.
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
   * Releases a task's hold on this pod, wiping the namespace's credential if it was the last one.
   *
   * <p>Refuses to decrement below zero or on behalf of a namespace that does not hold the lease.
   * Either would mean a completion callback fired without a matching {@link
   * #admitTask(NamespaceId)}, and acting on it would corrupt the count. An over-count strands the
   * pod because every admission path checks the count; an under-count wipes the credential out
   * from under running tasks. Both are worse than ignoring the call and saying so loudly.
   *
   * @param namespace the namespace of the task that finished
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

  /**
   * Returns the namespace this pod is leased to, or null if it has never served one.
   *
   * <p>Reported to the proxy on a rejection so it can correct its routing table.
   */
  @Nullable
  public synchronized NamespaceId getCurrentLease() {
    return currentLease;
  }

  @VisibleForTesting
  synchronized int getActiveTaskCount() {
    return activeTaskCount;
  }

  /**
   * Decides whether a task may be admitted, without changing any state.
   *
   * <p>Mirrors {@link PodState#tryAcquireWarmLease(String, int)} and {@code tryStealIdleLease}: a
   * matching namespace is admitted up to the concurrency limit, and an idle pod is admitted
   * regardless of which namespace last held it.
   */
  private AdmissionStatus evaluateAdmission(NamespaceId namespace) {
    if (activeTaskCount == 0) {
      // Idle pod. The lease, if any, is only a routing hint and is surrendered on demand.
      return AdmissionStatus.SUCCESS;
    }
    if (!namespace.equals(currentLease)) {
      // Busy with another namespace's user code. Handing this pod over now would mean two
      // namespaces sharing a JVM, which is the thing the lease exists to prevent.
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

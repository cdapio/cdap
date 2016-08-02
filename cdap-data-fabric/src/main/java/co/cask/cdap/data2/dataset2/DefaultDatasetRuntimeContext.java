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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.ReadWrite;
import co.cask.cdap.api.annotation.WriteOnly;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.writer.LineageWriterDatasetFramework;
import co.cask.cdap.internal.dataset.DatasetRuntimeContext;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.common.Cancellable;

import java.lang.annotation.Annotation;
import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * The default implementation of {@link DatasetRuntimeContext}. It performs authorization, lineage and usage recording
 * for each individual dataset operation.
 */
public class DefaultDatasetRuntimeContext extends DatasetRuntimeContext {

  /**
   * A helper interface for {@link DefaultDatasetRuntimeContext} to abstract lineage writing, audit log publishing
   * and usage recording logic.
   */
  public interface DatasetAccessRecorder {

    /**
     * Records lineage for the given dataset with the given access type.
     */
    void recordLineage(AccessType accessType);

    /**
     * Emits an audit log for the given dataset with the given access type.
     */
    void emitAudit(AccessType accessType);
  }

  private static final Map<Class<? extends Annotation>, AccessInfo> ANNOTATION_TO_ACCESS_INFO = ImmutableMap.of(
    ReadOnly.class, new AccessInfo(EnumSet.of(Action.READ), AccessType.READ),
    WriteOnly.class, new AccessInfo(EnumSet.of(Action.WRITE), AccessType.WRITE),
    ReadWrite.class, new AccessInfo(EnumSet.of(Action.READ, Action.WRITE), AccessType.READ_WRITE)
  );

  private final ThreadLocal<CallStack> callStack = new InheritableThreadLocal<CallStack>() {
      @Override
      protected CallStack initialValue() {
        return new CallStack();
      }

      @Override
      protected CallStack childValue(CallStack parentValue) {
        // Copy the stack
        return new CallStack(parentValue);
      }
    };

  private final AuthorizationEnforcer enforcer;
  private final DatasetAccessRecorder accessRecorder;
  private final Principal principal;
  private final DatasetId datasetId;

  // Just use simple primitive array to memorize whether a particular access type has lineage/audit recorded or not
  // Don't worry about concurrency because
  // 1. doesn't matter too much if the lineage/audit get recorded again from different thread, but it'll be only once
  //    per type per thread
  // 2. the lineage writer implementation anyway has a concurrent map for caching. The check here acts as a
  //    low cost gate instead of hitting the ConcurrentMap.putIfAbsent method with new object creation on
  //    every DS operation call. Similar for audit log publisher, it has a cache.
  private final boolean[] lineageRecorded;
  private final boolean[] auditRecorded;

  /**
   * Helper method to execute a {@link Callable} with a {@link DatasetRuntimeContext}.
   * This method is mainly called from
   * {@link LineageWriterDatasetFramework#getDataset(Id.DatasetInstance, Map, ClassLoader, DatasetClassLoaderProvider,
   * Iterable, AccessType)}.
   */
  public static <T> T execute(AuthorizationEnforcer enforcer,
                              DatasetAccessRecorder accessRecorder,
                              Principal principal,
                              DatasetId datasetId,
                              Callable<T> callable) throws Exception {
    // Memorize the old context, change to a new one and restore it at the end.
    // It is needed so that nested call to DatasetFramework.getDataset can create the call site context correctly.
    Cancellable cancel = setContext(new DefaultDatasetRuntimeContext(enforcer, accessRecorder, principal, datasetId));
    try {
      return callable.call();
    } finally {
      cancel.cancel();
    }
  }

  private DefaultDatasetRuntimeContext(AuthorizationEnforcer enforcer, DatasetAccessRecorder accessRecorder,
                                       Principal principal, DatasetId datasetId) {
    this.enforcer = enforcer;
    this.accessRecorder = accessRecorder;
    this.principal = principal;
    this.datasetId = datasetId;
    this.lineageRecorded = new boolean[AccessType.values().length];
    this.auditRecorded = new boolean[AccessType.values().length];
  }

  @Override
  public void onMethodEntry(@Nullable Class<? extends Annotation> annotation) {
    CallStack callStack = this.callStack.get();
    AccessInfo accessInfo;
    AccessType lineageType;
    AccessType auditType;

    if (annotation != null) {
      accessInfo = ANNOTATION_TO_ACCESS_INFO.get(annotation);
      if (accessInfo == null) {
        // shouldn't happen
        throw new DataSetException("Unsupported annotation " + annotation + " on dataset " + datasetId);
      }

      // Perform authorization if the method is annotated.
      try {
        enforcer.enforce(datasetId, principal, accessInfo.getActions());
      } catch (Exception e) {
        throw new DataSetException("The principal " + principal + " is not authorized to access " + datasetId +
                                     " for operation types " + accessInfo.getActions(), e);
      }

      lineageType = callStack.enter(accessInfo.getAccessType());
      auditType = accessInfo.getAccessType();
    } else {
      lineageType = callStack.enter(AccessType.UNKNOWN);
      auditType = AccessType.UNKNOWN;
    }
    recordAccess(lineageType, auditType);
  }

  @Override
  public void onMethodExit() {
    // This method should be called when the method exit, which should happen in the same thread
    // as the method entry call.
    callStack.get().exit();
  }

  private void recordAccess(AccessType lineageType, AccessType auditType) {
    if (!lineageRecorded[lineageType.ordinal()]) {
      accessRecorder.recordLineage(lineageType);
      lineageRecorded[lineageType.ordinal()] = true;
    }
    if (!auditRecorded[auditType.ordinal()]) {
      accessRecorder.emitAudit(auditType);
      auditRecorded[auditType.ordinal()] = true;
    }
  }

  /**
   * Inner container class for fast access information lookup based on method annotation.
   */
  private static final class AccessInfo {

    private final Set<Action> actions;
    private final AccessType accessType;

    private AccessInfo(Set<Action> actions, AccessType accessType) {
      this.actions = actions;
      this.accessType = accessType;
    }

    Set<Action> getActions() {
      return actions;
    }

    AccessType getAccessType() {
      return accessType;
    }
  }

  /**
   * Inner helper class to keep track of dataset method call stack.
   */
  private final class CallStack {

    private final ArrayDeque<AccessType> stack;
    private final int minSize;

    CallStack() {
      this.stack = new ArrayDeque<>(10);
      this.minSize = 0;
    }

    CallStack(CallStack other) {
      this.stack = new ArrayDeque<>(other.stack);
      this.minSize = stack.size();
    }

    /**
     * Called from {@link #onMethodEntry(Class)}.
     *
     * @param accessType the access type derived based on the method annotation
     * @return the actual access type to use for lineage recording
     */
    AccessType enter(AccessType accessType) {
      // If there is a parent access type (meaning the current call is nested inside some other dataset method call)
      // and if it is not UNKNOWN, then use that as the lineage access type and keep propagating that in the stack.
      AccessType parentType = stack.peekLast();
      if (parentType != null && parentType != AccessType.UNKNOWN) {
        accessType = parentType;
      }
      stack.addLast(accessType);
      return accessType;
    }

    void exit() {
      // Make sure we won't pop more than it should
      if (stack.size() <= minSize) {
        throw new DataSetException("Invalid dataset call stack for dataset " + datasetId +
                                     ". Potentially caused by illegal manipulation of callstack");
      }
      stack.removeLast();
    }
  }
}

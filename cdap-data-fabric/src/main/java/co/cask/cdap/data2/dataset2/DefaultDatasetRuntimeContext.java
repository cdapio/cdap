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

import co.cask.cdap.api.annotation.NoAccess;
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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.common.Cancellable;

import java.lang.annotation.Annotation;
import java.util.ArrayDeque;
import java.util.Deque;
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
    void recordAccess(AccessType accessType);
  }

  private static final Map<Class<? extends Annotation>, ? extends Set<Action>> ANNOTATION_TO_ACTIONS = ImmutableMap.of(
    ReadOnly.class, EnumSet.of(Action.READ),
    WriteOnly.class, EnumSet.of(Action.WRITE),
    ReadWrite.class, EnumSet.of(Action.READ, Action.WRITE),
    NoAccess.class, EnumSet.noneOf(Action.class)
  );

  private final ThreadLocal<Deque<Class<? extends Annotation>>> callStack =
    new InheritableThreadLocal<Deque<Class<? extends Annotation>>>() {
      @Override
      protected Deque<Class<? extends Annotation>> initialValue() {
        return new ArrayDeque<>(10);
      }

      @Override
      protected Deque<Class<? extends Annotation>> childValue(Deque<Class<? extends Annotation>> parentValue) {
        // Copy the top of the stack
        Deque<Class<? extends Annotation>> stack = new ArrayDeque<>(10);
        Class<? extends Annotation> annotation = parentValue.peekLast();

        // In normal case it shouldn't be null. However, in case the dataset calls the onMethodExit
        // intentionally, then this can be null.
        if (annotation != null) {
          stack.addLast(annotation);
        }
        return stack;
      }
    };

  private final AuthorizationEnforcer enforcer;
  private final DatasetAccessRecorder accessRecorder;
  private final Principal principal;
  private final DatasetId datasetId;

  // Just use simple primitive to memorize whether a particular access type has lineage recorded or not
  // Don't worry about concurrency because
  // 1. doesn't matter too much if the lineage get recorded again from different thread, but it'll be only once
  //    per type per thread
  // 2. the lineage writer implementation anyway has a concurrent map for caching. The check here acts as a
  //    low cost gate instead of hitting the ConcurrentMap.putIfAbsent method with new object creation on
  //    every DS operation call.
  private boolean recordedNoAccess;
  private boolean recordedRead;
  private boolean recordedWrite;
  private boolean recordedReadWrite;

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
  }

  @Override
  public void onMethodEntry(@Nullable Class<? extends Annotation> annotation,
                            Class<? extends Annotation> defaultAnnotation) {
    final Deque<Class<? extends Annotation>> callStack = this.callStack.get();
    Class<? extends Annotation> parentAnnotation = callStack.peekLast();

    if (annotation == null) {
      // If there is no annotation on the method, either use immediate parent one from the call stack
      // or use the default one if the call stack is empty
      annotation = (parentAnnotation == null) ? defaultAnnotation : parentAnnotation;
    }

    // Do a quick check if the current permission is allowed by the immediate parent if there is one.
    if (parentAnnotation != null && !isAllowed(parentAnnotation, annotation)) {
      throw new DataSetException(
        String.format("Current scope only allows @%s, but the current method requires @%s",
                      parentAnnotation.getSimpleName(), annotation.getSimpleName()));
    }

    // Performs authentication and lineage recording
    Set<Action> actions = ANNOTATION_TO_ACTIONS.get(annotation);
    if (actions == null) {
      // shouldn't happen
      throw new DataSetException("Unsupported annotation " + annotation);
    }
    try {
      enforcer.enforce(datasetId, principal, actions);
    } catch (Exception e) {
      // Need to turn the exception into runtime exception because the dataset method that call this
      // may not have throws Exception.
      throw Throwables.propagate(e);
    }

    // Record the access (lineage, audit).
    recordAccess(annotation);

    callStack.addLast(annotation);
  }

  @Override
  public void onMethodExit() {
    // The cancellable should be called when the method exit, which should happen in the same thread
    // as the method entry call.
    callStack.get().pollLast();
  }

  private boolean isAllowed(Class<? extends Annotation> parent, Class<? extends Annotation> current) {
    // If same annotation, then it's allowed
    if (parent == current) {
      return true;
    }
    // If parent != current and parent is NoAccess, then it's not allowed
    if (parent == NoAccess.class) {
      return false;
    }
    // If parent != current, then
    // 1. if current is NoAccess, then it's allowed
    // 2. if parent is ReadWrite, then current can be anything
    // All other combinations should be rejected.
    // reject (ReadOnly, WriteOnly)
    // reject (ReadOnly, ReadWrite)
    // reject (WriteOnly, ReadOnly)
    // reject (WriteOnly, ReadWrite)
    return current == NoAccess.class || parent == ReadWrite.class;
  }

  private void recordAccess(Class<? extends Annotation> annotation) {
    if (annotation == NoAccess.class && !recordedNoAccess) {
      accessRecorder.recordAccess(AccessType.UNKNOWN);
      recordedNoAccess = true;
      return;
    }
    if (annotation == ReadOnly.class && !recordedRead) {
      accessRecorder.recordAccess(AccessType.READ);
      recordedRead = true;
      return;
    }
    if (annotation == WriteOnly.class && !recordedWrite) {
      accessRecorder.recordAccess(AccessType.WRITE);
      recordedWrite = true;
      return;
    }
    if (annotation == ReadWrite.class && !recordedReadWrite) {
      accessRecorder.recordAccess(AccessType.READ_WRITE);
      recordedReadWrite = true;
    }
  }
}

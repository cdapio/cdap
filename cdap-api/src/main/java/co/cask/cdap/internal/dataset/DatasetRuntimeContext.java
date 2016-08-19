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

package co.cask.cdap.internal.dataset;

import co.cask.cdap.api.annotation.ReadOnly;
import co.cask.cdap.api.annotation.ReadWrite;
import co.cask.cdap.api.annotation.WriteOnly;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import javax.annotation.Nullable;

/**
 * The internal context object used by the Dataset framework to track Dataset methods.
 */
public abstract class DatasetRuntimeContext {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetRuntimeContext.class);
  private static final ThreadLocal<DatasetRuntimeContext> CONTEXT_THREAD_LOCAL = new InheritableThreadLocal<>();

  /**
   * Return the current {@link DatasetRuntimeContext}.
   */
  public static DatasetRuntimeContext getContext() {
    DatasetRuntimeContext context = CONTEXT_THREAD_LOCAL.get();
    if (context != null) {
      return context;
    }

    // If there is no current context, return a no-op one.
    // For user dataset, there is always one being set.
    return new DatasetRuntimeContext() {
      @Override
      public void onMethodEntry(boolean constructor, @Nullable Class<? extends Annotation> annotation) {
        // no-op
      }

      @Override
      public void onMethodExit() {
        // no-op
      }
    };
  }

  /**
   * Sets the current {@link DatasetRuntimeContext}. This method can only be initiated from system class.
   */
  public static Cancellable setContext(DatasetRuntimeContext context) {
    final Class[] callerClasses = CallerClassSecurityManager.getCallerClasses();
    // 0 is the CallerClassSecurityManager, 1 is this class, hence 2 is the actual caller
    if (callerClasses.length < 3) {
      // This shouldn't happen as there should be someone calling this method.
      throw new IllegalStateException("Invalid call stack.");
    }
    // This is the guard against if someone tries to call this method outside of CDAP system (e.g. from user code)
    if (callerClasses[2].getClassLoader() != DatasetRuntimeContext.class.getClassLoader()
      || !callerClasses[2].getName().equals("co.cask.cdap.data2.dataset2.DefaultDatasetRuntimeContext")) {
      throw new IllegalAccessError("Not allow to set context from " + callerClasses[2]);
    }

    final Thread callerThread = Thread.currentThread();
    final DatasetRuntimeContext oldContext = CONTEXT_THREAD_LOCAL.get();
    CONTEXT_THREAD_LOCAL.set(context);
    return new Cancellable() {
      @Override
      public void cancel() {
        // This must be called from the same thread that call setContext
        Thread currentThread = Thread.currentThread();
        if (currentThread != callerThread) {
          LOG.warn("Cancel is called from different thread. Expected {}, actual: {}", callerThread, currentThread);
          return;
        }
        CONTEXT_THREAD_LOCAL.set(oldContext);
      }
    };
  }

  /**
   * Method to call when a dataset method is invoked.
   *
   * @param constructor true if the call comes from constructor
   * @param annotation one of the {@link ReadOnly}, {@link WriteOnly} or {@link ReadWrite} annotation
   *                   on the method. Use {@code null} for method that is not annotated,
   */
  @SuppressWarnings("unused")
  public abstract void onMethodEntry(boolean constructor, @Nullable Class<? extends Annotation> annotation);

  /**
   * Method to call when a dataset method is about to return.
   */
  @SuppressWarnings("unused")
  public abstract void onMethodExit();

  /**
   * A {@link SecurityManager} to help get access to classes in the call stack.
   */
  private static final class CallerClassSecurityManager extends SecurityManager {

    private static final CallerClassSecurityManager INSTANCE = new CallerClassSecurityManager();

    static Class[] getCallerClasses() {
      return INSTANCE.getClassContext();
    }
  }
}

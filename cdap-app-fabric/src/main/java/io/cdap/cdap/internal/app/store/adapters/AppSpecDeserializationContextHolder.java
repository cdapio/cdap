/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store.adapters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the {@link AppSpecDeserializationContext} for the current thread's deserialization
 * operation. Ensures context is cleared after use.
 */
public final class AppSpecDeserializationContextHolder {

  private static final Logger LOG = LoggerFactory.getLogger(
      AppSpecDeserializationContextHolder.class);
  private static final ThreadLocal<AppSpecDeserializationContext> CURRENT_CONTEXT = new ThreadLocal<>();

  private AppSpecDeserializationContextHolder() {
  }

  /**
   * Sets the context for the current thread. Should be called before deserialization.
   */
  public static void setContext(AppSpecDeserializationContext context) {
    if (CURRENT_CONTEXT.get() != null) {
      LOG.warn("Overwriting existing AppSpecDeserializationContext on thread {}. "
              + "This might indicate a missing clearContext() call from a previous operation.",
          Thread.currentThread().getName());
    }
    CURRENT_CONTEXT.set(context);
  }

  /**
   * Gets the context for the current thread.
   *
   * @throws IllegalStateException if no context is set.
   */
  public static AppSpecDeserializationContext getContext() {
    AppSpecDeserializationContext context = CURRENT_CONTEXT.get();
    if (context == null) {
      throw new IllegalStateException(
          "AppSpecDeserializationContext has not been set for the current deserialization operation on this thread.");
    }
    return context;
  }

  /**
   * Clears the context for the current thread. Must be called in a finally block.
   */
  public static void clearContext() {
    CURRENT_CONTEXT.remove();
  }
}

/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.dynamic;

import co.cask.cdap.api.spark.dynamic.SparkInterpreter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.ref.WeakReference;

/**
 * Manage {@link SparkInterpreter} cleanup process. It tracks unclosed {@link SparkInterpreter} through
 * {@link WeakReference} and perform cleanup on behalf.
 */
public class SparkInterpreterCleanupManager implements AutoCloseable {

  private final Cache<SparkInterpreter, Closeable> interpreterCloseables;

  public SparkInterpreterCleanupManager() {
    this.interpreterCloseables = CacheBuilder
      .newBuilder()
      .weakKeys()
      .removalListener(new RemovalListener<SparkInterpreter, Closeable>() {
        @Override
        public void onRemoval(RemovalNotification<SparkInterpreter, Closeable> notification) {
          Closeables.closeQuietly(notification.getValue());
        }
      })
      .build();
  }

  /**
   * Adds a {@link SparkInterpreter}.
   *
   * @param interpreter the interpreter for cleanup management
   * @param closeable the {@link Closeable} for cleanup
   */
  public void addInterpreter(SparkInterpreter interpreter, Closeable closeable) {
    interpreterCloseables.put(interpreter, closeable);
    interpreterCloseables.cleanUp();
  }

  /**
   * Removes a {@link SparkInterpreter} with resource cleanup through the {@link Closeable} provided in
   * the {@link #addInterpreter(SparkInterpreter, Closeable)} method.
   *
   * @param interpreter the interpreter to remove
   */
  public void removeInterpreter(SparkInterpreter interpreter) {
    interpreterCloseables.invalidate(interpreter);
    interpreterCloseables.cleanUp();
  }

  /**
   * Performs cleanup for all {@link SparkInterpreter} managed by this manager.
   */
  @Override
  public void close() {
    interpreterCloseables.invalidateAll();
    interpreterCloseables.cleanUp();
  }
}

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

import co.cask.cdap.api.spark.dynamic.SparkCompiler;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closeables;

import java.io.Closeable;
import java.lang.ref.WeakReference;

/**
 * Manage {@link SparkCompiler} cleanup process. It tracks unclosed {@link SparkCompiler} through
 * {@link WeakReference} and perform cleanup on behalf.
 */
public class SparkCompilerCleanupManager implements AutoCloseable {

  private final Cache<SparkCompiler, Closeable> compilerCloseables;

  public SparkCompilerCleanupManager() {
    this.compilerCloseables = CacheBuilder
      .newBuilder()
      .weakKeys()
      .removalListener(new RemovalListener<SparkCompiler, Closeable>() {
        @Override
        public void onRemoval(RemovalNotification<SparkCompiler, Closeable> notification) {
          Closeables.closeQuietly(notification.getValue());
        }
      })
      .build();
  }

  /**
   * Adds a {@link SparkCompiler}.
   *
   * @param compiler the compiler for cleanup management
   * @param closeable the {@link Closeable} for cleanup
   */
  public void addCompiler(SparkCompiler compiler, Closeable closeable) {
    compilerCloseables.put(compiler, closeable);
    compilerCloseables.cleanUp();
  }

  /**
   * Removes a {@link SparkCompiler} with resource cleanup through the {@link Closeable} provided in
   * the {@link #addCompiler(SparkCompiler, Closeable)} method.
   *
   * @param compiler the compiler to remove
   */
  public void removeCompiler(SparkCompiler compiler) {
    compilerCloseables.invalidate(compiler);
    compilerCloseables.cleanUp();
  }

  /**
   * Performs cleanup for all {@link SparkCompiler} managed by this manager.
   */
  @Override
  public void close() {
    compilerCloseables.invalidateAll();
    compilerCloseables.cleanUp();
  }
}

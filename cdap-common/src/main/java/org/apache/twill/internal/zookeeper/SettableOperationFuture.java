/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal.zookeeper;

import com.google.common.util.concurrent.AbstractFuture;
import org.apache.twill.zookeeper.OperationFuture;

import java.util.concurrent.Executor;
import javax.annotation.Nullable;

/**
 * An implementation for {@link OperationFuture} that allows setting result directly.
 * Also, all listener callback will be fired from the given executor.
 *
 * @param <V> The result type returned by this Future's {@link #get()} method.
 */
public final class SettableOperationFuture<V> extends AbstractFuture<V> implements OperationFuture<V> {

  private final String requestPath;
  private final Executor executor;

  public static <V> SettableOperationFuture<V> create(String path, Executor executor) {
    return new SettableOperationFuture<>(path, executor);
  }

  private SettableOperationFuture(String requestPath, Executor executor) {
    this.requestPath = requestPath;
    this.executor = executor;
  }

  @Override
  public String getRequestPath() {
    return requestPath;
  }

  @Override
  public void addListener(final Runnable listener, final Executor exec) {
    super.addListener(new Runnable() {
      @Override
      public void run() {
        exec.execute(listener);
      }
    }, executor);
  }

  @Override
  public boolean setException(Throwable throwable) {
    return super.setException(throwable);
  }

  @Override
  public boolean set(@Nullable V value) {
    return super.set(value);
  }
}

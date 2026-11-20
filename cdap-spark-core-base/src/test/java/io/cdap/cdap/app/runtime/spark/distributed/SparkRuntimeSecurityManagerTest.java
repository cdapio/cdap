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

package io.cdap.cdap.app.runtime.spark.distributed;

import java.io.Closeable;
import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SparkRuntimeSecurityManager}.
 */
public class SparkRuntimeSecurityManagerTest {

  @Test
  public void testCheckExitClosesCloseableQuietlyAndDelegates() {
    final AtomicBoolean closed = new AtomicBoolean(false);
    Closeable failingCloseable = new Closeable() {
      @Override
      public void close() throws IOException {
        closed.set(true);
        throw new IOException("simulated close error");
      }
    };

    SecurityManager saved = System.getSecurityManager();
    final AtomicInteger delegatedExitCode = new AtomicInteger(-1);
    SecurityManager delegate = new SecurityManager() {
      @Override
      public void checkPermission(Permission perm) {
      }

      @Override
      public void checkPermission(Permission perm, Object context) {
      }

      @Override
      public void checkExit(int status) {
        delegatedExitCode.set(status);
      }
    };

    SparkRuntimeSecurityManager manager;
    try {
      System.setSecurityManager(delegate);
      manager = new SparkRuntimeSecurityManager(failingCloseable);
    } finally {
      System.setSecurityManager(saved);
    }

    manager.checkExit(42);
    Assert.assertEquals(42, delegatedExitCode.get());
    Assert.assertTrue(closed.get());

    // Null closeable must also be handled quietly without throwing.
    SparkRuntimeSecurityManager nullCloseableManager = new SparkRuntimeSecurityManager(null);
    nullCloseableManager.checkExit(0);
  }
}

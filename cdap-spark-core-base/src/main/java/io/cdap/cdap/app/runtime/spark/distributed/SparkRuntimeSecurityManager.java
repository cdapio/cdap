/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.security.Permission;

/**
 * A {@link SecurityManager} for Spark containers (driver and executors) to intercept call to System.exit and to
 * shutdown system services.
 */
final class SparkRuntimeSecurityManager extends SecurityManager {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeSecurityManager.class);

  private final SecurityManager delegate;
  private final Closeable closeable;

  SparkRuntimeSecurityManager(Closeable closeable) {
    this.delegate = System.getSecurityManager();
    this.closeable = closeable;
  }

  @Override
  public void checkPermission(Permission perm) {
    if ("setSecurityManager".equals(perm.getName()) && isFromSpark()) {
      LOG.warn("Spark program is setting SecurityManager. This can result in logs or metrics not " +
                 "being collected when the Spark application completed.");
    }
    if (delegate != null) {
      delegate.checkPermission(perm);
    }
  }

  @Override
  public void checkPermission(Permission perm, Object context) {
    if ("setSecurityManager".equals(perm.getName()) && isFromSpark()) {
      LOG.warn("Spark program is setting SecurityManager. This can result in logs or metrics not " +
                 "being collected when the Spark application completed.");
    }
    if (delegate != null) {
      delegate.checkPermission(perm, context);
    }
  }

  @Override
  public void checkExit(int status) {
    if (delegate != null) {
      delegate.checkExit(status);
    }
    Closeables.closeQuietly(closeable);
  }

  /**
   * Returns true if the current class context has spark class.
   */
  private boolean isFromSpark() {
    for (Class<?> c : getClassContext()) {
      if (c.getName().startsWith("org.apache.spark.")) {
        return true;
      }
    }
    return false;
  }
}

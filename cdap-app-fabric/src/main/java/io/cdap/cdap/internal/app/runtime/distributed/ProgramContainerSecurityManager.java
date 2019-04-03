/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import java.security.Permission;
import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * A {@link SecurityManager} used by the program container. Currently it specifically disabling
 * Spark classes to call {@link System#exit(int)}, {@link Runtime#halt(int)}
 * and {@link System#setSecurityManager(SecurityManager)}.
 * This is to workaround modification done in HDP Spark (CDAP-3014).
 *
 * In general, we can use the security manager to restrict what system actions can be performed by program as well.
 */
final class ProgramContainerSecurityManager extends SecurityManager {

  private final SecurityManager delegate;

  ProgramContainerSecurityManager(@Nullable SecurityManager delegate) {
    this.delegate = delegate;
  }

  @Override
  public void checkPermission(Permission perm) {
    if ("setSecurityManager".equals(perm.getName()) && isFromSpark()) {
      throw new SecurityException("Set SecurityManager not allowed from Spark class: "
                                    + Arrays.toString(getClassContext()));
    }
    if (delegate != null) {
      delegate.checkPermission(perm);
    }
  }

  @Override
  public void checkPermission(Permission perm, Object context) {
    if ("setSecurityManager".equals(perm.getName()) && isFromSpark()) {
      throw new SecurityException("Set SecurityManager not allowed from Spark class: "
                                    + Arrays.toString(getClassContext()));
    }
    if (delegate != null) {
      delegate.checkPermission(perm, context);
    }
  }

  @Override
  public void checkExit(int status) {
    if (isFromSpark()) {
      throw new SecurityException("Exit not allowed from Spark class: " + Arrays.toString(getClassContext()));
    }
    if (delegate != null) {
      delegate.checkExit(status);
    }
  }

  /**
   * Returns true if the current class context has spark class.
   */
  private boolean isFromSpark() {
    for (Class c : getClassContext()) {
      if (c.getName().startsWith("org.apache.spark.")) {
        return true;
      }
    }
    return false;
  }
}

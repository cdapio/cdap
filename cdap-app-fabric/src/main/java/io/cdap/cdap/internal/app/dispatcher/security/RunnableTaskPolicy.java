/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.dispatcher.security;

import java.io.FilePermission;
import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.security.SecurityPermission;

/**
 * Custom Policy for restricting operations in {@link io.cdap.cdap.internal.app.dispatcher.RunnableTask}
 */
public class RunnableTaskPolicy extends Policy {
  private static final String[] ALLOWED_PATHS_PREFIX = {System.getProperty("user.home") + "/", "/tmp/"};

  @Override
  public boolean implies(ProtectionDomain domain, Permission permission) {
    if (permission instanceof FilePermission) {
      for (String prefix : ALLOWED_PATHS_PREFIX) {
        if (permission.getName().startsWith(prefix)) {
          return true;
        }
      }
      return false;
    }
    if (permission instanceof SecurityPermission) {
      // disabling all security permissions
      return false;
    }
    if (permission instanceof RuntimePermission) {
      if (permission.getName().contains("exitVM")) {
        return false;
      }
      return true;
    }
    //white-list everything else
    return true;
  }
}

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

package io.cdap.cdap.internal.lang;

import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * A {@link SecurityManager} to help get access to classes in the call stack.
 */
public final class CallerClassSecurityManager extends SecurityManager {

  private static final CallerClassSecurityManager INSTANCE = new CallerClassSecurityManager();

  /**
   * Returns the current execution stack as an array of classes.
   *
   * @see SecurityManager#getClassContext()
   */
  public static Class[] getCallerClasses() {
    return INSTANCE.getClassContext();
  }

  /**
   * Finds the {@link ClassLoader} of the given type along the caller class chain. It returns {@code null} if it cannot
   * find any.
   */
  @Nullable
  public static ClassLoader findCallerClassLoader(Class<? extends ClassLoader> classloaderType) {
    return Arrays.stream(INSTANCE.getClassContext())
      .map(Class::getClassLoader)
      .filter(classloaderType::isInstance)
      .findFirst()
      .orElse(null);
  }

  private CallerClassSecurityManager() {
    super();
  }
}

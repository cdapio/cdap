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

package co.cask.cdap.logging.filter;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.common.app.MainClassLoader;

import java.util.HashMap;
import java.util.Map;

/**
 * Filter to only allow User logs
 */
public class UserLogsFilter implements Filter {
  private final boolean enabled;
  private static final Map<String, Boolean> classLoadableCache = new HashMap<>();

  public UserLogsFilter() {
    this("off");
  }

  public UserLogsFilter(String value) {
    if (value.equals("on")) {
      this.enabled = true;
    } else {
      this.enabled = false;
    }
  }

  /*
   * if the CDAP System classloader cannot load the class, assume its a User class
   */
  private boolean isClassLoadable(ILoggingEvent event) {
    ClassLoader mainClassLoader = MainClassLoader.createFromContext();
    String className = event.getCallerData()[0].getClassName();
    Boolean savedDecision = classLoadableCache.get(className);
    if (savedDecision != null) {
      return savedDecision;
    }
    try {
      mainClassLoader.loadClass(className);
      classLoadableCache.put(className, true);
      return true;
    } catch (ClassNotFoundException e) {
      // class cant be loaded, assume User Class
      classLoadableCache.put(className, false);
      return false;
    }
  }

  @Override
  public boolean match(ILoggingEvent event) {
    return !enabled || !isClassLoadable(event);
  }
}

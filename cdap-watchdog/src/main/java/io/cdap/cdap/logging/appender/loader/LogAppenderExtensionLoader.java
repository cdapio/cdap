/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.loader;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;

import java.util.Collections;
import java.util.Set;

/**
 * Extension loader to load log appenders.
 */
public class LogAppenderExtensionLoader extends AbstractExtensionLoader<String, Appender<ILoggingEvent>> {
  private final CConfiguration cConf;

  LogAppenderExtensionLoader(CConfiguration cConf) {
    super(cConf.get(Constants.Logging.LOG_APPENDER_EXT_DIR));
    this.cConf = cConf;
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(Appender<ILoggingEvent> appender) {
    return Collections.singleton(cConf.get(Constants.Logging.LOG_APPENDER_PROVIDER));
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // Only allow spi classes.
    return new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return resource.startsWith("ch/qos/logback");
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return packageName.startsWith("ch.qos.logback");
      }
    };
  }
}

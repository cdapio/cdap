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

package io.cdap.cdap.logging.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.logging.appender.CompositeLogAppender;
import io.cdap.cdap.logging.appender.ExtensionLogAppender;
import io.cdap.cdap.logging.appender.LogAppender;
import io.cdap.cdap.logging.appender.remote.RemoteLogAppender;
import java.util.Arrays;
import java.util.List;

/**
 * A Guice module to provide bindings for {@link LogAppender} implementations. Depending on the
 * configuration, it either provides a {@link RemoteLogAppender} or a {@link CompositeLogAppender}
 * that combines {@link RemoteLogAppender} and {@link ExtensionLogAppender}.
 */
public class RemoteLogAppenderModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(RemoteLogAppender.class).in(Scopes.SINGLETON);
    bind(ExtensionLogAppender.class).in(Scopes.SINGLETON);
  }

  @Provides
  @Singleton
  @SuppressWarnings("unused")
  protected LogAppender provideCompositeLogAppender(RemoteLogAppender remoteLogAppender,
      ExtensionLogAppender extensionLogAppender, CConfiguration cConf) {
    if (!cConf.getBoolean(Constants.Logging.LOG_PUBLISHER_ENABLED)) {
      return remoteLogAppender;
    }

    List<LogAppender> appenders = Arrays.asList(remoteLogAppender, extensionLogAppender);
    return new CompositeLogAppender(appenders);
  }
}

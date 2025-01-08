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

package io.cdap.cdap.logging.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.cdap.cdap.logging.appender.CompositeLogAppender;
import io.cdap.cdap.logging.appender.LogAppender;
import io.cdap.cdap.logging.appender.DefaultLogAppender;
import io.cdap.cdap.logging.framework.local.LocalLogAppender;
import java.util.Arrays;
import java.util.List;

/**
 * A Guice module to provide binding for {@link LogAppender} that writes log entries locally.
 */
public class LocalLogAppenderModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(LocalLogAppender.class).in(Scopes.SINGLETON);
    bind(DefaultLogAppender.class).in(Scopes.SINGLETON);
  }

  @Provides
  @Singleton
  @SuppressWarnings("unused")
  LogAppender provideCompositeLogAppender(LocalLogAppender localLogAppender,
      DefaultLogAppender defaultLogAppender) {
    List<LogAppender> appenders = Arrays.asList(localLogAppender, defaultLogAppender);
    return new CompositeLogAppender(appenders);
  }
}

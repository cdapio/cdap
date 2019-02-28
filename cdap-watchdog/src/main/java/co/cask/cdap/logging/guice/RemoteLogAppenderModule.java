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

package co.cask.cdap.logging.guice;

import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.remote.RemoteLogAppender;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 * A Guice module to provide binding for {@link LogAppender} that pushes log entries to log saver.
 */
public class RemoteLogAppenderModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(LogAppender.class).to(RemoteLogAppender.class).in(Scopes.SINGLETON);
  }
}

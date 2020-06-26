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

package io.cdap.cdap.logging.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.logging.appender.LogAppender;
import io.cdap.cdap.logging.framework.CustomLogPipelineConfigProvider;
import io.cdap.cdap.logging.framework.local.LocalLogAppender;

import java.io.File;
import java.util.Collections;
import java.util.List;

/**
 * A Guice module used for local mode of preview to provide binding for {@link LogAppender} that writes log entries
 * locally.
 */
public class PreviewLocalLogAppenderModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(LogAppender.class).to(LocalLogAppender.class).in(Scopes.SINGLETON);
  }

  @Provides
  public CustomLogPipelineConfigProvider provideCustomLogConfig(CConfiguration cConf) {
    // Return empty list for preview mode.
    return Collections::emptyList;
  }
}

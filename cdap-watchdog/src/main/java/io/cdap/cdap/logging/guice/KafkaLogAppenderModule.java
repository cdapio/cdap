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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.logging.appender.LogAppender;
import io.cdap.cdap.logging.appender.kafka.KafkaLogAppender;
import io.cdap.cdap.logging.framework.CustomLogPipelineConfigProvider;

import java.io.File;

/**
 * A Guice module to provide binding for {@link LogAppender} that writes log entries to Kafka.
 */
public class KafkaLogAppenderModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(LogAppender.class).to(KafkaLogAppender.class).in(Scopes.SINGLETON);
  }

  @Provides
  public CustomLogPipelineConfigProvider provideCustomLogConfig(CConfiguration cConf) {
    return () -> DirUtils.listFiles(new File(cConf.get(Constants.Logging.PIPELINE_CONFIG_DIR)), "xml");
  }
}

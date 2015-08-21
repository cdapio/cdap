/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.logging.common;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.logging.LogCollector;
import co.cask.cdap.common.logging.LogEvent;
import org.apache.hadoop.conf.Configuration;

/**
 * Implementation of LogWriter that writes to log collector directly.
 */
public class LocalLogWriter implements LogWriter {
  private final LogCollector collector;

  public LocalLogWriter(CConfiguration configuration) {
    Configuration hConfiguration = new Configuration();
    this.collector = new LogCollector(configuration, hConfiguration);
  }

  @Override
  public boolean write(final String tag, final String level, final String message) {
    LogEvent event = new LogEvent(tag, level, message);
    collector.log(event);
    return true;
  }
}

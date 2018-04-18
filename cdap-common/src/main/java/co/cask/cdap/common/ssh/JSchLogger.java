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

package co.cask.cdap.common.ssh;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link com.jcraft.jsch.Logger} that pipe into slf4j {@link Logger}.
 */
public class JSchLogger implements com.jcraft.jsch.Logger {

  private static final Logger LOG = LoggerFactory.getLogger(com.jcraft.jsch.Logger.class);

  @Override
  public boolean isEnabled(int level) {
    switch (level) {
      case DEBUG:
        return LOG.isDebugEnabled();
      case INFO:
        return LOG.isInfoEnabled();
      case WARN:
        return LOG.isWarnEnabled();
      case ERROR:
      case FATAL:
        return LOG.isErrorEnabled();
      default:
        return false;
    }
  }

  @Override
  public void log(int level, String message) {
    switch (level) {
      case DEBUG:
        LOG.debug(message);
        break;
      case INFO:
        LOG.info(message);
        break;
      case WARN:
        LOG.warn(message);
        break;
      case ERROR:
      case FATAL:
        LOG.error(message);
        break;
      default:
        // Ignore
    }
  }
}

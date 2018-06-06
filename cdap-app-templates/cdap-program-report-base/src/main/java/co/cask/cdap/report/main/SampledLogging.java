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
package co.cask.cdap.report.main;

import org.slf4j.Logger;

import javax.annotation.Nullable;

/**
 * log warning with interval count
 */
public class SampledLogging {
  private final Logger logger;
  private final int sample;
  private long count;

  public SampledLogging(Logger logger, int sample) {
    this.logger = logger;
    this.sample = sample;
    this.count = 0;
  }

  public void logWarning(String message, @Nullable Exception e) {
    if ((count) % sample == 0) {
      if (e != null) {
        logger.warn(message, e);
      } else {
        logger.warn(message);
      }
    }
    // increment count
    count = ((count + 1) < count) ? 0 : count + 1;
  }

}

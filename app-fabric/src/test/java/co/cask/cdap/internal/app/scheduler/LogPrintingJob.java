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

package co.cask.cdap.internal.app.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
*
*/
public class LogPrintingJob implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(LogPrintingJob.class);

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    try {
      LOG.info("Received Trigger at {}", context.getScheduledFireTime().toString());

      JobDataMap map = context.getMergedJobDataMap();
      String[] keys = map.getKeys();

      Preconditions.checkArgument(keys != null);
      Preconditions.checkArgument(keys.length > 0);
      LOG.info("Number of parameters {}", keys.length);
      for (String key : keys) {
        LOG.info("Parameter key: {}, value: {}", key, map.get(key));
      }
    } catch (Throwable e) {
      throw Throwables.propagate(e);
    }
    throw new JobExecutionException("excepion");
  }
}

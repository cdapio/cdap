/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.store;

import org.quartz.Trigger;
import org.quartz.listeners.TriggerListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs schedule trigger misfires.
 */
public class TriggerMisfireLogger extends TriggerListenerSupport {
  private static final Logger LOG = LoggerFactory.getLogger(TriggerMisfireLogger.class);

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  protected Logger getLog() {
    return LOG;
  }

  @Override
  public void triggerMisfired(Trigger trigger) {
    getLog().warn("Trigger {}.{} misfired job {}.{}  at: {}. Should have fired at: {}.",
                  trigger.getKey().getGroup(), trigger.getKey().getName(),
                  trigger.getJobKey().getGroup(), trigger.getJobKey().getName(), new java.util.Date(),
                  trigger.getNextFireTime());
  }
}

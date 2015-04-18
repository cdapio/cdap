/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.AbstractWorkflowAction;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * App Template to test batch adapter lifecycle.
 */
public class ActionBatchTemplate extends ApplicationTemplate<ActionBatchTemplate.Config> {
  public static final String NAME = ActionBatchTemplate.class.getSimpleName();

  public static class Config {
    private final String sourceName;
    private final String crontab;

    public Config(String sourceName, String crontab) {
      this.sourceName = sourceName;
      this.crontab = crontab;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Config that = (Config) o;

      return Objects.equal(sourceName, that.sourceName) && Objects.equal(crontab, that.crontab);
    }
  }


  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName(NAME);
    configurer.addWorkflow(new ActionWorkflow());
  }

  public static class ActionWorkflow extends AbstractWorkflow {
    public static final String NAME = ActionWorkflow.class.getSimpleName();

    @Override
    protected void configure() {
      setName(NAME);
      addAction(new Action());
    }
  }

  @Override
  public void configureAdapter(String adapterName, Config configuration,
                               AdapterConfigurer configurer) throws Exception {
    Preconditions.checkNotNull(configuration.sourceName);
    Preconditions.checkNotNull(configuration.crontab);

    Schedule schedule = Schedules.createTimeSchedule("dummy.schedule", "a dummy schedule", configuration.crontab);
    configurer.setSchedule(schedule);
  }

  public static class Action extends AbstractWorkflowAction {
    public static final Logger LOG = LoggerFactory.getLogger(Action.class);

    @Override
    public void run() {
      LOG.info("Action Ran!");
      try {
        TimeUnit.SECONDS.sleep(4);
      } catch (InterruptedException e) {
        LOG.error("Interrupted in Action", e);
      }
    }
  }
}

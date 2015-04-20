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
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.UUID;

/**
 * App Template to test adapter lifecycle.
 */
public class DummyBatchTemplate extends ApplicationTemplate<DummyBatchTemplate.Config> {
  public static final String NAME = DummyBatchTemplate.class.getSimpleName();

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

    @Override
    public int hashCode() {
      return Objects.hashCode(sourceName, crontab);
    }
  }

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName(NAME);
    // make the description different each time to distinguish between deployed versions in unit tests
    configurer.setDescription(UUID.randomUUID().toString());
    configurer.addWorkflow(new AdapterWorkflow());
    configurer.addMapReduce(new DummyMapReduceJob());
  }

  @Override
  public void configureAdapter(String adapterName, Config configuration,
                               AdapterConfigurer configurer) throws Exception {
    Preconditions.checkArgument(configuration.sourceName != null, "sourceName must be specified.");
    Preconditions.checkArgument(configuration.crontab != null, "crontab must be specified.");

    Schedule schedule = Schedules.createTimeSchedule("dummy.schedule", "a dummy schedule", configuration.crontab);
    configurer.setSchedule(schedule);
  }

  /**
   *
   */
  public static class AdapterWorkflow extends AbstractWorkflow {
    public static final String NAME = "AdapterWorkflow";
    @Override
    protected void configure() {
      setName(NAME);
      setDescription("Workflow to test Adapter");
      addMapReduce(DummyMapReduceJob.NAME);
    }
  }

  /**
   *
   */
  public static class DummyMapReduceJob extends AbstractMapReduce {
    public static final String NAME = "DummyMapReduceJob";
    @Override
    protected void configure() {
      setName(NAME);
      setDescription("Mapreduce that does nothing (and actually doesn't run) - it is here to test Adapter lifecycle");
    }
  }
}

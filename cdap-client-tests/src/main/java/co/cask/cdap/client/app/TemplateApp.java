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

package co.cask.cdap.client.app;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.workflow.AbstractWorkflow;

import java.util.Map;

/**
 *  App to test adapter lifecycle.
 */
public class TemplateApp extends ApplicationTemplate<Map<String, String>> {
  public static final String NAME = "dummyAdapter";

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName(NAME);
    configurer.setDescription("Application for to test Adapter lifecycle");
    configurer.addWorkflow(new AdapterWorkflow());
    configurer.addMapReduce(new DummyMapReduceJob());
  }

  @Override
  public void configureAdapter(String adapterName, Map<String, String> configuration,
                               AdapterConfigurer configurer) throws Exception {
    configurer.setSchedule(Schedules.createTimeSchedule("schedule", "description", "0/1 * * * *"));
  }

  /**
   *
   */
  public static class AdapterWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      setName("AdapterWorkflow");
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

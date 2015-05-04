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

package co.cask.cdap.template.etl.batch;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.common.Constants;
import co.cask.cdap.template.etl.common.ETLTemplate;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ETL Batch Template.
 */
public class ETLBatchTemplate extends ETLTemplate<ETLBatchConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(ETLBatchTemplate.class);
  private static final Gson GSON = new Gson();

  @Override
  public void configureAdapter(String adapterName, ETLBatchConfig etlBatchConfig,
                               AdapterConfigurer configurer) throws Exception {
    super.configureAdapter(adapterName, etlBatchConfig, configurer);
    String scheduleStr = etlBatchConfig.getSchedule();
    Preconditions.checkArgument(!Strings.isNullOrEmpty(scheduleStr), "Schedule must be specified in the config.");

    configurer.addRuntimeArgument(Constants.CONFIG_KEY, GSON.toJson(etlBatchConfig));
    configurer.setSchedule(new TimeSchedule(String.format("etl.batch.adapter.%s.schedule", adapterName),
                                            String.format("Schedule for %s Adapter", adapterName),
                                            etlBatchConfig.getSchedule()));
  }

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName(getAppName("etl.batch.plugin.name"));
    configurer.setDescription("Batch Extract-Transform-Load (ETL) Template");
    configurer.addMapReduce(new ETLMapReduce());
    configurer.addWorkflow(new ETLWorkflow());
  }
}

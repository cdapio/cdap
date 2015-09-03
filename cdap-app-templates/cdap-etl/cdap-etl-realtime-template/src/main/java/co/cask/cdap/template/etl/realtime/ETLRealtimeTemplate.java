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

package co.cask.cdap.template.etl.realtime;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.template.etl.common.Constants;
import co.cask.cdap.template.etl.common.ETLTemplate;
import co.cask.cdap.template.etl.realtime.config.ETLRealtimeConfig;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ETL Realtime Template.
 */
public class ETLRealtimeTemplate extends ETLTemplate<ETLRealtimeConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(ETLRealtimeTemplate.class);
  public static final String STATE_TABLE = "etlrealtimesourcestate";
  private static final Gson GSON = new Gson();

  @Override
  public void configureAdapter(String adapterName, ETLRealtimeConfig etlConfig, AdapterConfigurer configurer)
    throws Exception {
    super.configureAdapter(adapterName, etlConfig, configurer);
    int instances = (etlConfig.getInstances() != null) ? etlConfig.getInstances() : 1;
    Preconditions.checkArgument(instances > 0, "Instances should be greater than 0");
    configurer.addRuntimeArgument(Constants.CONFIG_KEY, GSON.toJson(etlConfig));
    configurer.setInstances(instances);
    // Generate unique id for this adapter creation.
    configurer.addRuntimeArgument(Constants.Realtime.UNIQUE_ID, String.valueOf(System.currentTimeMillis()));
  }

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName(getAppName("etl.realtime.plugin.name"));
    configurer.setDescription("Realtime Extract-Transform-Load (ETL) Template");
    configurer.addWorker(new ETLWorker());
    configurer.createDataset(STATE_TABLE, KeyValueTable.class, DatasetProperties.EMPTY);
  }
}

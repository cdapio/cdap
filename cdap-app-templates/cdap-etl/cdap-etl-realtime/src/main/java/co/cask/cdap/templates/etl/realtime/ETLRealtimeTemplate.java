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

package co.cask.cdap.templates.etl.realtime;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.ETLTemplate;
import co.cask.cdap.templates.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.templates.etl.realtime.sinks.NoOpSink;
import co.cask.cdap.templates.etl.realtime.sources.TestSource;
import co.cask.cdap.templates.etl.realtime.sources.TwitterStreamSource;
import co.cask.cdap.templates.etl.transforms.IdentityTransform;
import co.cask.cdap.templates.etl.transforms.ProjectionTransform;
import co.cask.cdap.templates.etl.transforms.ScriptFilterTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToGenericRecordTransform;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

/**
 * ETL Realtime Template.
 */
public class ETLRealtimeTemplate extends ETLTemplate<ETLRealtimeConfig> {
  public static final String STATE_TABLE = "etlrealtimesourcestate";
  private static final Gson GSON = new Gson();

  public ETLRealtimeTemplate() throws Exception {
    super();
    // Add class from lib here to be made available for use in the ETL Worker.
    // TODO : Remove this when plugins management is available.
    initTable(Lists.<Class>newArrayList(IdentityTransform.class,
                                        TwitterStreamSource.class,
                                        NoOpSink.class,
                                        TestSource.class,
                                        StructuredRecordToGenericRecordTransform.class,
                                        ScriptFilterTransform.class,
                                        ProjectionTransform.class));
  }

  @Override
  public void configureAdapter(String adapterName, ETLRealtimeConfig etlConfig, AdapterConfigurer configurer)
    throws Exception {
    super.configureAdapter(adapterName, etlConfig, configurer);
    Preconditions.checkArgument(etlConfig.getInstances() > 0, "Instances should be greater than 0");
    configurer.addRuntimeArgument(Constants.CONFIG_KEY, GSON.toJson(etlConfig));
    configurer.setInstances(etlConfig.getInstances());
    // Generate unique id for this adapter creation.
    configurer.addRuntimeArgument(Constants.Realtime.UNIQUE_ID, String.valueOf(System.currentTimeMillis()));
  }

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName("etlrealtime");
    configurer.setDescription("Realtime Extract-Transform-Load (ETL) Adapter");
    configurer.addWorker(new ETLWorker());
    configurer.createDataset(STATE_TABLE, KeyValueTable.class, DatasetProperties.EMPTY);
  }
}

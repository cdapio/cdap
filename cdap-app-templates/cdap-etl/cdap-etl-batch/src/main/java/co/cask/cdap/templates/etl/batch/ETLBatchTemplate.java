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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.templates.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.templates.etl.batch.sinks.BatchWritableSink;
import co.cask.cdap.templates.etl.batch.sinks.KVTableSink;
import co.cask.cdap.templates.etl.batch.sinks.TableSink;
import co.cask.cdap.templates.etl.batch.sinks.TimePartitionedFileSetDatasetAvroSink;
import co.cask.cdap.templates.etl.batch.sources.BatchReadableSource;
import co.cask.cdap.templates.etl.batch.sources.DBSource;
import co.cask.cdap.templates.etl.batch.sources.KVTableSource;
import co.cask.cdap.templates.etl.batch.sources.StreamBatchSource;
import co.cask.cdap.templates.etl.batch.sources.TableSource;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.ETLTemplate;
import co.cask.cdap.templates.etl.transforms.IdentityTransform;
import co.cask.cdap.templates.etl.transforms.RowToStructuredRecordTransform;
import co.cask.cdap.templates.etl.transforms.ScriptFilterTransform;
import co.cask.cdap.templates.etl.transforms.StreamToStructuredRecordTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToGenericRecordTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToPutTransform;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

/**
 * ETL Batch Template.
 */
public class ETLBatchTemplate extends ETLTemplate<ETLBatchConfig> {
  private static final Gson GSON = new Gson();

  public ETLBatchTemplate() throws Exception {
    super();

    // Add classes from Lib here to be available for use in the ETL Adapter.
    // TODO: Remove this when plugins management is available.
    initTable(Lists.<Class>newArrayList(KVTableSource.class,
                                        KVTableSink.class,
                                        BatchReadableSource.class,
                                        BatchWritableSink.class,
                                        TableSource.class,
                                        TableSink.class,
                                        IdentityTransform.class,
                                        StructuredRecordToPutTransform.class,
                                        RowToStructuredRecordTransform.class,
                                        StructuredRecordToGenericRecordTransform.class,
                                        StreamBatchSource.class,
                                        TimePartitionedFileSetDatasetAvroSink.class,
                                        StreamToStructuredRecordTransform.class,
                                        ScriptFilterTransform.class,
                                        DBSource.class));
  }

  @Override
  public void configureAdapter(String adapterName, ETLBatchConfig etlBatchConfig, AdapterConfigurer configurer)
    throws Exception {
    super.configureAdapter(adapterName, etlBatchConfig, configurer);
    configurer.addRuntimeArgument(Constants.CONFIG_KEY, GSON.toJson(etlBatchConfig));
    configurer.setSchedule(new TimeSchedule(String.format("etl.batch.adapter.%s.schedule", adapterName),
                                            String.format("Schedule for %s Adapter", adapterName),
                                            etlBatchConfig.getSchedule()));
  }

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName("etlbatch");
    configurer.setDescription("Batch Extract-Transform-Load (ETL) Adapter");
    configurer.addMapReduce(new ETLMapReduce());
    configurer.addWorkflow(new ETLWorkflow());
  }
}

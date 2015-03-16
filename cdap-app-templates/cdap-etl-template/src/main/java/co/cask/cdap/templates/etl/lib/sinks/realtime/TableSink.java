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

package co.cask.cdap.templates.etl.lib.sinks.realtime;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.templates.etl.api.SinkContext;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.stages.AbstractRealtimeSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table Dataset Sink.
 */
public class TableSink extends AbstractRealtimeSink<Put> {
  private static final Logger LOG = LoggerFactory.getLogger(TableSink.class);
  private Table table;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("Table Dataset Sink");
    configurer.setDescription("Supports writes to Table");
  }

  @Override
  public void initialize(SinkContext context) {
    super.initialize(context);
    table = context.getDataset("myTable");
  }

  @Override
  public void write(Put object) {
    LOG.error("Writing to Table : Key = {}", Bytes.toString(object.getRow()));
    table.put(object);
  }
}

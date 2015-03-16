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
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.templates.etl.api.SinkContext;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.stages.AbstractRealtimeSink;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KeyValueTableSink extends AbstractRealtimeSink<String> {
  private static final Logger LOG = LoggerFactory.getLogger(KeyValueTableSink.class);
  private KeyValueTable table;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("KeyValueTable Sink");
    configurer.setDescription("Writes to KVTable");
    configurer.setReqdProperties(Sets.newHashSet("tableName"));
  }

  @Override
  public void initialize(SinkContext context) {
    super.initialize(context);
    table = context.getDataset(context.getRuntimeArguments().get("tableName"));
  }

  @Override
  public void write(String object) {
    LOG.error("Writing to Dataset : {}", object);
    table.write(object, Bytes.toBytes(true));
  }
}

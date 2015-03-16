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

package co.cask.cdap.templates.etl.lib.sinks.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class KVTableBatchSink implements BatchSink<String> {
  private static final Logger LOG = LoggerFactory.getLogger(KVTableBatchSink.class);

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("KeyValuetable Batch Sink");
    configurer.setDescription("Writing to Key Value Table in Batch Mode");
    configurer.setReqdProperties(Sets.newHashSet("tableName"));
  }

  @Override
  public void prepareJob(MapReduceContext context) {
    context.setOutput("KVTableBatchSink");
  }

  @Override
  public void initialize(MapReduceContext context) {
    // no-op
  }

  @Override
  public void write(Mapper.Context context, String input) throws IOException, InterruptedException {
    LOG.error("Got this stuff {}", input);
    context.write(Bytes.toBytes(input), Bytes.toBytes(true));
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) {
    // no-op but one can think about creating partitions in fileset here
  }

  @Override
  public void destroy() {
    // no-op
  }
}

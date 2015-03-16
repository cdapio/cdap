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

package co.cask.cdap.templates.etl.lib.sources.batch;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.realtime.Emitter;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class BatchKafkaSource implements BatchSource<EtlKey, CamusWrapper<String>, String> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchKafkaSource.class);

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("KafkaBatchSource");
    configurer.setDescription("Kafka Batch Source");
  }

  @Override
  public void prepareJob(MapReduceContext context) {
    Job job = context.getHadoopJob();
    job.getConfiguration().set("kafka.brokers", "localhost:9092");
    job.getConfiguration().set("zookeeper.hosts", "localhost:2181");
    job.getConfiguration().set("kafka.client.name", "adapt");
    job.setInputFormatClass(EtlInputFormat.class);
  }

  @Override
  public void initialize(MapReduceContext context) {

  }

  @Override
  public void read(EtlKey etlKey, CamusWrapper<String> wrapper, Emitter<String> data) {
    data.emit(wrapper.getRecord());
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    LOG.info("MR job completed : {}", succeeded);
  }

  @Override
  public void destroy() {

  }
}
